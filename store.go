package redisstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/swithek/sessionup"
)

// RedisStore is a Redis implementation of sessionup.Store.
type RedisStore struct {
	pool   *redis.Pool
	prefix string
}

// New returns a fresh instance of RedisStore.
// prefix parameter determines the prefix that will be used for
// each session key (might be empty string). Useful when working
// with multiple session managers.
func New(pool *redis.Pool, prefix string) *RedisStore {
	return &RedisStore{
		pool:   pool,
		prefix: prefix,
	}
}

// Create inserts the provided session into the store and ensures
// that it is deleted when expiration time due.
func (r *RedisStore) Create(ctx context.Context, s sessionup.Session) error {
	c, err := r.pool.GetContext(ctx)
	if err != nil {
		return err
	}

	defer c.Close()

	if err = c.Send("MULTI"); err != nil {
		return err
	}

	now := time.Now().UnixNano()
	uKey := r.key(true, s.UserKey)

	// remove expired sessions from user session set
	err = c.Send("ZREMRANGEBYSCORE", uKey, "-inf", now)
	if err != nil {
		return err
	}

	// find previous user session set's expiration time
	uExpMilli, err := redis.Int64(c.Do("PTTL", uKey))
	if err != nil {
		return err
	}

	uExpMilli += now
	sExpNano := s.ExpiresAt.UnixNano()
	sExpMilli := sExpNano / 1000

	if sExpMilli > uExpMilli {
		uExpMilli = sExpMilli
	}

	// add session to user session set
	err = c.Send("ZADD", uKey, sExpNano, s.ID)
	if err != nil {
		return err
	}

	// update user session set's expiration time
	err = c.Send("PEXPIREAT", uKey, uExpMilli)
	if err != nil {
		return err
	}

	sKey := r.key(false, s.ID)

	// set session
	err = c.Send(
		"HMSET", sKey,
		"CreatedAt", s.CreatedAt,
		"ExpiresAt", s.ExpiresAt,
		"ID", s.ID,
		"UsetKey", s.UserKey,
		"IP", s.IP,
		"Agent.OS", s.Agent.OS,
		"Agent.Browser", s.Agent.Browser,
	)
	if err != nil {
		return err
	}

	// set session's expiration time
	err = c.Send("PEXPIREAT", sKey, sExpMilli)
	if err != nil {
		return err
	}

	return c.Send("EXEC")
}

// FetchByID retrieves a session from the store by the provided ID.
// The second returned value indicates whether the session was found
// or not (true == found), error should will be nil if session is not found.
func (r *RedisStore) FetchByID(ctx context.Context, id string) (sessionup.Session, bool, error) {
	c, err := r.pool.GetContext(ctx)
	if err != nil {
		return sessionup.Session{}, false, err
	}

	defer c.Close()

	vv, err := redis.Values(c.Do("HGETALL", r.key(false, id)))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			err = nil
		}

		return sessionup.Session{}, false, err
	}

	var s sessionup.Session
	if err = redis.ScanStruct(vv, &s); err != nil {
		return sessionup.Session{}, false, err
	}

	return s, true, nil
}

// FetchByUserKey retrieves all sessions associated with the
// provided user key. If none are found, both return values will be nil.
func (r *RedisStore) FetchByUserKey(ctx context.Context, key string) ([]sessionup.Session, error) {
	c, err := r.pool.GetContext(ctx)
	if err != nil {
		return nil, err
	}

	defer c.Close()

	if err = c.Send("MULTI"); err != nil {
		return nil, err
	}

	ids, err := redis.Strings(c.Do("ZRANGEBYSCORE", r.key(true, key), "-inf", "+inf"))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			err = nil
		}

		return nil, err
	}

	var ss []sessionup.Session

	for i := range ids {
		vv, err := redis.Values(c.Do("HGETALL", r.key(false, ids[i])))
		if err != nil {
			return nil, err
		}

		var s sessionup.Session
		if err = redis.ScanStruct(vv, &s); err != nil {
			return nil, err
		}

		ss = append(ss, s)
	}

	if err = c.Send("EXEC"); err != nil {
		return nil, err
	}

	return ss, nil
}

// DeleteByID deletes the session from the store by the provided ID.
// If session is not found, this function will be no-op.
func (r *RedisStore) DeleteByID(ctx context.Context, id string) error {
	c, err := r.pool.GetContext(ctx)
	if err != nil {
		return err
	}

	defer c.Close()

	if err = c.Send("MULTI"); err != nil {
		return err
	}

	sKey := r.key(false, id)

	vv, err := redis.Values(c.Do("HGETALL", sKey))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			err = nil
		}

		return err
	}

	var s sessionup.Session
	if err = redis.ScanStruct(vv, &s); err != nil {
		return err
	}

	if err = c.Send("ZREM", r.key(true, s.UserKey), sKey); err != nil {
		return err
	}

	if err = c.Send("DEL", sKey); err != nil {
		return err
	}

	return c.Send("EXEC")
}

// DeleteByUserKey deletes all sessions associated with the provided
// user key, except those whose IDs are provided as the last argument.
// If none are found, this function will no-op.
func (r *RedisStore) DeleteByUserKey(ctx context.Context, key string, expIDs ...string) error {
	c, err := r.pool.GetContext(ctx)
	if err != nil {
		return err
	}

	defer c.Close()

	if err = c.Send("MULTI"); err != nil {
		return err
	}

	uKey := r.key(true, key)

	ids, err := redis.Strings(c.Do("ZRANGEBYSCORE", uKey, "-inf", "+inf"))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			err = nil
		}

		return err
	}

Outer:
	for i := range ids {
		for j := range expIDs {
			if expIDs[j] == ids[i] {
				continue Outer
			}
		}

		if err = c.Send("DEL", r.key(false, ids[i])); err != nil {
			return err
		}
	}

	if len(expIDs) == 0 || len(ids) == 0 {
		if err = c.Send("DEL", uKey); err != nil {
			return err
		}
	}

	return c.Send("EXEC")
}

// key prepares a key for the appropriate namespace.
func (r *RedisStore) key(user bool, v string) string {
	namespace := "session"
	if user {
		namespace = "user"
	}

	return fmt.Sprintf("%s:%s:%s", r.prefix, namespace, v)
}
