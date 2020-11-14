package redisstore

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
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

	sKey := r.key(false, s.ID)
	uKey := r.key(true, s.UserKey)

	if _, err = c.Do("WATCH", sKey); err != nil {
		return err
	}

	if _, err = c.Do("WATCH", uKey); err != nil {
		return err
	}

	// check if session key is already present
	v, err := redis.Int64(c.Do("EXISTS", sKey))
	if err != nil {
		return err
	}

	if v > 0 {
		return sessionup.ErrDuplicateID
	}

	// find previous user session set's expiration time
	uExpMilli, err := redis.Int64(c.Do("PTTL", uKey))
	if err != nil {
		return err
	}

	now := time.Now().UnixNano()
	uExpMilli += now / int64(time.Millisecond)
	sExpNano := s.ExpiresAt.UnixNano()
	sExpMilli := sExpNano / int64(time.Millisecond)

	if sExpMilli > uExpMilli {
		uExpMilli = sExpMilli
	}

	// start transaction
	if _, err = c.Do("MULTI"); err != nil {
		return err
	}

	// remove expired sessions from user session set
	_, err = c.Do("ZREMRANGEBYSCORE", uKey, "-inf", now)
	if err != nil {
		return err
	}

	// add session key to user session set
	_, err = c.Do("ZADD", uKey, sExpNano, sKey)
	if err != nil {
		return err
	}

	// update user session set's expiration time
	_, err = c.Do("PEXPIREAT", uKey, uExpMilli)
	if err != nil {
		return err
	}

	// create session hash
	_, err = c.Do(
		"HMSET", sKey,
		"created_at", s.CreatedAt.Format(time.RFC3339Nano),
		"expires_at", s.ExpiresAt.Format(time.RFC3339Nano),
		"id", s.ID,
		"user_key", s.UserKey,
		"ip", s.IP.String(),
		"agent_os", s.Agent.OS,
		"agent_browser", s.Agent.Browser,
		"meta", metaToString(s.Meta),
	)
	if err != nil {
		return err
	}

	// set session's expiration time
	_, err = c.Do("PEXPIREAT", sKey, sExpMilli)
	if err != nil {
		return err
	}

	_, err = c.Do("EXEC")

	return err
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

	vv, err := redis.StringMap(c.Do("HGETALL", r.key(false, id)))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			err = nil
		}

		return sessionup.Session{}, false, err
	}

	if len(vv) == 0 {
		return sessionup.Session{}, false, nil
	}

	s, err := parse(vv)
	if err != nil {
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

	ids, err := redis.Strings(c.Do("ZRANGEBYSCORE", r.key(true, key), "-inf", "+inf"))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			err = nil
		}

		return nil, err
	}

	var ss []sessionup.Session

	for i := range ids {
		vv, err := redis.StringMap(c.Do("HGETALL", ids[i]))
		if err != nil {
			if errors.Is(err, redis.ErrNil) {
				continue
			}

			return nil, err
		}

		if len(vv) == 0 {
			continue
		}

		s, err := parse(vv)
		if err != nil {
			return nil, err
		}

		ss = append(ss, s)
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

	sKey := r.key(false, id)

	if _, err = c.Do("WATCH", sKey); err != nil {
		return err
	}

	vv, err := redis.StringMap(c.Do("HGETALL", sKey))
	if err != nil {
		if errors.Is(err, redis.ErrNil) {
			err = nil
		}

		return err
	}

	if len(vv) == 0 {
		return nil
	}

	s, err := parse(vv)
	if err != nil {
		return err
	}

	uKey := r.key(true, s.UserKey)

	if _, err = c.Do("WATCH", uKey); err != nil {
		return err
	}

	ids, err := redis.Strings(c.Do("ZRANGEBYSCORE", uKey, "-inf", "+inf"))
	if err != nil {
		return err
	}

	if _, err = c.Do("MULTI"); err != nil {
		return err
	}

	if _, err = c.Do("ZREM", uKey, sKey); err != nil {
		return err
	}

	if len(ids) == 1 && ids[0] == sKey {
		if _, err = c.Do("DEL", uKey); err != nil {
			return err
		}
	}

	if _, err = c.Do("DEL", sKey); err != nil {
		return err
	}

	_, err = c.Do("EXEC")

	return err
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

	uKey := r.key(true, key)

	if _, err = c.Do("WATCH", uKey); err != nil {
		return err
	}

	ids, err := redis.Strings(c.Do("ZRANGEBYSCORE", uKey, "-inf", "+inf"))
	if err != nil {
		if !errors.Is(err, redis.ErrNil) {
			return err
		}
	}

	if _, err = c.Do("MULTI"); err != nil {
		return err
	}

Outer:
	for i := range ids {
		id := extract(ids[i])

		for j := range expIDs {
			if expIDs[j] == id {
				continue Outer
			}
		}

		if _, err = c.Do("DEL", ids[i]); err != nil {
			return err
		}

		if len(expIDs) > 0 {
			if _, err = c.Do("ZREM", uKey, ids[i]); err != nil {
				return err
			}
		}
	}

	if len(expIDs) == 0 || len(ids) == 0 {
		if _, err = c.Do("DEL", uKey); err != nil {
			return err
		}
	}

	_, err = c.Do("EXEC")

	return err
}

// key prepares a key for the appropriate namespace.
func (r *RedisStore) key(user bool, v string) string {
	namespace := "session"
	if user {
		namespace = "user"
	}

	return fmt.Sprintf("%s:%s:%s", r.prefix, namespace, v)
}

// extract strips prefix and namespace data from the key.
func extract(v string) string {
	strs := strings.Split(v, ":")
	if len(strs) != 3 {
		return ""
	}

	return strs[2]
}

// parse converts a map of raw data into session structure.
func parse(vv map[string]string) (sessionup.Session, error) {
	s := sessionup.Session{
		ID:      vv["id"],
		UserKey: vv["user_key"],
		IP:      net.ParseIP(vv["ip"]),
		Meta:    metaFromString(vv["meta"]),
	}
	s.Agent.OS = vv["agent_os"]
	s.Agent.Browser = vv["agent_browser"]

	var err error
	s.CreatedAt, err = time.Parse(time.RFC3339Nano, vv["created_at"])
	if err != nil {
		return sessionup.Session{}, err
	}

	s.ExpiresAt, err = time.Parse(time.RFC3339Nano, vv["expires_at"])
	if err != nil {
		return sessionup.Session{}, err
	}

	return s, nil
}

// metaToString converts metadata map into string.
func metaToString(mm map[string]string) string {
	var b strings.Builder
	for k, v := range mm {
		b.WriteString(fmt.Sprintf("%s:%s;", k, v))
	}

	return b.String()
}

// metaFromString converts metadata string into map.
func metaFromString(s string) map[string]string {
	if s == "" {
		return nil
	}

	meta := make(map[string]string)
	mm := strings.Split(s, ";")

	for _, m := range mm {
		vv := strings.Split(m, ":")
		if len(vv) != 2 {
			continue
		}

		meta[vv[0]] = vv[1]
	}

	return meta
}
