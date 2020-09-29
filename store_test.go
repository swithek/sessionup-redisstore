package redisstore

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/rafaeljusto/redigomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/swithek/sessionup"
)

const prefix = "test"

func Test_New(t *testing.T) {
	r := New(&redis.Pool{}, prefix)
	require.NotNil(t, r)
	assert.NotNil(t, r.pool)
	assert.Equal(t, prefix, r.prefix)
}

func Test_RedisStore_Create(t *testing.T) {
	inp := sessionup.Session{
		UserKey:   "u123",
		ID:        "id123",
		ExpiresAt: time.Now().UTC().Add(time.Hour * 24),
		CreatedAt: time.Now().UTC(),
		IP:        net.ParseIP("127.0.0.1"),
	}
	inp.Agent.OS = "gnu/linux"
	inp.Agent.Browser = "firefox"

	uKey := prefix + ":user:" + inp.UserKey
	sKey := prefix + ":session:" + inp.ID

	cc := map[string]struct {
		Cancelled bool
		Conn      func() (*redigomock.Conn, func(*testing.T))
		Err       bool
	}{
		"Cancelled context": {
			Cancelled: true,
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during transaction creation": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI").ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during cleanup in user session set": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt()).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during previous user key expiration fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during user session set update": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).Expect(int64(20))
				conn.Command("ZADD", uKey, inp.ExpiresAt.UnixNano(), inp.ID).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during user key expiration update": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).Expect(int64(20))
				conn.Command("ZADD", uKey, inp.ExpiresAt.UnixNano(), inp.ID)
				conn.Command("PEXPIREAT", uKey, inp.ExpiresAt.UnixNano()/1000).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during session hash creation": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).Expect(int64(20))
				conn.Command("ZADD", uKey, inp.ExpiresAt.UnixNano(), inp.ID)
				conn.Command("PEXPIREAT", uKey, inp.ExpiresAt.UnixNano()/1000)
				conn.Command(
					"HMSET", sKey,
					"created_at", inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at", inp.ExpiresAt.Format(time.RFC3339Nano),
					"id", inp.ID,
					"user_key", inp.UserKey,
					"ip", inp.IP.String(),
					"agent_os", inp.Agent.OS,
					"agent_browser", inp.Agent.Browser,
				).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returnde during session expiration creation": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).Expect(int64(20))
				conn.Command("ZADD", uKey, inp.ExpiresAt.UnixNano(), inp.ID)
				conn.Command("PEXPIREAT", uKey, inp.ExpiresAt.UnixNano()/1000)
				conn.Command(
					"HMSET", sKey,
					"created_at", inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at", inp.ExpiresAt.Format(time.RFC3339Nano),
					"id", inp.ID,
					"user_key", inp.UserKey,
					"ip", inp.IP.String(),
					"agent_os", inp.Agent.OS,
					"agent_browser", inp.Agent.Browser,
				)
				conn.Command("PEXPIREAT", sKey, inp.ExpiresAt.UnixNano()/1000).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during transaction exec": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).Expect(int64(20))
				conn.Command("ZADD", uKey, inp.ExpiresAt.UnixNano(), inp.ID)
				conn.Command("PEXPIREAT", uKey, inp.ExpiresAt.UnixNano()/1000)
				conn.Command(
					"HMSET", sKey,
					"created_at", inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at", inp.ExpiresAt.Format(time.RFC3339Nano),
					"id", inp.ID,
					"user_key", inp.UserKey,
					"ip", inp.IP.String(),
					"agent_os", inp.Agent.OS,
					"agent_browser", inp.Agent.Browser,
				)
				conn.Command("PEXPIREAT", sKey, inp.ExpiresAt.UnixNano()/1000)
				conn.GenericCommand("EXEC").ExpectError(assert.AnError)

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Successful execution with previous user key expiration": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).Expect(time.Now().UTC().Add(time.Hour * 72).UnixNano())
				conn.Command("ZADD", uKey, inp.ExpiresAt.UnixNano(), inp.ID)
				conn.Command("PEXPIREAT", uKey, redigomock.NewAnyInt())
				conn.Command(
					"HMSET", sKey,
					"created_at", inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at", inp.ExpiresAt.Format(time.RFC3339Nano),
					"id", inp.ID,
					"user_key", inp.UserKey,
					"ip", inp.IP.String(),
					"agent_os", inp.Agent.OS,
					"agent_browser", inp.Agent.Browser,
				)
				conn.Command("PEXPIREAT", sKey, inp.ExpiresAt.UnixNano()/1000)
				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
		"Successful execution": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZREMRANGEBYSCORE", uKey, "-inf", redigomock.NewAnyInt())
				conn.Command("PTTL", uKey).Expect(int64(20))
				conn.Command("ZADD", uKey, inp.ExpiresAt.UnixNano(), inp.ID)
				conn.Command("PEXPIREAT", uKey, inp.ExpiresAt.UnixNano()/1000)
				conn.Command(
					"HMSET", sKey,
					"created_at", inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at", inp.ExpiresAt.Format(time.RFC3339Nano),
					"id", inp.ID,
					"user_key", inp.UserKey,
					"ip", inp.IP.String(),
					"agent_os", inp.Agent.OS,
					"agent_browser", inp.Agent.Browser,
				)
				conn.Command("PEXPIREAT", sKey, inp.ExpiresAt.UnixNano()/1000)
				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
	}

	for cn, c := range cc {
		c := c

		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			conn, check := c.Conn()

			r := RedisStore{
				pool: &redis.Pool{
					Dial: func() (redis.Conn, error) {
						return conn, nil
					},
					Wait:      true,
					MaxActive: 10,
				},
				prefix: prefix,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if c.Cancelled {
				cancel()
			}

			err := r.Create(ctx, inp)
			if c.Err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			check(t)
		})
	}
}

func Test_RedisStore_FetchByID(t *testing.T) {
	inp := sessionup.Session{
		UserKey:   "u123",
		ID:        "id123",
		ExpiresAt: time.Now().UTC().Add(time.Hour * 24).Round(0),
		CreatedAt: time.Now().UTC().Round(0),
		IP:        net.ParseIP("127.0.0.1"),
	}
	inp.Agent.OS = "gnu/linux"
	inp.Agent.Browser = "firefox"

	sKey := prefix + ":session:" + inp.ID

	cc := map[string]struct {
		Cancelled bool
		Conn      func() (*redigomock.Conn, func(*testing.T))
		Result    bool
		Found     bool
		Err       bool
	}{
		"Cancelled context": {
			Cancelled: true,
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during session hash fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.Command("HGETALL", sKey).ExpectError(assert.AnError)

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during parsing": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    "123",
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Not found": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.Command("HGETALL", sKey).ExpectError(redis.ErrNil)

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
		"Successful fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Result: true,
			Found:  true,
		},
	}

	for cn, c := range cc {
		c := c

		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			conn, check := c.Conn()

			r := RedisStore{
				pool: &redis.Pool{
					Dial: func() (redis.Conn, error) {
						return conn, nil
					},
					Wait:      true,
					MaxActive: 10,
				},
				prefix: prefix,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if c.Cancelled {
				cancel()
			}

			s, ok, err := r.FetchByID(ctx, inp.ID)
			if c.Err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if c.Result {
				assert.Equal(t, inp, s)
			} else {
				assert.Zero(t, s)
			}

			assert.Equal(t, c.Found, ok)
			check(t)
		})
	}
}

func Test_RedisStore_FetchByUserKey(t *testing.T) {
	inp := make([]sessionup.Session, 5)

	for i := 0; i < 5; i++ {
		s := sessionup.Session{
			UserKey:   "u123",
			ID:        "id" + strconv.Itoa(i),
			ExpiresAt: time.Now().UTC().Add(time.Hour * 24).Round(0),
			CreatedAt: time.Now().UTC().Round(0),
			IP:        net.ParseIP("127.0.0.1"),
		}
		s.Agent.OS = "gnu/linux"
		s.Agent.Browser = "firefox"
		inp[i] = s
	}

	uKey := prefix + ":user:" + inp[0].UserKey

	cc := map[string]struct {
		Cancelled bool
		Conn      func() (*redigomock.Conn, func(*testing.T))
		Result    bool
		Err       bool
	}{
		"Cancelled context": {
			Cancelled: true,
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during transaction creation": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI").ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during user session set fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", uKey, "-inf", "+inf").ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during single session fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", uKey, "-inf", "+inf").ExpectSlice(
					inp[0].ID,
					inp[1].ID,
					inp[2].ID,
					inp[3].ID,
					inp[4].ID,
				)

				sKey := prefix + ":session:" + inp[0].ID
				conn.Command("HGETALL", sKey).ExpectError(assert.AnError)

				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during parsing": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", uKey, "-inf", "+inf").ExpectSlice(
					inp[0].ID,
					inp[1].ID,
					inp[2].ID,
					inp[3].ID,
					inp[4].ID,
				)

				sKey := prefix + ":session:" + inp[0].ID
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp[0].CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    "123",
					"id":            inp[0].ID,
					"user_key":      inp[0].UserKey,
					"ip":            inp[0].IP.String(),
					"agent_os":      inp[0].Agent.OS,
					"agent_browser": inp[0].Agent.Browser,
				})

				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Errors returned during transaction exec": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", uKey, "-inf", "+inf").ExpectSlice(
					inp[0].ID,
					inp[1].ID,
					inp[2].ID,
					inp[3].ID,
					inp[4].ID,
				)

				for i := 0; i < 5; i++ {
					sKey := prefix + ":session:" + inp[i].ID
					conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
						"created_at":    inp[i].CreatedAt.Format(time.RFC3339Nano),
						"expires_at":    inp[i].ExpiresAt.Format(time.RFC3339Nano),
						"id":            inp[i].ID,
						"user_key":      inp[i].UserKey,
						"ip":            inp[i].IP.String(),
						"agent_os":      inp[i].Agent.OS,
						"agent_browser": inp[i].Agent.Browser,
					})
				}

				conn.GenericCommand("EXEC").ExpectError(assert.AnError)

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Not found": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", uKey, "-inf", "+inf").ExpectError(redis.ErrNil)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
		"Successful fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", uKey, "-inf", "+inf").ExpectSlice(
					inp[0].ID,
					inp[1].ID,
					inp[2].ID,
					inp[3].ID,
					inp[4].ID,
				)

				for i := 0; i < 5; i++ {
					sKey := prefix + ":session:" + inp[i].ID
					conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
						"created_at":    inp[i].CreatedAt.Format(time.RFC3339Nano),
						"expires_at":    inp[i].ExpiresAt.Format(time.RFC3339Nano),
						"id":            inp[i].ID,
						"user_key":      inp[i].UserKey,
						"ip":            inp[i].IP.String(),
						"agent_os":      inp[i].Agent.OS,
						"agent_browser": inp[i].Agent.Browser,
					})
				}

				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Result: true,
		},
	}

	for cn, c := range cc {
		c := c

		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			conn, check := c.Conn()

			r := RedisStore{
				pool: &redis.Pool{
					Dial: func() (redis.Conn, error) {
						return conn, nil
					},
					Wait:      true,
					MaxActive: 10,
				},
				prefix: prefix,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if c.Cancelled {
				cancel()
			}

			ss, err := r.FetchByUserKey(ctx, inp[0].UserKey)
			if c.Err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if c.Result {
				assert.Equal(t, inp, ss)
			} else {
				assert.Nil(t, ss)
			}

			check(t)
		})
	}
}

func Test_RedisStore_DeleteByID(t *testing.T) {
	inp := sessionup.Session{
		UserKey:   "u123",
		ID:        "id123",
		ExpiresAt: time.Now().UTC().Add(time.Hour * 24).Round(0),
		CreatedAt: time.Now().UTC().Round(0),
		IP:        net.ParseIP("127.0.0.1"),
	}
	inp.Agent.OS = "gnu/linux"
	inp.Agent.Browser = "firefox"

	sKey := prefix + ":session:" + inp.ID
	uKey := prefix + ":user:" + inp.UserKey

	cc := map[string]struct {
		Cancelled bool
		Conn      func() (*redigomock.Conn, func(*testing.T))
		Err       bool
	}{
		"Cancelled context": {
			Cancelled: true,
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during transaction creation": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI").ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during session fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during parsing": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    "123",
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during session id deletion from user set": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.Command("ZREM", uKey, sKey).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during user session count fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.Command("ZREM", uKey, sKey)
				conn.Command("ZCARD", uKey).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during user key deletion": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.Command("ZREM", uKey, sKey)
				conn.Command("ZCARD", uKey).Expect(int64(0))
				conn.Command("DEL", uKey).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during session key deletion": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.Command("ZREM", uKey, sKey)
				conn.Command("ZCARD", uKey).Expect(int64(3))
				conn.Command("DEL", sKey).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during transaction exec": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.Command("ZREM", uKey, sKey)
				conn.Command("ZCARD", uKey).Expect(int64(3))
				conn.Command("DEL", sKey)
				conn.GenericCommand("EXEC").ExpectError(assert.AnError)

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Not found": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectError(redis.ErrNil)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
		"Successful deletion with empty user session set": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.Command("ZREM", uKey, sKey)
				conn.Command("ZCARD", uKey).Expect(int64(0))
				conn.Command("DEL", uKey)
				conn.Command("DEL", sKey)
				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
		"Successful deletion": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("HGETALL", sKey).ExpectMap(map[string]string{
					"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
					"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
					"id":            inp.ID,
					"user_key":      inp.UserKey,
					"ip":            inp.IP.String(),
					"agent_os":      inp.Agent.OS,
					"agent_browser": inp.Agent.Browser,
				})
				conn.Command("ZREM", uKey, sKey)
				conn.Command("ZCARD", uKey).Expect(int64(3))
				conn.Command("DEL", sKey)
				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
	}

	for cn, c := range cc {
		c := c

		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			conn, check := c.Conn()

			r := RedisStore{
				pool: &redis.Pool{
					Dial: func() (redis.Conn, error) {
						return conn, nil
					},
					Wait:      true,
					MaxActive: 10,
				},
				prefix: prefix,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if c.Cancelled {
				cancel()
			}

			err := r.DeleteByID(ctx, inp.ID)
			check(t)

			if c.Err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func Test_RedisStore_DeleteByUserKey(t *testing.T) {
	const (
		inpKey     = "u123"
		inpFullKey = prefix + ":user:" + inpKey
	)

	inpExceptIDs := []string{"id222", "id333"}

	cc := map[string]struct {
		Cancelled      bool
		Conn           func() (*redigomock.Conn, func(*testing.T))
		WithExceptions bool
		Err            bool
	}{
		"Cancelled context": {
			Cancelled: true,
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during transaction creation": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI").ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned user session set fetch": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", inpFullKey, "-inf", "+inf").ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during single session deletion": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", inpFullKey, "-inf", "+inf").ExpectSlice(
					"id111",
					"id222",
					"id333",
				)
				conn.Command("DEL", prefix+":session:id111").ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during user key deletion": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", inpFullKey, "-inf", "+inf").ExpectSlice(
					"id111",
					"id222",
					"id333",
				)
				conn.Command("DEL", prefix+":session:id111")
				conn.Command("DEL", prefix+":session:id222")
				conn.Command("DEL", prefix+":session:id333")
				conn.Command("DEL", inpFullKey).ExpectError(assert.AnError)
				conn.GenericCommand("DISCARD")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Error returned during transaction exec": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", inpFullKey, "-inf", "+inf").ExpectSlice(
					"id111",
					"id222",
					"id333",
				)
				conn.Command("DEL", prefix+":session:id111")
				conn.Command("DEL", prefix+":session:id222")
				conn.Command("DEL", prefix+":session:id333")
				conn.Command("DEL", inpFullKey)
				conn.GenericCommand("EXEC").ExpectError(assert.AnError)

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			Err: true,
		},
		"Successful deletion with id exceptions": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", inpFullKey, "-inf", "+inf").ExpectSlice(
					"id111",
					"id222",
					"id333",
				)
				conn.Command("DEL", prefix+":session:id111")
				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
			WithExceptions: true,
		},
		"Successful deletion without sessions": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", inpFullKey, "-inf", "+inf").ExpectError(redis.ErrNil)
				conn.Command("DEL", inpFullKey)
				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
		"Successful deletion": {
			Conn: func() (*redigomock.Conn, func(*testing.T)) {
				conn := redigomock.NewConn()
				conn.GenericCommand("MULTI")
				conn.Command("ZRANGEBYSCORE", inpFullKey, "-inf", "+inf").ExpectSlice(
					"id111",
					"id222",
					"id333",
				)
				conn.Command("DEL", prefix+":session:id111")
				conn.Command("DEL", prefix+":session:id222")
				conn.Command("DEL", prefix+":session:id333")
				conn.Command("DEL", inpFullKey)
				conn.GenericCommand("EXEC")

				return conn, func(t *testing.T) {
					err := conn.ExpectationsWereMet()
					assert.NoError(t, err)
				}
			},
		},
	}

	for cn, c := range cc {
		c := c

		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			conn, check := c.Conn()

			r := RedisStore{
				pool: &redis.Pool{
					Dial: func() (redis.Conn, error) {
						return conn, nil
					},
					Wait:      true,
					MaxActive: 10,
				},
				prefix: prefix,
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if c.Cancelled {
				cancel()
			}

			var err error

			if c.WithExceptions {
				err = r.DeleteByUserKey(ctx, inpKey, inpExceptIDs...)
			} else {
				err = r.DeleteByUserKey(ctx, inpKey)
			}

			check(t)

			if c.Err {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func Test_RedisStore_key(t *testing.T) {
	r := RedisStore{prefix: "test"}
	assert.Equal(t, "test:session:hello", r.key(false, "hello"))
	assert.Equal(t, "test:user:hello", r.key(true, "hello"))
}

func Test_parse(t *testing.T) {
	inp := sessionup.Session{
		UserKey:   "u123",
		ID:        "id123",
		ExpiresAt: time.Now().UTC().Add(time.Hour * 24).Round(0),
		CreatedAt: time.Now().UTC().Round(0),
		IP:        net.ParseIP("127.0.0.1"),
	}
	inp.Agent.OS = "gnu/linux"
	inp.Agent.Browser = "firefox"

	cc := map[string]struct {
		Data map[string]string
		Err  bool
	}{
		"Invalid creation format": {
			Data: map[string]string{
				"user_key":      inp.UserKey,
				"id":            inp.ID,
				"created_at":    "123",
				"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
				"ip":            inp.IP.String(),
				"agent_os":      inp.Agent.OS,
				"agent_browser": inp.Agent.Browser,
			},
			Err: true,
		},
		"Invalid expiration format": {
			Data: map[string]string{
				"user_key":      inp.UserKey,
				"id":            inp.ID,
				"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
				"expires_at":    "123",
				"ip":            inp.IP.String(),
				"agent_os":      inp.Agent.OS,
				"agent_browser": inp.Agent.Browser,
			},
			Err: true,
		},
		"Successful execution": {
			Data: map[string]string{
				"user_key":      inp.UserKey,
				"id":            inp.ID,
				"created_at":    inp.CreatedAt.Format(time.RFC3339Nano),
				"expires_at":    inp.ExpiresAt.Format(time.RFC3339Nano),
				"ip":            inp.IP.String(),
				"agent_os":      inp.Agent.OS,
				"agent_browser": inp.Agent.Browser,
			},
		},
	}

	for cn, c := range cc {
		c := c

		t.Run(cn, func(t *testing.T) {
			t.Parallel()

			res, err := parse(c.Data)

			if c.Err {
				assert.Error(t, err)
				assert.Zero(t, res)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, inp, res)
		})
	}
}
