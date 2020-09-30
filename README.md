# sessionup-redisstore

[![Build status](https://travis-ci.org/swithek/sessionup-redisstore.svg?branch=master)](https://travis-ci.org/swithek/sessionup-redisstore)
[![GoDoc](https://godoc.org/github.com/swithek/sessionup-redisstore?status.png)](https://godoc.org/github.com/swithek/sessionup-redisstore)

Redis session store implementation for [sessionup](https://github.com/swithek/sessionup)

## Installation
```
go get github.com/swithek/sessionup-redisstore
```

## Usage
Create and activate a new RedisStore:
```go
// create redigo connection pool
pool := &redis.Pool{
    Dial: func() (redis.Conn, error) {
         return redis.Dial("tcp", "localhost:6379")
    },
}

store, err := redisstore.New(pool, "customers")
if err != nil {
      // handle error
}

manager := sessionup.NewManager(store)
```
