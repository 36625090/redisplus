package redisplus

import (
	"errors"
	"fmt"
	"gopkg.in/redis.v5"
	"strings"
	"time"
)

var ErrorResultNotOK = errors.New("result is not OK")
var ErrorResultNotTrue = errors.New("result is not true")
var ErrorInputValuesIsNil = errors.New("input values is nil")

var _ RedisCli = (*redisView)(nil)

type redisView struct {
	prefix string
	cmd    RedisCmd
	pubSub redis.PubSub
}

func NewRedisCli(config *Config, prefix string) (RedisCli, error) {
	cmd, err := NewRedisCmd(config)
	if err != nil {
		return nil, err
	}
	view := &redisView{
		cmd: cmd,
	}
	if config.KeyPrefix != "" && prefix != "" {
		view.prefix = strings.Join([]string{config.KeyPrefix, prefix}, RedisKeySep)
	} else if config.KeyPrefix != "" {
		view.prefix = config.KeyPrefix
	} else {
		view.prefix = prefix
	}
	return view, nil
}

func (r *redisView) KeyPrefix() string {
	return r.prefix
}
func (r *redisView) NativeCmd() RedisCmd {
	return r.cmd
}

func (r *redisView) SetNX(key string, value []byte, duration string) (bool, error) {
	if duration != "" {
		timeout, err := time.ParseDuration(duration)
		if nil != err {
			return false, err
		}
		return r.cmd.SetNX(r.expandKey(key), value, timeout).Result()
	}
	return r.cmd.SetNX(r.expandKey(key), value, 0).Result()
}

func (r *redisView) Scan(cursor uint64, match string, count int64) ([]string, error) {
	result, _, err := r.cmd.Scan(cursor, r.expandKey(match), count).Result()
	if nil != err {
		return nil, err
	}
	return result, err
}

func (r *redisView) Exists(key string) (bool, error) {
	ek := r.expandKey(key)
	has, err := r.cmd.Exists(ek).Result()
	if nil != err && err != redis.Nil {
		return false, fmt.Errorf("get value with key: %s, err: %s", ek, err)
	}
	return has, nil
}

func (r *redisView) Get(key string) ([]byte, error) {
	ek := r.expandKey(key)
	result, err := r.cmd.Get(ek).Result()
	if nil != err && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("get value with key: %s, err: %s", ek, err)
	}

	return []byte(result), nil
}

func (r *redisView) Set(key string, value []byte, duration string) error {
	if duration != "" {
		timeout, err := time.ParseDuration(duration)
		if nil != err {
			return err
		}
		return r.cmd.Set(r.expandKey(key), value, timeout).Err()
	}
	return r.cmd.Set(r.expandKey(key), value, 0).Err()
}
func (r *redisView) SetTimeout(key string, value []byte, duration time.Duration) error {
	return r.cmd.Set(r.expandKey(key), value, duration).Err()
}
func (r *redisView) Del(keys ...string) (int64, error) {
	var all []string
	for _, key := range keys {
		all = append(all, r.expandKey(key))
	}
	return r.cmd.Del(all...).Result()
}

func (r *redisView) Expire(key string, duration string) error {
	timeout, err := time.ParseDuration(duration)
	if nil != err {
		return err
	}

	return wrapResult(func() (interface{}, error) {
		return r.cmd.Expire(r.expandKey(key), timeout).Result()
	})
}
