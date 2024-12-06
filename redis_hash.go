package redisplus

import (
	"errors"
	"fmt"
	"gopkg.in/redis.v5"
)

func (r *redisView) HSetNX(key, field string, value []byte) error {
	return wrapResult(func() (interface{}, error) {
		return r.cmd.HSetNX(r.expandKey(key), field, value).Result()
	})
}

func (r *redisView) HSet(key, field string, value []byte) error {
	return wrapResult(func() (interface{}, error) {
		return r.cmd.HSet(r.expandKey(key), field, value).Result()
	})
}

func (r *redisView) HMSet(key string, Values map[string][]byte) error {
	if nil == Values {
		return ErrorInputValuesIsNil
	}
	in := make(map[string]string)
	for s, bytes := range Values {
		in[s] = string(bytes)
	}
	return wrapResult(func() (interface{}, error) {
		return r.cmd.HMSet(r.expandKey(key), in).Result()
	})
}

func (r *redisView) HGet(key, field string) ([]byte, error) {
	ek := r.expandKey(key)
	result, err := r.cmd.HGet(ek, field).Result()
	if nil != err && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("get value with key: %s, err: %s", ek, err)
	}

	return []byte(result), err
}

func (r *redisView) HMGet(key string, fields ...string) ([][]byte, error) {
	ek := r.expandKey(key)
	result, err := r.cmd.HMGet(ek, fields...).Result()
	if nil != err && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("get value with key: %s, err: %s", ek, err)
	}
	var out [][]byte
	for _, i2 := range result {
		out = append(out, i2.([]byte))
	}
	return out, nil
}

func (r *redisView) HGetAll(key string) (map[string][]byte, error) {
	ek := r.expandKey(key)
	result, err := r.cmd.HGetAll(ek).Result()
	if nil != err && err != redis.Nil {
		return nil, fmt.Errorf("get value with key: %s, err: %s", ek, err)
	}
	out := make(map[string][]byte)
	for s, s2 := range result {
		out[s] = []byte(s2)
	}
	return out, nil
}

func (r *redisView) HDel(key string, fields ...string) (int64, error) {
	return r.cmd.HDel(r.expandKey(key), fields...).Result()
}

func (r *redisView) HLen(key string) (int64, error) {
	return r.cmd.HLen(r.expandKey(key)).Result()
}

func (r *redisView) HKeys(key string) ([]string, error) {
	return r.cmd.HKeys(r.expandKey(key)).Result()
}

func (r *redisView) HValues(key string) ([][]byte, error) {
	return wrapSliceStringToSliceBytes(func() ([]string, error) {
		return r.cmd.HVals(r.expandKey(key)).Result()
	})
}

func (r *redisView) HExists(key, field string) (bool, error) {
	return r.cmd.HExists(r.expandKey(key), field).Result()
}
