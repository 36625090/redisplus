package redisplus

import (
	"errors"
	"fmt"
	"gopkg.in/redis.v5"
)

func (r *redisView) LRem(key string, count int64, value []byte) (int64, error) {
	return r.cmd.LRem(r.expandKey(key), count, value).Result()
}

func (r *redisView) LIndex(key string, index int64) ([]byte, error) {
	result, err := r.cmd.LIndex(r.expandKey(key), index).Result()
	if nil != err {
		return nil, err
	}
	return []byte(result), nil
}

func (r *redisView) LTrim(key string, start, stop int64) error {
	return wrapResult(func() (interface{}, error) {
		return r.cmd.LTrim(r.expandKey(key), start, stop).Result()
	})
}

func (r *redisView) LSet(key string, index int64, value []byte) error {
	return wrapResult(func() (interface{}, error) {
		return r.cmd.LSet(r.expandKey(key), index, value).Result()
	})
}

func (r *redisView) LPush(key string, values ...[]byte) (int64, error) {
	var vals []interface{}
	for _, value := range values {
		vals = append(vals, value)
	}
	return r.cmd.LPush(r.expandKey(key), vals...).Result()
}

func (r *redisView) LAppend(key string, values ...[]byte) (int64, error) {
	var vals []interface{}
	for _, value := range values {
		vals = append(vals, value)
	}
	return r.cmd.RPush(r.expandKey(key), vals...).Result()
}

func (r *redisView) LPop(key string) ([]byte, error) {
	ek := r.expandKey(key)
	result, err := r.cmd.LPop(ek).Result()
	if nil != err && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("get value with key: %s, err: %s", ek, err)
	}
	return []byte(result), nil
}

func (r *redisView) LRPop(key string) ([]byte, error) {
	ek := r.expandKey(key)
	result, err := r.cmd.RPop(ek).Result()
	if nil != err && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("get value with key: %s, err: %s", ek, err)
	}
	return []byte(result), nil
}

func (r *redisView) LRange(key string, start, stop int64) ([][]byte, error) {
	return wrapSliceStringToSliceBytes(func() ([]string, error) {
		return r.cmd.LRange(r.expandKey(key), start, stop).Result()
	})
}

func (r *redisView) LLen(key string) (int64, error) {
	return r.cmd.LLen(r.expandKey(key)).Result()
}

func (r *redisView) LInsert(key string, op InsertOP, pivot, value []byte) (int64, error) {
	return r.cmd.LInsert(r.expandKey(key), string(op), pivot, value).Result()
}
