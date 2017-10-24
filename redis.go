package luavm

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
	lua "github.com/yuin/gopher-lua"
)

//luaRedis redis插件
type luaRedis struct {
	pool *redis.Pool
}

//newLuaRedis 这里会初始化redis连接池
func newLuaRedis() *luaRedis {
	r := new(luaRedis)

	return r
}

//Init 初始化redis插件
func (r *luaRedis) Init(addr, passwd string, database int) (err error) {
	r.pool = &redis.Pool{
		MaxIdle:     100,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", addr)
			if err != nil {
				return nil, err
			}
			if _, err = c.Do("AUTH", passwd); err != nil {
				c.Close()
				return nil, err
			}
			if _, err = c.Do("select", database); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	return nil
}

//Loader ...
func (r *luaRedis) Loader(L *lua.LState) int {
	var exports = map[string]lua.LGFunction{
		"get":  r.get,
		"set":  r.set,
		"del":  r.del,
		"hget": r.hget,
		"hset": r.hset,
		"hdel": r.hdel,
	}
	mod := L.SetFuncs(L.NewTable(), exports)
	L.Push(mod)
	return 1
}

func getNumArgs(num int, L *lua.LState) (args []interface{}, err error) {
	if L.GetTop() != num {
		err = fmt.Errorf("参数个数不匹配[%d]-[%d]", num, L.GetTop())
		return
	}
	for i := 1; i <= num; i++ {
		arg := L.Get(i)
		//这里只支持字符串和数字
		switch arg.Type() {
		case lua.LTString:
			args = append(args, arg.String())
		case lua.LTNumber:
			args = append(args, arg.String())
		default:
			err = fmt.Errorf("参数[%d]类型错误", i)
			return
		}
	}
	return
}

func pushTwoErr(err error, L *lua.LState) {
	L.Push(lua.LNil)
	L.Push(lua.LString(err.Error()))
	return
}

func (r *luaRedis) get(L *lua.LState) int {
	conn := r.pool.Get()
	defer conn.Close()

	args, err := getNumArgs(1, L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	ret, err := redis.String(conn.Do("get", args[0]))
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	L.Push(lua.LString(ret))
	return 1
}

func (r *luaRedis) set(L *lua.LState) int {
	conn := r.pool.Get()
	defer conn.Close()

	args, err := getNumArgs(2, L)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	_, err = conn.Do("set", args[0], args[1])
	if err != nil {
		pushErr(err, L)
		return 1
	}
	return 0
}

func (r *luaRedis) del(L *lua.LState) int {
	conn := r.pool.Get()
	defer conn.Close()

	args, err := getNumArgs(1, L)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	_, err = conn.Do("del", args[0])
	if err != nil {
		pushErr(err, L)
		return 1
	}
	return 0
}

func (r *luaRedis) hget(L *lua.LState) int {
	conn := r.pool.Get()
	defer conn.Close()

	args, err := getNumArgs(2, L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	ret, err := redis.String(conn.Do("hget", args[0], args[1]))
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	L.Push(lua.LString(ret))
	return 1
}

func (r *luaRedis) hset(L *lua.LState) int {
	conn := r.pool.Get()
	defer conn.Close()

	args, err := getNumArgs(3, L)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	_, err = conn.Do("hset", args[0], args[1], args[2])
	if err != nil {
		pushErr(err, L)
		return 1
	}
	return 0
}

func (r *luaRedis) hdel(L *lua.LState) int {
	conn := r.pool.Get()
	defer conn.Close()

	args, err := getNumArgs(2, L)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	_, err = conn.Do("hdel", args[0], args[1])
	if err != nil {
		pushErr(err, L)
		return 1
	}
	return 0
}
