package luavm

import (
	"testing"
)

func TestLuaRedis(t *testing.T) {
	pool := NewLuaPool()
	vm := pool.Get()
	defer pool.Put(vm)

	redis := newLuaRedis()
	if err := redis.Init("192.168.1.30:6379", "easy", 0); err != nil {
		t.Fatal(err)
	}
	vm.PreLoadModule("redis", redis.Loader)

	script := `
		local redis = require("redis")
		err = redis.set("test", 2)
		if (err ~= nil) then
			error(err)
		end
		
		ret, err = redis.get("test")
		if(ret == nil) then
			error(err)
		end
		if(ret ~= "2") then
			error("redis get 不匹配")
		end

		
		err = redis.del("test")
		if(err ~= nil) then
			error(err)
		end
		
		err = redis.hset("test", "easy", 1)
		if(err ~= nil) then
			error(err)
		end
		ret, err = redis.hget("test", "easy")
		if(ret == nil) then
			error(err)
		end
		if(ret ~= "1") then
			error("redis hget 不相符")
		end

		err = redis.hdel("test", "easy")
		if(err ~= nil) then
			error(err)
		end
	`

	if err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkRedis(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		pool := NewLuaPool()

		redis := newLuaRedis()
		if err := redis.Init("192.168.1.30:6379", "easy", 0); err != nil {
			b.Fatal(err)
		}

		script := `
			local redis = require("redis")
			--[[
			err = redis.set("test", 2)
			if (err ~= nil) then
				error(err)
			end
			--]]
			ret, err = redis.get("name")
			if(ret == nil) then
				error(err)
			end
			if(ret ~= "zhangsan") then
				error("redis get 不匹配")
			end
		`
		for pb.Next() {
			vm := pool.Get()
			vm.PreLoadModule("redis", redis.Loader)
			if err := vm.DoString(script); err != nil {
				b.Fatal(err)
			}
			pool.Put(vm)
		}

	})
}
