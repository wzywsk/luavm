package luavm

import (
	"testing"
)

func TestMongodb(t *testing.T) {
	pool := NewLuaPool()
	vm := pool.Get()
	defer pool.Put(vm)

	mgo := newLuaMgo()
	if err := mgo.Init("192.168.1.30:27017", "root", "root"); err != nil {
		t.Fatal(err)
	}
	vm.PreLoadModule("mongodb", mgo.Loader)

	script := `
			local mgo = require("mongodb")
			--data = {["name"]="zhangsan",["age"]=18}
			data = '{"name":"lisi", "age":19}'
			err = mgo.insert("test", "easy", data)
			if(err ~= nil) then
				error(err)
			end

			ret, err = mgo.find("test","easy","{}")
			if (ret == nil) then
				error(err)
			end
			if((ret[0].name ~= "lisi") or (ret[0].age ~= 19)) then
				error("mongodb find 不匹配")
			end


			data, err = mgo.findone("test", "easy", '{"name":"lisi"}')
			if (data == nil) then
				error(err)
			end
			if((data.name ~= "lisi") or (data.age ~= 19)) then
				error("mongodb findone 不匹配")
			end

			err = mgo.update("test", "easy", '{"name":"lisi"}', '{"$set":{"age":20}}')
			if (err ~= nil) then
				error(err)
			end
			data, err = mgo.findone("test", "easy", '{"name":"lisi"}')
			if (data == nil) then
				error(err)
			end
			if(data.age ~= 20) then
				error("mongodb update 不匹配")
			end

			err = mgo.remove("test", "easy", '{"name":"lisi"}')
			if (err ~= nil) then
				error(err)
			end
	`
	if err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMongodb(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		pool := NewLuaPool()

		mgo := newLuaMgo()
		if err := mgo.Init("192.168.1.30:27017", "root", "root"); err != nil {
			b.Fatal(err)
		}

		script := `
			local mgo = require("mongodb")
			ret, err = mgo.find("test","easy","{}")
			if (ret == nil) then
				error(err)
			end
			if((ret[0].name ~= "lisi") or (ret[0].age ~= 19)) then
				error("mongodb find 不匹配")
			end
		`
		for pb.Next() {
			vm := pool.Get()
			vm.PreLoadModule("mongodb", mgo.Loader)
			if err := vm.DoString(script); err != nil {
				b.Fatal(err)
			}
			pool.Put(vm)
		}

	})
}
