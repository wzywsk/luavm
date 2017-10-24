package luavm

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestMysql(t *testing.T) {
	pool := NewLuaPool()
	vm := pool.Get()
	defer pool.Put(vm)

	conf := []string{
		"main",
		"root:easy@tcp(192.168.1.30:3306)/test?charset=utf8",
	}
	my := newLuaMysql()
	if err := my.Init(conf); err != nil {
		t.Fatal(err)
	}
	vm.PreLoadModule("mysql", my.Loader)

	script := `
		local mysql = require("mysql")
		conn, err = mysql.connect("main")
		if(conn == nil) then
			error(err)
		end

		--测试insert
		conn.begin()
		ret ,err = conn.exec("insert into user values (?,?)", "lisi", 25)
		if(ret == nil) then
			error(err)
		end
		conn.commit()

		rows, err = conn.query("select * from user where name = ?", "lisi")
		if(rows == nil) then
			error(err)
		end
		if(rows[0].name ~= "lisi") or (rows[0].age ~= 25) then
			error("mysql insert query 不符")
		end

		row, err = conn.queryrow("select * from user where name = ?", "lisi")
		if (row == nil) then
			error(err)
		end
		if(row.name ~= "lisi") or (row.age ~= 25) then
			error("mysql insert queryrow 不符")
		end

		--测试update
		conn.begin()
		result ,err = conn.exec("update user set age = ? where name = ?", 20, "lisi")
		if (result == nil) then
			error(err)
		end
		conn.commit()
		if(result.affected ~= 1) then
			error("mysql update affected 不相符")
		end
		`
	if err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMysql(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		pool := NewLuaPool()

		conf := []string{
			"main",
			"root:easy@tcp(192.168.1.30:3306)/test?charset=utf8",
		}
		my := newLuaMysql()
		if err := my.Init(conf); err != nil {
			b.Fatal(err)
		}

		script := `
		local mysql = require("mysql")
		conn, err = mysql.connect("main")
		if(conn == nil) then
			error(err)
		end
		--[[
		row, err = conn.queryrow("select * from user where name = ?", "lisi")
		if (row == nil) then
			error(err)
		end
		--]]
		conn.begin()
		result ,err = conn.exec("update user set age = ? where name = ?", 20, "lisi")
		if (result == nil) then
			error(err)
		end
		conn.commit()
		`
		for pb.Next() {
			vm := pool.Get()
			vm.PreLoadModule("mysql", my.Loader)
			if err := vm.DoString(script); err != nil {
				b.Fatal(err)
			}
			pool.Put(vm)
		}

	})
}
