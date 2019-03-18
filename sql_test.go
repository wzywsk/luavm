package luavm

import (
	"testing"

	_ "github.com/denisenkom/go-mssqldb"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

func TestMysql(t *testing.T) {
	//t.Skip()
	pool := NewLuaPool()
	vm := pool.Get()
	defer pool.Put(vm)

	conf := []*sqlConfig{
		&sqlConfig{
			Name:     "mysql-main",
			Type:     "mysql",
			Addr:     "192.168.1.22:3306",
			User:     "root",
			Passwd:   "easy",
			DataBase: "test",
		},
	}
	my := newluaMySQL()
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
		if(rows[1].name ~= "lisi") or (rows[1].age ~= 25) then
			error("mysql insert query 不符")
		end
		row, err = conn.queryRow("select * from user where name = ?", "lisi1")
		if (row == nil) and (err ~= "sql: no rows in result set") then
			error(err)
		end
		if (row ~= nil) and (err ~= "sql: no rows in result set") then
			if(row.name ~= "lisi") or (row.age ~= 25) then
				error("mysql insert queryrow 不符")
			end
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
	if _, _, err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
}

func TestMssql(t *testing.T) {
	t.Skip()
	pool := NewLuaPool()
	vm := pool.Get()
	defer pool.Put(vm)

	conf := []*sqlConfig{
		&sqlConfig{
			Name:     "mssql-main",
			Type:     "mssql",
			Addr:     "192.168.1.23:1433",
			User:     "sa",
			Passwd:   "`1easy",
			DataBase: "test",
		},
	}
	my := newluaMsSQL()
	if err := my.Init(conf); err != nil {
		t.Fatal(err)
	}
	vm.PreLoadModule("mssql", my.Loader)

	script := `
		local mysql = require("mssql")
		conn, err = mysql.connect("main")
		if(conn == nil) then
			error(err)
		end

		--测试insert
		conn.begin()
		ret ,err = conn.exec("insert into [dbo].[user] values (?,?)", "lisi", 25)
		if(ret == nil) then
			error(err)
		end
		conn.commit()
		rows, err = conn.query("select * from [dbo].[user] where name = ?", "lisi")
		if(rows == nil) then
			error(err)
		end
		if(rows[1].name ~= "lisi") or (rows[1].age ~= 25) then
			error("mysql insert query 不符")
		end
		row, err = conn.queryRow("select * from [dbo].[user] where name = ?", "lisi1")
		if (row == nil) and (err ~= "sql: no rows in result set") then
			error(err)
		end
		if (row ~= nil) and (err ~= "sql: no rows in result set") then
			if(row.name ~= "lisi") or (row.age ~= 25) then
				error("mysql insert queryrow 不符")
			end
		end

		--测试update
		conn.begin()
		result ,err = conn.exec("update [dbo].[user] set age = ? where name = ?", 20, "lisi")
		if (result == nil) then
			error(err)
		end
		conn.commit()
		if(result.affected ~= 1) then
			error("mysql update affected 不相符")
		end
		`
	if _, _, err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMssql(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		pool := NewLuaPool()

		conf := []*sqlConfig{
			&sqlConfig{
				Name:     "mssql-main",
				Type:     "mssql",
				Addr:     "192.168.1.23:1433",
				User:     "sa",
				Passwd:   "`1easy",
				DataBase: "test",
			},
		}
		my := newluaMsSQL()
		if err := my.Init(conf); err != nil {
			b.Fatal(err)
		}

		script := `
		local mysql = require("sql")
		conn, err = mysql.connect("mssql-main")
		if(conn == nil) then
			error(err)
		end
		--[[
		row, err = conn.queryrow("select * from [dbo].[user] where name = ?", "lisi")
		if (row == nil) then
			error(err)
		end
		--]]
		conn.begin()
		result ,err = conn.exec("update [dbo].[user] set age = ? where name = ?", 20, "lisi")
		if (result == nil) then
			error(err)
		end
		conn.commit()
		`
		for pb.Next() {
			vm := pool.Get()
			vm.PreLoadModule("mysql", my.Loader)
			if _, _, err := vm.DoString(script); err != nil {
				b.Fatal(err)
			}
			pool.Put(vm)
		}

	})
}

func BenchmarkMysql(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		pool := NewLuaPool()

		conf := []*sqlConfig{
			&sqlConfig{
				Name:     "mysql-main",
				Type:     "mysql",
				Addr:     "192.168.1.22:3306",
				User:     "root",
				Passwd:   "easy",
				DataBase: "test",
			},
		}
		my := newluaMySQL()
		if err := my.Init(conf); err != nil {
			b.Fatal(err)
		}

		script := `
		local mysql = require("sql")
		conn, err = mysql.connect("mysql-main")
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
			vm.PreLoadModule("sql", my.Loader)
			if _, _, err := vm.DoString(script); err != nil {
				b.Fatal(err)
			}
			pool.Put(vm)
		}

	})
}
