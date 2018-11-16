package luavm

import (
	"database/sql"
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestMysqlCache(t *testing.T) {
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
		--测试queryCache
		ret ,err = conn.queryCache("test", "select * from info", 10)
		if(ret == nil) then
			error(err)
		end
		`
	if _, _, err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkMysqlCache(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		db, err := sql.Open("mysql", "root:easy@tcp(192.168.1.22:3306)/test?charset=utf8")
		if err != nil {
			b.Fatal(err)
		}
		c := NewCache(db)
		for pb.Next() {
			_, err = c.QueryCache("test", "select * from info", 10)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
