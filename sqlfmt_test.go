package luavm

import (
	"testing"

	_ "github.com/go-sql-driver/mysql"
)

func TestFmtSelect(t *testing.T) {
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
	my := newLuaMySQL()
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

		local testTable = {}
		testTable.Name = ""
		testTable.Age = ""

		result, err = conn.select("info",testTable,"Name = '%v'","hehe")
		if(result == nil) then
			error(err)
		end
		print(result[1].Age)
	`
	if _, _, err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
}
