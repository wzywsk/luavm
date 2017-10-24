package luavm

import (
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	lua "github.com/yuin/gopher-lua"
)

var vmPool *LuaPool

func TestLuaVM(t *testing.T) {
	vmPool = NewLuaPool()

	for i := 0; i < 1; i++ {
		t.Run("golang注入lua全局变量", luaGetGolang)

		t.Run("golang获取lua全局变量", golangGetLua)

		t.Run("table测试", tableTest)

		t.Run("类型转换测试", typeConvert)
	}
}

func luaGetGolang(t *testing.T) {
	t.Parallel()
	vm := vmPool.Get()
	defer vmPool.Put(vm)

	//测试easy全局变量
	vm.SetEasyAttr("name", lua.LString("lisi"))
	vm.SetEasyAttr("age", lua.LNumber(25))
	vm.SetEasyAttr("test", vm.l.NewFunction(func(L *lua.LState) int {
		L.Push(lua.LString("hello"))
		return 1
	}))
	//注入全局变量
	vm.SetGlobal("name", lua.LString("zhangsan"))
	vm.SetGlobal("age", lua.LNumber(25))

	str := `
			--测试全局变量
			if(name ~= "zhangsan") or (age ~= 25) then
				error("name全局变量不相符")
			end
			--测试easy全局变量
			if(easy == nil) then
				error("easy全局变量为空")
			end
			--测试easy注入函数
			local ret = easy:test()
			if(ret ~= "hello") then
				error("easy注入函数返回值不符")
			end
			--测试easy属性
			if(easy.name ~= "lisi") or (easy.age ~= 25) then
				error("easy属性不相符")
			end
			`
	if err := vm.DoString(str); err != nil {
		t.Error(err)
	}
}

func golangGetLua(t *testing.T) {
	t.Parallel()
	vm := vmPool.Get()
	defer vmPool.Put(vm)

	script := `
			name = "easy"
			age = 25
			function test(name) 
				return name
			end
		`
	if err := vm.DoString(script); err != nil {
		t.Error(err)
	}
	if name := vm.GetGlobal("name").String(); name != "easy" {
		t.Fatalf("name变量不相符[%s]", name)
	}
	if age, err := strconv.Atoi(vm.GetGlobal("age").String()); err != nil {
		t.Fatal(err)
	} else {
		if age != 25 {
			t.Fatalf("age变量不相符[%d]", age)
		}
	}
	ret, err := vm.CallGlobal("test", lua.LString("easy"))
	if err != nil {
		t.Fatal(err)
	}
	if ret.String() != "easy" {
		t.Fatalf("test函数返回不符[%s]", ret.String())
	}

}

func tableTest(t *testing.T) {
	t.Parallel()
	vm := vmPool.Get()
	defer vmPool.Put(vm)

	table := vm.NewLuaTable()
	table.RawSetString("name", lua.LString("zhangsan"))

	vm.SetGlobal("test", table)
	script := `
			test.age = 25		
		`
	if err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}

	test := vm.GetGlobal("test")
	//这里测试执行lua后,先前的变量会不会保留
	if name := vm.GetField(test, "name"); name.String() != "zhangsan" {
		t.Fatalf("table中name值不符[%s]", name.String())
	}
	//这里测试lua中对table的赋值
	if age, err := strconv.Atoi(vm.GetField(test, "age").String()); err != nil {
		t.Fatal(err)
	} else {
		if age != 25 {
			t.Fatalf("age变量不相符[%d]", age)
		}
	}
}

func typeConvert(t *testing.T) {
	t.Parallel()
	vm := vmPool.Get()
	defer vmPool.Put(vm)

	//测试golang类型转换为lua类型
	str := "hello"
	num := 8
	user := struct {
		Name string
		Age  int
	}{
		"lisi",
		25,
	}

	dst := vm.ConvLuaType(str)
	vm.SetGlobal("str", dst)

	dst = vm.ConvLuaType(num)
	vm.SetGlobal("num", dst)

	dst = vm.ConvLuaType(user)
	vm.SetGlobal("user", dst)

	script := `
			if(type(str) ~= "string") then
				error("string类型转换错误")
			end
			if(type(num) ~= "number") then
				error("number类型转换错误")
			end
			if(user.name ~= "lisi") or (user.age ~= 25) then
				error("user结构体转换错误")
			end
		`
	if err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
	//测试lua类型转换为goalng类型
	script = `
			table  = {}
			table.name = "zhangsan"
			table.age = 18
			string = "hello"
			num = 26
		`
	if err := vm.DoString(script); err != nil {
		t.Fatal(err)
	}
	table := vm.GetGlobal("table")
	if err := vm.ConvGoType(table, &user); err != nil {
		t.Fatal(err)
	}
	if user.Name != "zhangsan" || user.Age != 18 {
		t.Fatalf("转换为golang类型不符")
	}
	strtype := vm.GetGlobal("string")
	if strtype.Type() != lua.LTString {
		t.Fatal("转换为golang类型string不符")
	}
	numtype := vm.GetGlobal("num")
	if numtype.Type() != lua.LTNumber {
		t.Fatal("转换为golang类型num不符")
	}

}

func BenchmarkLua(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		vmPool := NewLuaPool()
		if err := vmPool.Init(); err != nil {
			b.Fatal(err)
		}
		for pb.Next() {
			vm := vmPool.Get()
			if err := vm.DoString(`local m`); err != nil {
				b.Fatal(err)
			}
			vmPool.Put(vm)
		}
	})
}
