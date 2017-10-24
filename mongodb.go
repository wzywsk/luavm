package luavm

import (
	"fmt"

	"github.com/yuin/gluamapper"
	"gopkg.in/mgo.v2/bson"

	lua "github.com/yuin/gopher-lua"
	"gopkg.in/mgo.v2"
)

//luaMgo mongodb插件
type luaMgo struct {
	conn *mgo.Session
}

//newLuaMgo ...
func newLuaMgo() *luaMgo {
	m := new(luaMgo)
	return m
}

//Init 初始化mongodb插件
func (m *luaMgo) Init(addr, user, passwd string) (err error) {
	conn, err := mgo.Dial(addr)
	if err != nil {
		return
	}
	auth := &mgo.Credential{
		Username: user,
		Password: passwd,
	}
	err = conn.Login(auth)
	if err != nil {
		return
	}

	conn.SetMode(mgo.Strong, true)
	conn.SetPoolLimit(128)
	m.conn = conn
	return nil
}

//Loader ...
func (m *luaMgo) Loader(L *lua.LState) int {
	var exports = map[string]lua.LGFunction{
		"insert":  m.insert,
		"update":  m.update,
		"remove":  m.remove,
		"find":    m.find,
		"findone": m.findone,
	}
	mod := L.SetFuncs(L.NewTable(), exports)
	L.Push(mod)
	return 1
}

func getOneValue(L *lua.LState) (dbname string, cname string, arg bson.M, err error) {
	num := L.GetTop()
	if num != 3 {
		err = fmt.Errorf("参数个数错误[%d]", num)
		return
	}
	//数据库名称
	dbname = L.CheckString(1)
	//集合名称
	cname = L.CheckString(2)
	if dbname == "" || cname == "" {
		err = fmt.Errorf("数据库名和集合名不能为空")
		return
	}
	value := L.Get(3)
	arg = make(bson.M)
	switch value.Type() {
	case lua.LTTable:
		if err = gluamapper.Map(value.(*lua.LTable), &arg); err != nil {
			return
		}
	case lua.LTString:
		if err = bson.UnmarshalJSON([]byte(value.String()), &arg); err != nil {
			return
		}
	default:
		err = fmt.Errorf("参数类型错误[%s]", value.Type())
		return
	}
	return
}

func getTwoValue(L *lua.LState) (dbname string, cname string, argone bson.M, argtwo bson.M, err error) {
	num := L.GetTop()
	if num != 4 {
		err = fmt.Errorf("参数个数错误[%d]", num)
		return
	}
	//数据库名称
	dbname = L.CheckString(1)
	//集合名称
	cname = L.CheckString(2)
	if dbname == "" || cname == "" {
		err = fmt.Errorf("数据库名和集合名不能为空")
		return
	}
	value := L.Get(3)
	argone = make(bson.M)
	switch value.Type() {
	case lua.LTTable:
		if err = gluamapper.Map(value.(*lua.LTable), &argone); err != nil {
			return
		}
	case lua.LTString:
		if err = bson.UnmarshalJSON([]byte(value.String()), &argone); err != nil {
			return
		}
	default:
		err = fmt.Errorf("参数类型错误[%s]", value.Type())
		return
	}
	value = L.Get(4)
	argtwo = make(bson.M)
	switch value.Type() {
	case lua.LTTable:
		if err = gluamapper.Map(value.(*lua.LTable), &argtwo); err != nil {
			return
		}
	case lua.LTString:
		if err = bson.UnmarshalJSON([]byte(value.String()), &argtwo); err != nil {
			return
		}
	default:
		err = fmt.Errorf("参数类型错误[%s]", value.Type())
		return
	}
	return
}

func pushErr(err error, L *lua.LState) {
	L.Push(lua.LString(err.Error()))
	return
}
func pushTwoError(err error, L *lua.LState) {
	L.Push(lua.LNil)
	L.Push(lua.LString(err.Error()))
	return
}

func (m *luaMgo) insert(L *lua.LState) int {
	session := m.conn.Copy()
	defer session.Close()

	dbname, cname, cmd, err := getOneValue(L)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	if err = session.DB(dbname).C(cname).Insert(cmd); err != nil {
		pushErr(err, L)
		return 1
	}
	return 0
}

func (m *luaMgo) update(L *lua.LState) int {
	session := m.conn.Copy()
	defer session.Close()
	dbname, cname, argone, argtwo, err := getTwoValue(L)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	err = session.DB(dbname).C(cname).Update(argone, argtwo)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	return 0
}

func (m *luaMgo) remove(L *lua.LState) int {
	session := m.conn.Copy()
	defer session.Close()

	dbname, cname, cmd, err := getOneValue(L)
	if err != nil {
		pushErr(err, L)
		return 1
	}
	if err = session.DB(dbname).C(cname).Remove(cmd); err != nil {
		pushErr(err, L)
		return 1
	}
	return 0
}

func (m *luaMgo) find(L *lua.LState) int {
	session := m.conn.Copy()
	defer session.Close()

	dbname, cname, cmd, err := getOneValue(L)
	if err != nil {
		pushTwoError(err, L)
		return 2
	}
	query := session.DB(dbname).C(cname).Find(cmd)
	count, err := query.Count()
	if err != nil {
		pushTwoError(err, L)
		return 2
	}
	result := make([]bson.M, count)
	if err = query.All(&result); err != nil {
		pushTwoError(err, L)
		return 2
	}
	table := L.NewTable()
	index := 0
	for _, doc := range result {
		one := L.NewTable()
		for key, value := range doc {
			var v lua.LValue
			switch value.(type) {
			case int:
				v = lua.LNumber(float64(value.(int)))
			case float32:
				v = lua.LNumber(float64(value.(float32)))
			case float64:
				v = lua.LNumber(value.(float64))
			case string:
				v = lua.LString(value.(string))
			default:
				v = lua.LString(fmt.Sprintf("%v", value))
			}
			one.RawSetString(key, v)
		}
		table.RawSetInt(index, one)
		index++
	}
	L.Push(table)
	return 1
}

func (m *luaMgo) findone(L *lua.LState) int {
	session := m.conn.Copy()
	defer session.Close()
	dbname, cname, cmd, err := getOneValue(L)
	if err != nil {
		pushTwoError(err, L)
		return 2
	}
	result := make(bson.M)
	if err = session.DB(dbname).C(cname).Find(cmd).One(&result); err != nil {
		pushTwoError(err, L)
		return 2
	}
	table := L.NewTable()
	for key, value := range result {
		var v lua.LValue
		switch value.(type) {
		case int:
			v = lua.LNumber(float64(value.(int)))
		case float32:
			v = lua.LNumber(float64(value.(float32)))
		case float64:
			v = lua.LNumber(value.(float64))
		case string:
			v = lua.LString(value.(string))
		default:
			v = lua.LString(fmt.Sprintf("%v", value))
		}
		table.RawSetString(key, v)
	}
	L.Push(table)
	return 1
}
