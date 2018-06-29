package luavm

import (
	"database/sql"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/yuin/gopher-lua"
)

//luaSQL lua容器sql注入插件,将根据配置初始化多个数据库
type luaSQL struct {
	lock      *sync.Mutex
	db        map[string]*sql.DB
	name2type map[string]string //记录名字对应数据库类型
}

//newLuaSQL ...
func newLuaSQL() *luaSQL {
	l := new(luaSQL)
	l.lock = new(sync.Mutex)
	l.db = make(map[string]*sql.DB, 10)
	l.name2type = make(map[string]string, 10)
	return l
}

//Init 初始化sql插件
//格式为第一个名称,第二个source
func (l *luaSQL) Init(cs []*sqlConfig) (err error) {
	var source string
	for _, c := range cs {
		var db *sql.DB
		switch c.Type {
		case "mysql":
			source = fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", c.User, c.Passwd, c.Addr, c.DataBase)
			db, err = sql.Open("mysql", source)
			if err != nil {
				return err
			}
		case "mssql":
			query := url.Values{}
			query.Add("connection+timeout", "30")
			query.Add("encrypt", "disable")
			query.Add("database", c.DataBase)

			u := &url.URL{
				Scheme:   "sqlserver",
				User:     url.UserPassword(c.User, c.Passwd),
				Host:     c.Addr,
				RawQuery: query.Encode(),
			}
			db, err = sql.Open("mssql", u.String())
			if err != nil {
				return err
			}
		default:
			err = fmt.Errorf("未知数据库类型[%s]", c.Type)
			return
		}
		l.db[c.Name] = db
		l.name2type[c.Name] = c.Type
	}
	return nil
}

//Loader ...
func (l *luaSQL) Loader(L *lua.LState) int {
	var exports = map[string]lua.LGFunction{
		"connect": l.connect,
	}
	mod := L.SetFuncs(L.NewTable(), exports)
	L.Push(mod)
	return 1
}

func (l *luaSQL) connect(L *lua.LState) int {
	name := L.CheckString(1)
	db := l.db[name]
	if db == nil {
		pushTwoErr(fmt.Errorf("数据库[%s]不存在", name), L)
		return 2
	}
	sqlType := l.name2type[name]
	m := newSQLState(db, sqlType)
	my := L.NewTable()
	my.RawSetString("query", L.NewFunction(m.query))
	my.RawSetString("queryRow", L.NewFunction(m.queryrow))
	my.RawSetString("exec", L.NewFunction(m.exec))
	my.RawSetString("begin", L.NewFunction(m.begin))
	my.RawSetString("commit", L.NewFunction(m.commit))
	my.RawSetString("rollback", L.NewFunction(m.rollback))
	my.RawSetString("logger", L.NewFunction(m.logger))
	my.RawSetString("insert", L.NewFunction(m.sqlInsert))
	my.RawSetString("select", L.NewFunction(m.sqlSelect))
	my.RawSetString("update", L.NewFunction(m.sqlUpdate))
	//添加sql事务状态
	ctx := L.Context()
	//注册数据库连接状态
	if addFunc, ok := ctx.Value("addTran").(func(*sqlState)); ok {
		addFunc(m)
	}
	//初始化日志接口
	if logger, ok := ctx.Value(loggerInterface).(Logger); ok {
		m.SetLogger(logger)
	}
	L.Push(my)
	return 1
}

//同一时间只能维护一个事务
type sqlState struct {
	status  int32  //记录事务状态
	sqlType string //数据库类型
	db      *sql.DB
	tx      *sql.Tx
	l       Logger
}

func newSQLState(db *sql.DB, sqlType string) *sqlState {
	m := new(sqlState)
	m.db = db
	m.sqlType = sqlType
	return m
}

//SetLogger ...
func (my *sqlState) SetLogger(l Logger) {
	my.l = l
}

//GetArgs 获取诸如(cmd string, a ...interface{})形式的参数
func GetArgs(L *lua.LState) (cmd string, args []interface{}, err error) {
	num := L.GetTop()
	if num < 1 {
		err = fmt.Errorf("参数个数错误[%d]", num)
		return
	}
	cmd = L.CheckString(1)
	args = make([]interface{}, num-1)
	if num > 1 {
		for i := 2; i <= num; i++ {
			arg := L.Get(i)
			switch arg.Type() {
			case lua.LTString:
				args[i-2] = arg.String()
			case lua.LTNumber:
				a, _ := strconv.Atoi(arg.String())
				args[i-2] = a
			default:
				err = fmt.Errorf("参数类型错误[%d]", i)
				return
			}
		}
	}
	return
}

//插入sql日志表专用,不走事务,直接返回错误
func (my *sqlState) logger(L *lua.LState) int {
	str := L.CheckString(1)
	_, err := my.db.Exec(str)
	if err != nil {
		if l := len(str); l > 0 && str[l-1] == '\n' {
			my.l.Error("  <%s> logger error: %v\n  <sql->\n%s  <-sql>\n", my.sqlType, err.Error(), str)
		} else {
			my.l.Error("  <%s> logger error: %v\n  <sql->\n%s\n  <-sql>\n", my.sqlType, err.Error(), str)
		}
		return 0
	}
	return 0
}

func (my *sqlState) query(L *lua.LState) int {
	cmd, args, err := GetArgs(L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	rows, err := my.db.Query(cmd, args...)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	defer rows.Close()

	//获取每一行的数据类型和个数
	cols, err := rows.ColumnTypes()
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	m := make([]interface{}, len(cols))
	values := make([]sql.RawBytes, len(cols))
	for i := range m {
		m[i] = &values[i]
	}
	//这里存放读取的所有数据
	all := L.NewTable()
	//读出所有数据并转换为lua数据类型
	//lua 数组下标从1开始
	index := 1
	for rows.Next() {
		if err = rows.Scan(m...); err != nil {
			pushTwoErr(err, L)
			return 2
		}
		table := L.NewTable()
		for i := range values {
			switch cols[i].DatabaseTypeName() {
			case "INT", "BIGINT", "FLOAT", "DOUBLE":
				val, err := strconv.ParseFloat(string(values[i]), 64)
				if err != nil {
					pushTwoErr(err, L)
					return 2
				}
				L.SetField(table, cols[i].Name(), lua.LNumber(val))
			default:
				L.SetField(table, cols[i].Name(), lua.LString(string(values[i])))
			}
		}
		L.RawSetInt(all, index, table)
		index++
	}
	if rows.Err() != nil {
		pushTwoErr(err, L)
		return 2
	}
	L.Push(all)
	return 1
}

func (my *sqlState) queryrow(L *lua.LState) int {
	cmd, args, err := GetArgs(L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	rows, err := my.db.Query(cmd, args...)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	m := make([]interface{}, len(cols))
	values := make([]sql.RawBytes, len(cols))
	for i := range m {
		m[i] = &values[i]
	}
	//这里如果没有查询到数据则返回错误
	table := L.NewTable()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			pushTwoErr(err, L)
			return 2
		}
		//table.RawSetString("_affected", lua.LNumber(0))
		//L.Push(table)
		pushTwoErr(fmt.Errorf("sql: no rows in result set"), L)
		return 2
	}
	if err := rows.Scan(m...); err != nil {
		pushTwoErr(err, L)
		return 2
	}
	for i := range values {
		switch cols[i].DatabaseTypeName() {
		case "INT", "BIGINT", "FLOAT", "DOUBLE":
			val, err := strconv.ParseFloat(string(values[i]), 64)
			if err != nil {
				pushTwoErr(err, L)
				return 2
			}
			L.SetField(table, cols[i].Name(), lua.LNumber(val))
		default:
			L.SetField(table, cols[i].Name(), lua.LString(string(values[i])))
		}
	}
	L.Push(table)
	return 1
}

func (my *sqlState) exec(L *lua.LState) int {
	//检查事务状态
	if atomic.LoadInt32(&my.status) == 0 {
		pushTwoErr(fmt.Errorf("请先开始事务"), L)
		return 2
	}
	cmd, args, err := GetArgs(L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	result, err := my.tx.Exec(cmd, args...)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	t := L.NewTable()
	lastInsertID, err := result.LastInsertId()
	if err == nil {
		L.SetField(t, "insertid", lua.LNumber(float64(lastInsertID)))
	}
	affectRow, err := result.RowsAffected()
	if err == nil {
		L.SetField(t, "affected", lua.LNumber(float64(affectRow)))
	}
	L.Push(t)
	return 1
}

func (my *sqlState) begin(L *lua.LState) int {
	if !atomic.CompareAndSwapInt32(&my.status, 0, 1) {
		L.Push(lua.LString("事务已经开始"))
		return 1
	}
	tx, err := my.db.Begin()
	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	my.tx = tx
	return 0

}

func (my *sqlState) rollback(L *lua.LState) int {
	if !atomic.CompareAndSwapInt32(&my.status, 1, 0) {
		L.Push(lua.LString("请先开始事务"))
		return 1
	}
	err := my.tx.Rollback()
	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	return 0

}

func (my *sqlState) commit(L *lua.LState) int {
	if !atomic.CompareAndSwapInt32(&my.status, 1, 0) {
		L.Push(lua.LString("请先开始事务"))
		return 1
	}
	err := my.tx.Commit()
	if err != nil {
		L.Push(lua.LString(err.Error()))
		return 1
	}
	return 0
}
