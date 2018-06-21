package luavm

import (
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/yuin/gopher-lua"
)

//luaMysql lua容器mysql注入插件,将根据配置初始化多个数据库
type luaMysql struct {
	lock *sync.Mutex
	db   map[string]*sql.DB
}

//newLuaMysql ...
func newLuaMysql() *luaMysql {
	l := new(luaMysql)
	l.lock = new(sync.Mutex)
	l.db = make(map[string]*sql.DB)
	return l
}

//Init 初始化mysql插件
//格式为第一个名称,第二个source
func (l *luaMysql) Init(conf []string) (err error) {
	for i := 0; i < len(conf); i = i + 2 {
		//db, err := sql.Open("mysql", "root:easy@tcp(192.168.1.30:3306)/graph?charset=utf8")
		db, err := sql.Open("mysql", conf[i+1])
		if err != nil {
			return err
		}
		l.db[conf[i]] = db
	}
	return nil
}

//Loader ...
func (l *luaMysql) Loader(L *lua.LState) int {
	var exports = map[string]lua.LGFunction{
		"connect": l.connect,
	}
	mod := L.SetFuncs(L.NewTable(), exports)
	L.Push(mod)
	return 1
}

func (l *luaMysql) connect(L *lua.LState) int {
	name := L.CheckString(1)
	db := l.db[name]
	if db == nil {
		pushTwoErr(fmt.Errorf("数据库[%s]不存在", name), L)
		return 2
	}
	m := newMysqlState(db)
	my := L.NewTable()
	my.RawSetString("query", L.NewFunction(m.query))
	my.RawSetString("queryRow", L.NewFunction(m.queryrow))
	my.RawSetString("exec", L.NewFunction(m.exec))
	my.RawSetString("begin", L.NewFunction(m.begin))
	my.RawSetString("commit", L.NewFunction(m.commit))
	my.RawSetString("rollback", L.NewFunction(m.rollback))
	my.RawSetString("logger", L.NewFunction(m.logger))
	//添加mysql事务状态
	if ctx := L.Context(); ctx != nil {
		f := ctx.Value(tranfunc("addTran")).(func(*mysqlState))
		f(m)
	} else {
		pushTwoErr(fmt.Errorf("ctx为空"), L)
		return 2
	}
	//获取Logger接口
	logger := GetLogger(L)
	m.SetLogger(logger)

	L.Push(my)
	return 1
}

//同一时间只能维护一个事务
type mysqlState struct {
	status int32 //记录事务状态
	db     *sql.DB
	tx     *sql.Tx
	l      Logger
}

func newMysqlState(db *sql.DB) *mysqlState {
	m := new(mysqlState)
	m.db = db
	return m
}

//SetLogger ...
func (my *mysqlState) SetLogger(l Logger) {
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

//插入mysql日志表专用,不走事务,直接返回错误
func (my *mysqlState) logger(L *lua.LState) int {
	str := L.CheckString(1)
	_, err := my.db.Exec(str)
	if err != nil {
		my.l.Error(err.Error())
		return 0
	}
	return 0
}

func (my *mysqlState) query(L *lua.LState) int {
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

func (my *mysqlState) queryrow(L *lua.LState) int {
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

func (my *mysqlState) exec(L *lua.LState) int {
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

func (my *mysqlState) begin(L *lua.LState) int {
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

func (my *mysqlState) rollback(L *lua.LState) int {
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

func (my *mysqlState) commit(L *lua.LState) int {
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
