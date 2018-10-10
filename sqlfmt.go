package luavm

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"unsafe"

	"github.com/yuin/gopher-lua"
)

const (
	//MYSQL ...
	MYSQL = "mysql"
	//MSSQL ...
	MSSQL = "mssql"
)

func toString(b []byte) (s string) {
	return *(*string)(unsafe.Pointer(&b))
}

func quote(sqlType, name string) string {
	switch sqlType {
	case MYSQL:
		return "`" + name + "`"
	case MSSQL:
		return "[" + name + "]"
	}
	return ""
}

func quoteByte(sqlType string) byte {
	switch sqlType {
	case MYSQL:
		return '\\'
	case MSSQL:
		return '\''
	}
	return ' '
}

func isText(sqlType, s string) (bool, string) {
	l := len(s)
	if l == 0 {
		return true, "'"
	}

	if s[0] == '\'' || s[l-1] == '\'' {
		return true, ""
	}

	var start, end byte
	for i := range s {
		start = s[i]
		end = s[len(s)-1-i]
		if (start == ' ' || start == '\t' || start == '\n' || start == '\r') &&
			(end == ' ' || end == '\t' || end == '\n' || end == '\r') {
			continue
		}
		if start == '\'' || end == '\'' {
			return true, ""
		}
		if sqlType == MSSQL {
			if start == '[' || end == ']' {
				return false, ""
			}
		}
		if sqlType == MYSQL {
			if start == '`' || end == '`' {
				return false, ""
			}
		}
		break
	}

	if start == 'n' || start == 'N' || end == 'l' || end == 'L' {
		if n := strings.TrimSpace(s); len(n) == 4 &&
			(n[0] == 'n' || n[0] == 'N') &&
			(n[1] == 'u' || n[1] == 'U') &&
			(n[2] == 'l' || n[2] == 'L') &&
			(n[3] == 'l' || n[3] == 'L') {
			return false, ""
		}
	}

	if strings.IndexByte(s, '(') < 0 {
		return true, "'"
	}
	return false, ""

}

func fmtTxt(sqlType, txt string) string {
	var last byte
	var pos int
	var has = false
	var qu = quoteByte(sqlType)
	for pos = range txt {
		cur := txt[pos]
		if cur == '\'' {
			if last != qu {
				has = true
				break //发现非格式化单引号，需要进行格式化
			}
		}
		last = cur
	}

	if !has { //无需转义返还
		return txt
	}

	//发现需要转义
	tmp := make([]byte, 0, len(txt)+8)
	tmp = append(tmp, txt[:pos]...)
	tmp = append(tmp, qu)
	tmp = append(tmp, txt[pos])
	for i := pos + 1; i < len(txt); i++ {
		cur := txt[i]
		if cur == '\'' {
			if last != qu {
				tmp = append(tmp, qu, cur) //发现非格式化单引号，需要进行格式化
				last = cur
				continue
			}
		}
		tmp = append(tmp, cur)
		last = cur
	}
	return toString(tmp)
}

func getArgs(sqlType string, L *lua.LState, top int) (args []interface{}, err error) {

	num := L.GetTop()
	if num <= top {
		return nil, nil
	}
	args = make([]interface{}, num-top)
	for i := top; i < num; i++ {
		arg := L.Get(i + 1)
		switch arg.Type() {
		case lua.LTNil:
			args[i-top] = "null"
		case lua.LTBool:
			args[i-top] = L.ToBool(i + 1)
		case lua.LTNumber:
			args[i-top] = L.ToNumber(i + 1)
		case lua.LTString:
			args[i-top] = fmtTxt(sqlType, L.ToString(i+1))
		default:
			err = fmt.Errorf("参数类型错误[%d]", i+1)
			return
		}
	}
	return
}

func getCmdArgs(sqlType string, L *lua.LState) (cmd string, args []interface{}, err error) {
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
			case lua.LTNil:
				args[i-2] = "null"
			case lua.LTBool:
				args[i-2] = L.ToBool(i)
			case lua.LTNumber:
				args[i-2] = L.ToNumber(i)
			case lua.LTString:
				args[i-2] = fmtTxt(sqlType, L.ToString(i))
			default:
				err = fmt.Errorf("参数类型错误[%d]", i)
				return
			}
		}
	}
	return
}

func toBool(b lua.LBool) int {
	if b {
		return 1
	}
	return 0
}

//不断生成Field集合
func genInsertField(sqlType string, buff *strings.Builder, index int, key lua.LValue) {
	keyStr := quote(sqlType, key.String())
	if index == 0 {
		buff.WriteString(fmt.Sprintf("%s", keyStr))
		return
	}
	buff.WriteString(fmt.Sprintf(", %s", keyStr))
	return
}

//不断生成Value集合
func genInsertValue(sqlType string, buff *strings.Builder, value lua.LValue) {
	switch value.Type() {
	case lua.LTBool:
		if sqlType == MSSQL {
			buff.WriteString(fmt.Sprintf(" %d ", toBool(value.(lua.LBool))))
			return
		} else if sqlType == MYSQL {
			buff.WriteString(fmt.Sprintf(" %t ", value.(lua.LBool)))
			return
		}
	case lua.LTNumber:
		buff.WriteString(fmt.Sprintf(" %v ", value.(lua.LNumber)))
		return
	case lua.LTString:
		s := string(value.(lua.LString))
		if ok, quotes := isText(sqlType, s); ok {
			if quotes != "" {
				buff.WriteString(fmt.Sprintf("%s%s%s", quotes, fmtTxt(sqlType, s), quotes))
				return
			}
			buff.WriteString(fmt.Sprintf(" %s ", s))
			return
		}
		buff.WriteString(fmt.Sprintf(" %s ", s))
		return
	}
	return
}

func (my *sqlState) fmtInsert(L *lua.LState) int {
	str, err := my.fmtInsertSQL(L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	L.Push(lua.LString(str))
	return 1
}

func (my *sqlState) fmtInsertSQL(L *lua.LState) (str string, err error) {
	if L.GetTop() != 2 {
		err = fmt.Errorf("参数不正确, 只能为2而不是%d", L.GetTop())
		return
	}

	var table = L.CheckString(1)
	var fields = L.CheckTable(2)

	//从Table中顺序遍历所有的属性
	var f, v strings.Builder
	f.WriteString(fmt.Sprintf("insert into%s(", quote(my.sqlType, table)))
	v.WriteString(" values(")
	index := 0
	key, value := fields.Next(lua.LNil)
	for key.Type() != lua.LTNil {
		if key.Type() != lua.LTString {
			err = fmt.Errorf("key类型[%s]不为String", key.Type().String())
			return
		}
		genInsertField(my.sqlType, &f, index, key)

		if t := value.Type(); t != lua.LTBool && t != lua.LTNumber && t != lua.LTString {
			err = fmt.Errorf("val类型[%s]不为String或Bool或Number", key.Type().String())
			return
		}
		if index > 0 {
			v.WriteString(" ,")
		}
		genInsertValue(my.sqlType, &v, value)
		key, value = fields.Next(key)
		index++
	}
	f.WriteString(")")
	v.WriteString(")")
	f.WriteString(v.String())
	return f.String(), nil
}

func (my *sqlState) sqlInsert(L *lua.LState) int {
	//检查事务状态
	if atomic.LoadInt32(&my.status) == 0 {
		pushTwoErr(fmt.Errorf("请先开始事务"), L)
		return 2
	}
	str, err := my.fmtInsertSQL(L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	result, err := my.tx.Exec(str)
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

func genSelectField(sqlType string, buff *strings.Builder, key, value lua.LValue) {
	keyStr := quote(sqlType, key.String())
	switch value.Type() {
	case lua.LTBool:
		if sqlType == MSSQL {
			buff.WriteString(fmt.Sprintf(" %d %s", toBool(value.(lua.LBool)), keyStr))
			return
		} else if sqlType == MYSQL {
			buff.WriteString(fmt.Sprintf(" %t %s", value.(lua.LBool), keyStr))
			return
		}
	case lua.LTNumber:
		buff.WriteString(fmt.Sprintf(" %v %s", value.(lua.LNumber), keyStr))
		return
	case lua.LTString:
		s := string(value.(lua.LString))
		if s == "" {
			buff.WriteString(fmt.Sprintf("%s", keyStr))
			return
		}
		if ok, quotes := isText(sqlType, s); ok {
			if quotes != "" {
				buff.WriteString(fmt.Sprintf("%s%s%s %s", quotes, fmtTxt(sqlType, s), quotes, keyStr))
				return
			}
			buff.WriteString(fmt.Sprintf(" %s %s", s, keyStr))
			return
		}
		buff.WriteString(fmt.Sprintf(" %s %s", s, keyStr))
		return
	}
	return
}

//格式化Select SQL CMD
func (my *sqlState) fmtSelect(L *lua.LState) int {
	str, err := my.fmtSelectSQL(L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}
	L.Push(lua.LString(str))
	return 1
}

func (my *sqlState) sqlSelect(L *lua.LState) int {
	str, err := my.fmtSelectSQL(L)
	if err != nil {
		pushTwoErr(err, L)
		return 2
	}

	rows, err := my.db.Query(str)
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

func (my *sqlState) fmtSelectSQL(L *lua.LState) (str string, err error) {
	if L.GetTop() < 2 {
		err = fmt.Errorf("参数不正确, 至少为2而不是%d", L.GetTop())
		return
	}
	var table = L.CheckString(1)
	var fields = L.CheckTable(2)
	var where string
	var args []interface{}
	if L.GetTop() > 2 {
		where = L.CheckString(3)
	}
	if L.GetTop() > 3 {
		args, err = getArgs(my.sqlType, L, 3)
	}
	if err != nil {
		err = fmt.Errorf("参数不正确%v", err)
		return
	}

	//从Table中顺序遍历所有的属性
	var f strings.Builder
	f.WriteString("select ")
	index := 0
	key, value := fields.Next(lua.LNil)
	for key.Type() != lua.LTNil {
		if key.Type() != lua.LTString {
			err = fmt.Errorf("key类型[%s]不为String", key.Type().String())
			return
		}

		if t := value.Type(); t != lua.LTBool && t != lua.LTNumber && t != lua.LTString {
			pushTwoErr(fmt.Errorf("val类型[%s]不为String或Bool或Number", key.Type().String()), L)
			return
		}
		if index > 0 {
			f.WriteString(" ,")
		}
		genSelectField(my.sqlType, &f, key, value)
		key, value = fields.Next(key)
		index++
	}

	f.WriteString(fmt.Sprintf(" from %s", quote(my.sqlType, table)))
	if len(where) > 0 {
		f.WriteString(" where ")
		f.WriteString(fmt.Sprintf(where, args...))
	}
	return f.String(), nil
}

func genUpdate(sqlType string, buff *strings.Builder, key, value lua.LValue) {
	keyStr := quote(sqlType, key.String())
	switch value.Type() {
	case lua.LTBool:
		if sqlType == MSSQL {
			buff.WriteString(fmt.Sprintf(" %s = %d ", keyStr, toBool(value.(lua.LBool))))
			return
		} else if sqlType == MYSQL {
			buff.WriteString(fmt.Sprintf(" %s = %t ", keyStr, value.(lua.LBool)))
			return
		}
	case lua.LTNumber:
		buff.WriteString(fmt.Sprintf(" %s = %v ", keyStr, value.(lua.LNumber)))
		return
	case lua.LTString:
		s := string(value.(lua.LString))
		if ok, quotes := isText(sqlType, s); ok {
			if quotes != "" {
				buff.WriteString(fmt.Sprintf(" %s = %s%s%s", keyStr, quotes, fmtTxt(sqlType, s), quotes))
				return
			}
			buff.WriteString(fmt.Sprintf(" %s = %s ", keyStr, s))
			return
		}
		buff.WriteString(fmt.Sprintf(" %s = %s ", keyStr, s))
		return
	}
	return
}

//格式化Update SQL CMD
func (my *sqlState) fmtUpate(L *lua.LState) int {
	if L.GetTop() < 3 {
		pushTwoErr(fmt.Errorf("参数不正确, 至少为3而不是%d", L.GetTop()), L)
		return 2
	}
	var table = L.CheckString(1)
	var sets = L.CheckTable(2)
	L.CheckTypes(3, lua.LTString, lua.LTTable)
	var any = L.Get(3)
	var where string
	var wheArgs []interface{}
	var wheTable *lua.LTable

	switch any.Type() {
	case lua.LTString:
		where = string(any.(lua.LString))
		var err error
		if L.GetTop() > 3 {
			wheArgs, err = getArgs(my.sqlType, L, 3)
		}
		if err != nil {
			pushTwoErr(fmt.Errorf("参数不正确 %v", err), L)
			return 2
		}
	case lua.LTTable:
		wheTable = any.(*lua.LTable)
	default:
		pushTwoErr(fmt.Errorf("类型不正确 %v", any.Type().String()), L)
		return 2
	}

	//从Table中顺序遍历所有的属性, 生成set
	var f strings.Builder
	var index = 0
	f.WriteString(fmt.Sprintf("update %s set ", quote(my.sqlType, table)))
	key, value := sets.Next(lua.LNil)
	for key.Type() != lua.LTNil {
		if key.Type() != lua.LTString {
			pushTwoErr(fmt.Errorf("key类型[%s]不为String", key.Type().String()), L)
			return 2
		}
		if t := value.Type(); t != lua.LTBool && t != lua.LTNumber && t != lua.LTString {
			pushTwoErr(fmt.Errorf("val类型[%s]不为String或Bool或Number", key.Type().String()), L)
			return 2
		}
		if index > 0 {
			f.WriteString(",")
		}
		genUpdate(my.sqlType, &f, key, value)
		key, value = sets.Next(key)
		index++
	}

	switch any.Type() {
	case lua.LTString:
		f.WriteString("  where ")
		f.WriteString(fmt.Sprintf(where, wheArgs...))
	case lua.LTTable:
		if wheTable != nil {
			f.WriteString("  where ")
			index = 0
			key, value = wheTable.Next(lua.LNil)
			for key.Type() != lua.LTNil {
				if key.Type() != lua.LTString {
					pushTwoErr(fmt.Errorf("key类型[%s]不为String", key.Type().String()), L)
					return 2
				}
				if index > 0 {
					f.WriteString(" AND ")
				}
				genUpdate(my.sqlType, &f, key, value)

				if t := value.Type(); t != lua.LTBool && t != lua.LTNumber && t != lua.LTString {
					pushTwoErr(fmt.Errorf("val类型[%s]不为String或Bool或Number", key.Type().String()), L)
					return 2
				}
				key, value = wheTable.Next(key)
				index++
			}
		}
	}
	L.Push(lua.LString(f.String()))
	return 1
}

func (my *sqlState) fmtSQL(L *lua.LState) int {
	cmd, args, err := getCmdArgs(my.sqlType, L)
	if err != nil {
		pushTwoErr(fmt.Errorf("参数类型不正确,至少1而不是%d", L.GetTop()), L)
		return 2
	}
	var buff strings.Builder
	if l := len(cmd); l > 0 && cmd[0] != '\t' {
		buff.WriteString("\t")
	}
	buff.WriteString(fmt.Sprintf(cmd, args...))
	if buff.String()[buff.Len()-1] != '\n' {
		buff.WriteString("\n")
	}
	L.Push(lua.LString(buff.String()))
	return 1
}
