package luavm

import (
	"database/sql"
	"strconv"
	"sync"

	"github.com/yuin/gopher-lua"

	"github.com/cespare/xxhash"
)

const (
	segmentCount = 256
	segmentAnd   = 255
)

//Cache ...
type Cache struct {
	db    *sql.DB
	locks [segmentCount]sync.Mutex
	segs  [segmentCount]*segment
}

//NewCache ...
func NewCache(db *sql.DB) *Cache {
	c := new(Cache)
	c.db = db
	for i := range c.segs {
		c.segs[i] = newsegment()
	}
	return c
}

func (cache *Cache) set(key string, value *lua.LTable, expire int) (err error) {
	segID := int(xxhash.Sum64String(key) & segmentAnd)
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()

	return cache.segs[segID].set(key, value, expire)
}

func (cache *Cache) get(key string) (value *lua.LTable, err error) {
	segID := int(xxhash.Sum64String(key) & segmentAnd)
	cache.locks[segID].Lock()
	defer cache.locks[segID].Unlock()

	return cache.segs[segID].get(key)
}

func (cache *Cache) getMysqlData(sqlCommand string) (value *lua.LTable, err error) {
	rows, err := cache.db.Query(sqlCommand)
	if err != nil {
		return
	}
	defer rows.Close()

	//获取每一行的数据类型和个数
	cols, err := rows.ColumnTypes()
	if err != nil {
		return
	}
	m := make([]interface{}, len(cols))
	values := make([]sql.RawBytes, len(cols))
	for i := range m {
		m[i] = &values[i]
	}
	//这里存放读取的所有数据
	var L *lua.LState
	value = L.NewTable()
	//读出所有数据并转换为lua数据类型
	//lua 数组下标从1开始
	index := 1
	for rows.Next() {
		if err = rows.Scan(m...); err != nil {
			return
		}
		table := L.NewTable()
		for i := range values {
			switch cols[i].DatabaseTypeName() {
			case "INT", "BIGINT", "FLOAT", "DOUBLE":
				val, err := strconv.ParseFloat(string(values[i]), 64)
				if err != nil {
					return nil, err
				}
				L.SetField(table, cols[i].Name(), lua.LNumber(val))
			default:
				L.SetField(table, cols[i].Name(), lua.LString(string(values[i])))
			}
		}
		L.RawSetInt(value, index, table)
		index++
	}
	if rows.Err() != nil {
		return
	}
	return
}

func (cache *Cache) queryCache(key, cmd string, expire int) (value *lua.LTable, err error) {
	value, err = cache.get(key)
	if err == nil || err != errNotFound && err != errExpired {
		return
	}
	//如果返回过期或者不存在则读取数据库数据
	value, err = cache.getMysqlData(cmd)
	if err != nil {
		return
	}
	//存储后返回
	if err = cache.set(key, value, expire); err != nil {
		return
	}
	return value, nil
}

//QueryCache expire过期时间,单位为秒
func (cache *Cache) QueryCache(path, cmd string, expire int) (value *lua.LTable, err error) {
	return cache.queryCache(path, cmd, expire)
}

//Destory 清空所以缓存
func (cache *Cache) Destory() {
	for id := range cache.locks {
		cache.locks[id].Lock()
		cache.segs[id].destroy()
		cache.locks[id].Unlock()
	}
}
