package luavm

import (
	"container/list"
	"errors"
	"runtime"
	"sync"
	"time"

	"github.com/yuin/gopher-lua"
)

var (
	errNotFound = errors.New("key not found")
	errExpired  = errors.New("key already expired")
)

const (
	defsegmentSize   = 1000
	defHighSize      = 10000
	defCleanInterval = 60 * 60
)

type mysqlItem struct {
	key    string
	value  *lua.LTable
	expire int64
}

//segment mysql缓存
type segment struct {
	high      int
	cacheSize int
	quit      chan struct{}
	l         sync.Mutex
	keyMap    map[string]*list.Element
	lruList   *list.List
}

//newsegment ...
func newsegment() *segment {
	m := new(segment)
	m.keyMap = make(map[string]*list.Element, defsegmentSize)
	m.lruList = list.New()
	m.high = defHighSize
	m.quit = make(chan struct{})
	go m.loopClean()
	return m
}

func (my *segment) get(key string) (t *lua.LTable, err error) {
	//先检查元素是否过期
	//如果存在此元素,则将元素移到链表头
	my.l.Lock()
	defer my.l.Unlock()
	if e, ok := my.keyMap[key]; ok {
		item := e.Value.(*mysqlItem)
		if item.expire != 0 && time.Now().Unix() > item.expire {
			delete(my.keyMap, key)
			my.lruList.Remove(e)
			my.cacheSize--
			return nil, errExpired
		}
		return item.value, nil
	}
	//从mysql中取出此元素并返回
	return nil, errNotFound
}

func (my *segment) set(key string, t *lua.LTable, expire int) (err error) {
	my.l.Lock()
	defer my.l.Unlock()

	//如果容量已经达到最大限制,则删除最少访问元素
	if my.cacheSize >= my.high {
		element := my.lruList.Back()
		item := element.Value.(*mysqlItem)
		my.lruList.Remove(element)
		delete(my.keyMap, item.key)
		my.cacheSize--
	}
	if e, ok := my.keyMap[key]; ok {
		item := e.Value.(*mysqlItem)
		item.value = t
		item.expire = time.Now().Unix() + int64(expire)
		my.lruList.MoveToFront(e)
		return nil
	}
	var expireTime int64
	if expire == 0 {
		expireTime = 0
	} else {
		expireTime = time.Now().Unix() + int64(expire)
	}
	item := &mysqlItem{key: key, value: t, expire: expireTime}
	elment := &list.Element{Value: item}
	my.keyMap[key] = elment
	my.lruList.PushFront(item)
	my.cacheSize++
	return nil
}

func (my *segment) loopClean() {
	ticker := time.NewTicker(time.Second * defCleanInterval)
	for {
		select {
		case <-ticker.C:
			my.clean()
		case <-my.quit:
			return
		}
	}
}

func (my *segment) clean() {
	my.l.Lock()
	temp := make([]string, my.cacheSize)
	for key := range my.keyMap {
		temp = append(temp, key)
	}
	my.l.Unlock()

	cur := time.Now().Unix()
	var item *mysqlItem
	for _, key := range temp {
		my.l.Lock()
		if e, ok := my.keyMap[key]; ok {
			item = e.Value.(*mysqlItem)
			if item.expire != 0 && cur > item.expire {
				delete(my.keyMap, key)
				my.lruList.Remove(e)
				my.cacheSize--
			}
		}
		my.l.Unlock()
	}
	runtime.GC()
}

func (my *segment) destroy() {
	my.l.Lock()
	defer my.l.Unlock()

	my.keyMap = make(map[string]*list.Element)
	my.lruList = list.New()
	my.cacheSize = 0
	close(my.quit)
}
