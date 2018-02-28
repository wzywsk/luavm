package luavm

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/yuin/gluamapper"
	"github.com/yuin/gopher-lua"
	"layeh.com/gopher-json"
	"layeh.com/gopher-luar"
)

//LuaVM lua虚拟机,每一个lua脚本维护一个lua状态
type LuaVM struct {
	lock  sync.Mutex
	l     *lua.LState
	conf  *luaConfig
	easy  *lua.LTable   //easy 全局对象
	trans []*mysqlState //mysql事务状态
}

//NewLuaVM ...
func NewLuaVM(conf *luaConfig) *LuaVM {
	l := new(LuaVM)
	l.conf = conf
	l.l = lua.NewState(lua.Options{
		SkipOpenLibs: true,
	})
	return l
}

//Close 关闭虚拟机
func (l *LuaVM) Close() {
	if l.l == nil {
		return
	}
	l.l.Close()
}

//SetContext 设置上下文,一般用来设置超时时间
func (l *LuaVM) SetContext(ctx context.Context) {
	l.l.SetContext(ctx)
}

//LoadLibs 加载常用的库
func (l *LuaVM) LoadLibs(ml, rl, gl lua.LGFunction) {
	//加载基本库
	l.OpenLibs()
	//加载bigint库
	if err := l.l.DoString(bigintLib); err != nil {
		log.Printf("加载bigint库失败 %v", err)
	}
	//加载json插件
	l.PreLoadModule("json", json.Loader)
	//加载mysql插件
	l.PreLoadModule("mysql", ml)
	//加载redis插件
	l.PreLoadModule("redis", rl)
	//加载mongodb插件
	l.PreLoadModule("mongodb", gl)
}

//PreLoadModule 加载自定义库
func (l *LuaVM) PreLoadModule(name string, loader lua.LGFunction) {
	l.l.PreloadModule(name, loader)
}

//DoString 执行一个lua字符串
func (l *LuaVM) DoString(str string) (errNo, errMsg string, err error) {
	//初始化easy全局变量
	l.initEasy()

	defer func() {
		//如果存在事务状态则全部回滚
		for _, tran := range l.trans {
			if tran.tx != nil {
				tran.tx.Rollback()
			}
		}
		l.trans = nil
	}()

	l.lock.Lock()
	defer l.lock.Unlock()

	if l.l == nil {
		err = fmt.Errorf("请先初始化虚拟机")
		return
	}

	if err = l.l.DoString(str); err != nil {
		return
	}

	//获取lua返回值
	num := l.l.GetTop()
	switch num {
	//没有返回值默认为成功
	case 0:
		errNo, errMsg = "", ""
	case 1:
		errNo = l.l.ToString(1)
		errMsg = ""
	case 2:
		errNo = l.l.ToString(1)
		errMsg = l.l.ToString(2)
	default:
	}
	return
}

//GetEasyAttr 往easy全局对象中读取属性
func (l *LuaVM) GetEasyAttr(name string) lua.LValue {
	return l.easy.RawGetString(name)
}

//DoFile 根据busitype和trancode加载一个lua文件并运行,
//这里将设置luarequire目录, 只允许lua中加载同一业务下的代码
func (l *LuaVM) DoFile(busi, trancode string) (err error) {
	l.initEasy()
	l.lock.Lock()
	defer l.lock.Unlock()

	fp := fmt.Sprintf("./%s/%s/main.lua", busi, trancode)
	dir := fmt.Sprintf("./%s/?.lua", busi)
	//设置require目录
	l.l.SetField(l.l.GetField(l.l.Get(lua.EnvironIndex), "package"), "path", lua.LString(dir))

	if err = l.l.DoFile(fp); err != nil {
		return
	}

	//获取lua返回值
	num := l.l.GetTop()
	var arg1, arg2 string
	switch num {
	//没有返回值默认为成功
	case 0:
	case 1:
		arg1 = l.l.CheckString(1)
	case 2:
		arg1 = l.l.CheckString(1)
		arg2 = l.l.CheckString(2)
	default:
	}
	_, _ = arg1, arg2
	return nil
}

//添加mysql事务状态
func (l *LuaVM) addTran(tran *mysqlState) {
	l.trans = append(l.trans, tran)
}

//SetGlobal 为lua设置一个全局类型
//如果传入函数格式必须为 func(L *lua.LState) int
//传入参数和返回都使用栈,返回int代表有几个返回参数
//func Test(L *lua.LState) int {
//	s0 := L.ToInt(0)
//	s1 := L.ToInt(1)
//	s2 := L.ToInt(2)
//	s3 := L.Get(6)
//	if s3 == lua.LNil {
//		fmt.Println("hehe")
//	}
//	L.Push(lua.LNumber(s1))
//	L.Push(lua.LNumber(s2))
//	return 2
//}
func (l *LuaVM) SetGlobal(name string, value lua.LValue) {
	l.lock.Lock()
	defer l.lock.Unlock()

	l.l.SetGlobal(name, value)
	//l.l.SetGlobal(name, luar.New(l.l, value))
}

//AddField 为一个对象添加字段
//dst 必须为userdata或者table
func (l *LuaVM) AddField(dst lua.LValue, key string, field interface{}) {
	if dst.Type() != lua.LTTable && dst.Type() != lua.LTUserData {
		return
	}
	l.l.SetField(dst, key, luar.New(l.l, field))
}

//NewFunction ...
func (l *LuaVM) NewFunction(fn lua.LGFunction) *lua.LFunction {
	return l.l.NewFunction(fn)
}

//GetGlobal 获取全局类型
func (l *LuaVM) GetGlobal(name string) (value lua.LValue) {
	l.lock.Lock()
	defer l.lock.Unlock()

	return l.l.GetGlobal(name)
}

//NewLuaTable 创建一个lua的table
func (l *LuaVM) NewLuaTable() (r *lua.LTable) {
	return l.l.NewTable()
}

//ConvLuaType 将一个Go类型转换为Lua类型
func (l *LuaVM) ConvLuaType(src interface{}) (dst lua.LValue) {
	dst = luar.New(l.l, src)
	return
}

//ConvGoType 将一个lua转换为Go类型,仅支持go结构体
func (l *LuaVM) ConvGoType(src lua.LValue, dst interface{}) (err error) {
	if _, ok := src.(*lua.LTable); !ok {
		err = fmt.Errorf("%s lua类型必须为 table", src.Type().String())
		return
	}
	return gluamapper.Map(src.(*lua.LTable), dst)
}

//CallGlobal 调用一个全局函数
func (l *LuaVM) CallGlobal(name string, args ...interface{}) (r lua.LValue, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	fn := l.l.GetGlobal(name)
	if fn == nil {
		err = fmt.Errorf("未能获取到全局函数 %s", name)
		return
	}
	if fn.Type() != lua.LTFunction {
		err = fmt.Errorf("%s 类型不是函数", fn.String())
		return
	}

	largs := make([]lua.LValue, len(args))
	for i, arg := range args {
		largs[i] = luar.New(l.l, arg)
	}
	if err = l.l.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, largs...); err != nil {
		return
	}
	r = l.l.Get(-1)
	l.l.Pop(1)
	return
}

//CallLuaFunc 调用一个Lua函数
func (l *LuaVM) CallLuaFunc(fn lua.LValue, args ...interface{}) (r lua.LValue, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()

	if fn.Type() != lua.LTFunction {
		err = fmt.Errorf("%s 类型不是函数", fn.String())
		return
	}

	largs := make([]lua.LValue, len(args))
	for i, arg := range args {
		largs[i] = luar.New(l.l, arg)
	}
	if err = l.l.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, largs...); err != nil {
		return
	}
	r = l.l.Get(-1)
	l.l.Pop(1)
	return
}

//GetField 从value中获取字段name
func (l *LuaVM) GetField(value lua.LValue, key string) (r lua.LValue) {
	return l.l.GetField(value, key)
}

//Clean 清理虚拟机状态
func (l *LuaVM) Clean() {
	//清除堆栈和全局变量
	l.l.SetTop(0)
	//这里清除全局变量后lua无法定义全局变量,待查
	//l.l.G.Global = l.l.CreateTable(0, 64)
	l.easy = l.NewLuaTable()
}

//加入easy全局对象
func (l *LuaVM) initEasy() {
	l.SetGlobal("easy", l.easy)
}

//SetEasyAttr 往easy全局对象中添加属性
func (l *LuaVM) SetEasyAttr(name string, value lua.LValue) {
	l.easy.RawSetString(name, value)
}

//LuaPool lua虚拟机池
type LuaPool struct {
	m     sync.Mutex
	saved []*LuaVM
	conf  *luaConfig
	//mysql插件
	mysql *luaMysql
	//redis插件
	redis *luaRedis
	//mongodb插件
	mgo *luaMgo
}

//NewLuaPool 用法
/*
	func MyWorker() {
  		L := luaPool.Get()
   		defer luaPool.Put(L)
	}

	func main() {
		luaPool := NewLuaPool()
		defer luaPool.Shutdown()

    	go MyWorker()
    	go MyWorker()
	}

*/
func NewLuaPool() *LuaPool {
	p := new(LuaPool)
	p.saved = make([]*LuaVM, 0, 10000)
	p.conf = new(luaConfig)
	return p
}

//InitFromFile 初始化lua容器,必须调用.
func (pl *LuaPool) InitFromFile(file string) (err error) {
	//读取配置文件
	if err = pl.conf.LoadFromFile(file); err != nil {
		return
	}
	if err = pl.initDB(); err != nil {
		return
	}
	return nil
}

//InitFromConf 初始化lua容器,必须调用.
func (pl *LuaPool) InitFromConf(conf string) (err error) {
	//读取配置文件
	if err = pl.conf.LoadFromConf(conf); err != nil {
		return
	}
	if err = pl.initDB(); err != nil {
		return
	}
	return nil
}

//InitDB 初始化数据库
func (pl *LuaPool) initDB() (err error) {
	//初始化mysql插件
	my1 := pl.conf.MainMySQL
	my2 := pl.conf.SalveMySQL
	conf := []string{
		"main",
		fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", my1.User, my1.Passwd, my1.Addr, my1.DataBase),
		"salve",
		fmt.Sprintf("%s:%s@tcp(%s)/%s?charset=utf8", my2.User, my2.Passwd, my2.Addr, my2.DataBase),
	}
	pl.mysql = newLuaMysql()
	if err = pl.mysql.Init(conf); err != nil {
		return
	}
	//初始化redis插件
	re := pl.conf.Redis
	pl.redis = newLuaRedis()
	if err = pl.redis.Init(re.Addr, re.Passwd, re.DataBase); err != nil {
		return
	}
	//初始化mongodb插件
	mg := pl.conf.Mongodb
	pl.mgo = newLuaMgo()
	if err = pl.mgo.Init(mg.Addr, mg.User, mg.Passwd); err != nil {
		return
	}
	return nil
}

//Get 如果没有则会新建
func (pl *LuaPool) Get() *LuaVM {
	pl.m.Lock()
	defer pl.m.Unlock()

	n := len(pl.saved)
	if n == 0 {
		return pl.new()
	}
	x := pl.saved[n-1]
	pl.saved = pl.saved[0 : n-1]
	return x
}

type tranfunc string

//这里将加载lua库和初始化easy全局变量
func (pl *LuaPool) new() *LuaVM {
	L := NewLuaVM(pl.conf)
	L.LoadLibs(pl.mysql.Loader, pl.redis.Loader, pl.mgo.Loader)
	L.easy = L.NewLuaTable()
	//初始化context
	ctx := context.WithValue(context.Background(), tranfunc("addTran"), L.addTran)
	L.l.SetContext(ctx)
	return L
}

//Put ...
func (pl *LuaPool) Put(L *LuaVM) {
	L.Clean()
	//放入对象池
	pl.m.Lock()
	defer pl.m.Unlock()

	pl.saved = append(pl.saved, L)
}

//Shutdown 关闭池内的所以虚拟机
func (pl *LuaPool) Shutdown() {
	for _, L := range pl.saved {
		L.Close()
	}
}
