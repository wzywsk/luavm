package luavm

import lua "github.com/yuin/gopher-lua"

const (
	//BaseLibName lua基本函数
	baseLibName = ""
	//LoadLibName 包管理函数
	loadLibName = "package"
	//TabLibName table相关函数
	tabLibName = "table"
	// IoLibName io相关函数,包含文件操作函数
	ioLibName = "io"
	// OsLibName 系统相关
	osLibName = "os"
	// StringLibName 字符串格式化相关
	stringLibName = "string"
	// MathLibName 数学相关函数
	mathLibName = "math"
	// DebugLibName 调试相关函数
	debugLibName = "debug"
	// ChannelLibName 管道相关函数
	channelLibName = "channel"
	// CoroutineLibName 协程相关函数
	coroutineLibName = "coroutine"
)

type luaLib struct {
	libName string
	libFunc lua.LGFunction
}

var luaLibs = []luaLib{
	luaLib{loadLibName, lua.OpenPackage},
	luaLib{baseLibName, lua.OpenBase},
	luaLib{tabLibName, lua.OpenTable},
	luaLib{ioLibName, lua.OpenIo},
	luaLib{osLibName, lua.OpenOs},
	luaLib{stringLibName, lua.OpenString},
	luaLib{mathLibName, lua.OpenMath},
	luaLib{debugLibName, lua.OpenDebug},
	//luaLib{channelLibName, lua.OpenChannel},
	luaLib{coroutineLibName, lua.OpenCoroutine},
}

//OpenLibs 加载lua基本库
func (l *LuaVM) OpenLibs() {
	for _, lib := range luaLibs {
		l.l.Push(l.l.NewFunction(lib.libFunc))
		l.l.Push(lua.LString(lib.libName))
		l.l.Call(1, 0)
	}
}
