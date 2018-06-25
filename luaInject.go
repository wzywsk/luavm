// luaPlugInject
package luavm

import (
	"github.com/yuin/gopher-lua"
)

const (
	loggerInterface = "logger-interface"
	injectInterface = "inject-interface"
)

//Logger 日志接口,存放于easy.logger-interface
type Logger interface {

	//记录错误日志
	//format	是输入日志格式，与Golang保持一致
	//a...  	是输入参数，允许参数数量不定
	Error(format string, a ...interface{})

	//记录警告日志
	//format	是输入日志格式，与Golang保持一致
	//a...  	是输入参数，允许参数数量不定
	Warn(format string, a ...interface{})

	//记录通知日志
	//format	是输入日志格式，与Golang保持一致
	//a...  	是输入参数，允许参数数量不定
	Trace(format string, a ...interface{})
}

type Injecter interface {
	InjectMysql(*lua.LTable) *lua.LTable
	InjectMssql(*lua.LTable) *lua.LTable
}
