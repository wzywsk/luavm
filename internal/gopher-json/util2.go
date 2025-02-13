package json // import "game/luavm/internal/gopher-json"

import (
	"fmt"

	lua "github.com/yuin/gopher-lua"
)

const (
	ErrJsonDecodeNo = "0206" //json解包失败
	ErrJsonEncodeNo = "0207" //安全码无效
)

func toLuaError(errNo, errMsg string) lua.LString {
	sErr := fmt.Sprintf("<err> #%s %s ", ErrJsonDecodeNo, errMsg)
	return lua.LString(sErr)
}
