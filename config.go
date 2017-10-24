package luavm

import (
	"github.com/BurntSushi/toml"
)

type luaConfig struct {
	Redis struct {
		Addr     string
		Passwd   string
		DataBase int
	}
	MainMySQL struct {
		Addr     string
		User     string
		Passwd   string
		DataBase string
	}
	SalveMySQL struct {
		Addr     string
		User     string
		Passwd   string
		DataBase string
	}
	Mongodb struct {
		Addr   string
		User   string
		Passwd string
	}
}

func (l *luaConfig) Load(filename string) (err error) {
	if l == nil {
		l = new(luaConfig)
	}
	if _, err = toml.DecodeFile(filename, l); err != nil {
		return
	}
	return nil
}
