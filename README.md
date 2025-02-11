# LuaVM

LuaVM is a lightweight and efficient Lua virtual machine implemented in Go. This project aims to provide a simple and easy-to-use Lua interpreter for embedding into Go applications.

## Features

- Lightweight and efficient
- Easy to embed in Go applications
- Supports Lua 5.3 syntax and features

## Installation

To install LuaVM, use `go get`:

```sh
go get github.com/yourusername/luavm
```

## Usage

Here's a simple example of how to use LuaVM in your Go application:

```go
package main

import (
    "fmt"
    "github.com/yourusername/luavm"
)

func main() {
    L := luavm.NewState()
    defer L.Close()

    err := L.DoString(`print("Hello, LuaVM!")`)
    if err != nil {
        fmt.Println("Error:", err)
    }
}
```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request on GitHub.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Acknowledgements

- [Lua](https://www.lua.org/)
- [Go](https://golang.org/)
