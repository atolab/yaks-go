package main

import (
	"fmt"
	"os"

	"github.com/atolab/yaks-go"
)

func main() {
	locator := "tcp/127.0.0.1:7447"
	if len(os.Args) > 1 {
		locator = os.Args[1]
	}

	// If not specified as 2nd argument, use a relative path (to the workspace below): "yaks-go-put"
	path := "yaks-go-put"
	if len(os.Args) > 2 {
		path = os.Args[2]
	}

	value := "Put from Yaks Go!"
	if len(os.Args) > 3 {
		value = os.Args[3]
	}

	p, err := yaks.NewPath(path)
	if err != nil {
		panic(err.Error())
	}
	v := yaks.NewStringValue(value)

	fmt.Println("Login to " + locator + "...")
	y, err := yaks.Login(locator, nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Use Workspace on '/demo/example'")
	root, _ := yaks.NewPath("/demo/example")
	w := y.Workspace(root)

	fmt.Println("Put on " + p.ToString() + " : " + v.ToString())
	err = w.Put(p, v)
	if err != nil {
		panic(err.Error())
	}

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
