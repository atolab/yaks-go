package main

import (
	"fmt"
	"os"

	"github.com/atolab/yaks-go"
)

func main() {
	// If not specified as 1st argument, use a relative path (to the workspace below): "yaks-go-put"
	path := "yaks-go-put"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}

	value := "Put from Yaks Go!"
	if len(os.Args) > 2 {
		value = os.Args[2]
	}

	var locator *string
	if len(os.Args) > 3 {
		locator = &os.Args[3]
	}

	p, err := yaks.NewPath(path)
	if err != nil {
		panic(err.Error())
	}
	v := yaks.NewStringValue(value)

	fmt.Println("Login to Yaks...")
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
