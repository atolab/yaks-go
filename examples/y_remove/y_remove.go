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

	var locator *string
	if len(os.Args) > 2 {
		locator = &os.Args[2]
	}

	p, err := yaks.NewPath(path)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Login to Yaks...")
	y, err := yaks.Login(locator, nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Use Workspace on '/demo/example'")
	root, _ := yaks.NewPath("/demo/example")
	w := y.Workspace(root)

	fmt.Println("Remove " + p.ToString())
	err = w.Remove(p)
	if err != nil {
		panic(err.Error())
	}

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
