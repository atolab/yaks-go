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

	selector := "/demo/**"
	if len(os.Args) > 2 {
		selector = os.Args[2]
	}

	s, err := yaks.NewSelector(selector)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Login to " + locator + "...")
	y, err := yaks.Login("tcp/127.0.0.1:7447", nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Use Workspace on '/'")
	root, _ := yaks.NewPath("/")
	w := y.Workspace(root)

	fmt.Println("Eval on " + s.ToString())
	for _, pv := range w.Eval(s) {
		fmt.Println("  " + pv.Path().ToString() + " : " + pv.Value().ToString())
	}

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
