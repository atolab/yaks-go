package main

import (
	"fmt"
	"os"
	"time"

	"github.com/atolab/yaks-go"
)

func main() {
	locator := "tcp/127.0.0.1:7447"
	if len(os.Args) > 1 {
		locator = os.Args[1]
	}

	p, err := yaks.NewPath("/demo/eval")
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

	eval := func(path *yaks.Path, props yaks.Properties) yaks.Value {
		fmt.Println(props)
		name := props["name"]
		if name == "" {
			name = "World"
		}

		// if name starts with '/' consider it as a path to get the real name to use
		if name[0] == '/' {
			s, err := yaks.NewSelector(name)
			if err == nil {
				kvs := w.Get(s)
				if len(kvs) > 0 {
					name = kvs[0].Value().ToString()
				}
			}
		}
		return yaks.NewStringValue("Hello " + name + "!")
	}

	fmt.Println("Register eval " + p.ToString())
	err = w.RegisterEval(p, eval)
	if err != nil {
		panic(err.Error())
	}

	time.Sleep(60 * time.Second)

	w.UnregisterEval(p)

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
