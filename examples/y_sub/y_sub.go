package main

import (
	"fmt"
	"os"
	"time"

	"github.com/atolab/yaks-go"
)

func listener(changes []yaks.Change) {
	for _, c := range changes {
		fmt.Printf(" -> %s : %s\n", c.Path().ToString(), c.Value().ToString())
	}
}

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
	y, err := yaks.Login(locator, nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Use Workspace on '/'")
	root, _ := yaks.NewPath("/")
	w := y.Workspace(root)

	fmt.Println("Subscribe on " + selector)
	subid, err := w.Subscribe(s, listener)
	if err != nil {
		panic(err.Error())
	}

	time.Sleep(60 * time.Second)

	w.Unsubscribe(subid)

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
