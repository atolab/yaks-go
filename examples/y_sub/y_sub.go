package main

import (
	"fmt"
	"os"

	"github.com/atolab/yaks-go"
)

func main() {
	selector := "/demo/example/**"
	if len(os.Args) > 1 {
		selector = os.Args[1]
	}

	var locator *string
	if len(os.Args) > 2 {
		locator = &os.Args[2]
	}

	s, err := yaks.NewSelector(selector)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Login to Yaks...")
	y, err := yaks.Login(locator, nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Use Workspace on '/'")
	root, _ := yaks.NewPath("/")
	w := y.Workspace(root)

	fmt.Println("Subscribe on " + selector)
	subid, err := w.Subscribe(s,
		func(changes []yaks.Change) {
			for _, c := range changes {
				switch c.Kind() {
				case yaks.PUT:
					fmt.Printf(">> [Subscription listener] Received PUT on '%s': '%s')\n", c.Path().ToString(), c.Value().ToString())
				case yaks.UPDATE:
					fmt.Printf(">> [Subscription listener] Received UPDATE on '%s': '%s')\n", c.Path().ToString(), c.Value().ToString())
				case yaks.REMOVE:
					fmt.Printf(">> [Subscription listener] Received REMOVE on '%s')\n", c.Path().ToString())
				default:
					fmt.Printf(">> [Subscription listener] Received unkown operation with kind '%d' on '%s')\n", c.Kind(), c.Path().ToString())
				}
			}
		})
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Enter 'q' to quit...")
	fmt.Println()
	var b = make([]byte, 1)
	for b[0] != 'q' {
		os.Stdin.Read(b)
	}

	err = w.Unsubscribe(subid)
	if err != nil {
		panic(err.Error())
	}

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
