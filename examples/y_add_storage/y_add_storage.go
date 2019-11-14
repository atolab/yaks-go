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

	storageID := "Demo"
	if len(os.Args) > 2 {
		storageID = os.Args[2]
	}

	var locator *string
	if len(os.Args) > 3 {
		locator = &os.Args[3]
	}

	fmt.Println("Login to Yaks...")
	y, err := yaks.Login(locator, nil)
	if err != nil {
		panic(err.Error())
	}

	admin := y.Admin()

	fmt.Println("Add storage " + storageID + " with selector " + selector)
	p := make(map[string]string)
	p["selector"] = selector
	admin.AddStorage(storageID, p)

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
