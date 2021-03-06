package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/atolab/yaks-go"
)

func main() {
	var locator *string
	if len(os.Args) < 2 {
		fmt.Printf("USAGE:\n\ty_put_thr <payload-size> [<zenoh-locator>]\n\n")
		os.Exit(-1)
	}

	length, err := strconv.Atoi(os.Args[1])
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("Running throughput test for payload of %d bytes\n", length)
	if len(os.Args) > 2 {
		locator = &os.Args[2]
	}

	path := "/test/thr"

	data := make([]byte, length)
	for i := 0; i < length; i++ {
		data[i] = byte(i % 10)
	}

	p, err := yaks.NewPath(path)
	if err != nil {
		panic(err.Error())
	}
	v := yaks.NewRawValue(data)

	fmt.Println("Login to Yaks...")
	y, err := yaks.Login(locator, nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Use Workspace on '/'")
	root, _ := yaks.NewPath("/")
	w := y.Workspace(root)

	fmt.Printf("Put on %s : %db\n", p.ToString(), len(data))

	for {
		err = w.Put(p, v)
		if err != nil {
			panic(err.Error())
		}
	}

}
