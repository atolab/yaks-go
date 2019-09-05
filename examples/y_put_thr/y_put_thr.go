package main

import (
	"fmt"
	"os"
	"strconv"

	"github.com/atolab/yaks-go"
)

func main() {
	locator := "tcp/127.0.0.1:7447"
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
		locator = os.Args[2]
	}

	path := "/test/thr"

	chars := make([]byte, length)
	for i := range chars {
		chars[i] = 'X'
	}

	p, err := yaks.NewPath(path)
	if err != nil {
		panic(err.Error())
	}
	v := yaks.NewStringValue(string(chars))

	fmt.Println("Login to " + locator + "...")
	y, err := yaks.Login("tcp/127.0.0.1:7447", nil)
	if err != nil {
		panic(err.Error())
	}

	fmt.Println("Use Workspace on '/'")
	root, _ := yaks.NewPath("/")
	w := y.Workspace(root)

	fmt.Printf("Put on %s : %db\n", p.ToString(), len(v.ToString()))

	for {
		err = w.Put(p, v)
		if err != nil {
			panic(err.Error())
		}
	}

}
