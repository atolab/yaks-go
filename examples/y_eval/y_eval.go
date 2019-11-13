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

	p, err := yaks.NewPath("/demo/example/yaks-go-eval")
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
	w := y.WorkspaceWithExecutor(root)

	fmt.Println("Register eval " + p.ToString())
	err = w.RegisterEval(p,
		func(path *yaks.Path, props yaks.Properties) yaks.Value {
			// In this Eval function, we choosed to get the name to be returned in the StringValue in 3 possible ways,
			// depending the properties specified in the selector. For example, with the following selectors:
			//   - "/demo/example/yaks-java-eval" : no properties are set, a default value is used for the name
			//   - "/demo/example/yaks-java-eval?(name=Bob)" : "Bob" is used for the name
			//   - "/demo/example/yaks-java-eval?(name=/demo/example/name)" :
			//     the Eval function does a GET on "/demo/example/name" an uses the 1st result for the name

			fmt.Printf(">> Processing eval for path %s with properties: %s\n", path, props)
			name := props["name"]
			if name == "" {
				name = "Yaks Go!"
			}

			if name[0] == '/' {
				fmt.Printf("   >> Get name to use from Yaks at path: %s\n", name)
				s, err := yaks.NewSelector(name)
				if err == nil {
					kvs := w.Get(s)
					if len(kvs) > 0 {
						name = kvs[0].Value().ToString()
					}
				}
			}
			fmt.Printf("   >> Returning string: \"Eval from %s\"\n", name)
			return yaks.NewStringValue("Eval from " + name)
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

	w.UnregisterEval(p)
	if err != nil {
		panic(err.Error())
	}

	err = y.Logout()
	if err != nil {
		panic(err.Error())
	}

}
