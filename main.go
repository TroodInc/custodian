package main

import (
	"fmt"
	"logger"
	"server"
	"log"
	"os"
)

type OptsError struct {
	arg, msg string
}

func (e *OptsError) Error() string {
	return fmt.Sprintf("Wrong argument '%s': %s", e.arg, e.msg)
}

type OptsDesc struct {
	prmsCnt int
	handler func(p []string) error
}

func init() {
	logger.SetOut(os.Stdout)
	logger.SetLevel("debug")
	log.Printf("The logger is initialized: level: '%s', output: '%s'.\n", "debug", "stdout")
}

//Main function runs Custodian server. The following options are avaliable:
// -a - address to use. Default value is empty.
// -p - port to use. Default value is 8080.
// -r - path root to use. Default value is "/custodian".
// -d - PostrgeSQL connection string. For example:
//host=infra-pdb01 user=custodian password=custodian dbname=custodian_test sslmode=disable
//For more information see https://godoc.org/github.com/lib/pq
//
//Run example: ./custodian -d "host=infra-pdb01 user=custodian password=custodian dbname=custodian_test sslmode=disable"
func main() {
	var srv = server.New("", "8080", "/custodian", "")

	var opts = map[string]OptsDesc{
		"-a": {1, func(p []string) error {
			srv.SetAddr(p[0])
			return nil
		}},
		"-p": {1, func(p []string) error {
			srv.SetPort(p[0])
			return nil
		}},
		"-r": {1, func(p []string) error {
			srv.SetRoot(p[0])
			return nil
		}},
		"-d": {1, func(p []string) error {
			srv.SetDb(p[0])
			return nil
		}},
	}

	args := os.Args[1:]
	for len(args) > 0 {
		if v, e := opts[args[0]]; e && len(args)-1 >= v.prmsCnt {
			if err := v.handler(args[1: v.prmsCnt+1]); err != nil {
				log.Fatalln(err)
				os.Exit(127)
			}
			args = args[1+v.prmsCnt:]
		} else {
			log.Fatalf("Wrong argument '%s'", args[0])
			os.Exit(127)
		}
	}

	log.Println("Custodian server started.")
	srv.Run()
}
