package main

import (
	"custodian/logger"
	"custodian/server"
	"custodian/utils"
	"github.com/getsentry/raven-go"
	"log"
	"os"
)

type OptsDesc struct {
	prmsCnt int
	handler func(p []string) error
}

func init() {
	logger.SetOut(os.Stdout)
	logger.SetLevel("debug")
	log.Printf("The logger is initialized: level: '%s', output: '%s'.\n", "debug", "stdout")

	appConfig := utils.GetConfig()
	if len(appConfig.SentryDsn) > 0 {
		raven.SetDSN(appConfig.SentryDsn)
	}
}

//Main function runs Custodian server. The following options are avaliable:
// -a - address to use. Default value is empty.
// -p - port to use. Default value is 8080.
// -r - path root to use. Default value is "/custodian".
//Setup example: ./custodian -d "host=infra-pdb01 user=custodian password=custodian dbname=custodian_test sslmode=disable"

//TODO: The application has 2 ways of configuration now: command line arguments and dotenv file
//it should be unified somehow
func main() {
	//instantiate Server with default configuration
	var srv = server.New("", "8000", "/custodian", "")

	//apply command-line-specified options if there are some
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

	//get AppConfig
	appConfig := utils.GetConfig()
	log.Println("Custodian server started.")
	srv.Setup(appConfig).ListenAndServe()
}
