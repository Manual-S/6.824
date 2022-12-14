package main

//
// start the master process, which is implemented
// in ../mr/master.go
//
// go run mrmaster.go pg*.txt
//
// Please do not change this file.
//

import (
	"../mr"
	"fmt"
	"log"
	"os"
	"time"
)

func main() {

	log.SetFlags(log.Llongfile | log.Ldate)

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster inputfiles...\n")
		os.Exit(1)
	}

	log.Printf("mrmaster os.Args[1:] is %v", os.Args[1:])

	m := mr.MakeMaster(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
