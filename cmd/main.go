package main

import (
	"bufio"
	"flag"
	"fmt"
	"kataklysm/pkg/lsm"
	"log"
	"os"
	"runtime/pprof"
	"strings"
)

func load(l *lsm.LSM) {
	file, err := os.Open("../test.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// optionally, resize scanner's capacity for lines over 64K, see next example
	i := 0
	prf, _ := os.Create("kataklysm.pprof")
	pprof.StartCPUProfile(prf)
	defer pprof.StopCPUProfile()
	for scanner.Scan() {
		s := strings.Clone(scanner.Text())
		l.Set(s, []byte(s))
		if i%1000 == 0 {
			print(i, s)
		}
		i++
	}
	l.Sync()
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	l := lsm.CreateLSM(10000)
	mmode := flag.Bool("manual", false, "Set manual mode")
	flag.Parse()
	if *mmode {
		fmt.Println("Welcome to kataklysm. Valid commands: [add <key> <value>, get <key>, flush]")
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Enter text: ")
		for {
			fmt.Print("$ ")
			text, _ := reader.ReadString('\n')
			args := strings.Split(text[:len(text)-1], " ")
			if args[0] == "add" && len(args) == 3 {
				l.Set(args[1], []byte(args[2]))
			} else if args[0] == "get" && len(args) == 2 {
				r, err := l.Get(args[1])
				if err == nil {
					fmt.Println("Got: ", r)
				} else {
					fmt.Println("Error: ", err)
				}
			} else if args[0] == "flush" {
				l.Flush()
			}
		}
	} else {
		load(l)
	}
}
