package main

import (
	"TikBase/engine"
	"TikBase/pack/net/tcp/tiko"
	"TikBase/pack/poll"
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

var cli *tiko.Client

var (
	errNumOfArguments = errors.New("invalid number of arguments")
	errInvalidCommand = errors.New("invalid command")
)

func startServer() {
	eng := engine.NewCacheEngine()
	p := poll.New(&poll.Config{
		Address:    "127.0.0.1:9999",
		MaxConnect: 20,
		Timeout:    time.Second,
	}, tiko.NewHandler(eng))
	err := p.Run()
	if err != nil {
		panic(err)
	}
}

func main() {
	address := "127.0.0.1:9999"
	reader := bufio.NewReader(os.Stdin)
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", "127.0.0.1:9999")
	if err != nil {
		panic(err)
	}

	cli = tiko.NewClient(conn)
	println(logo)
	println("connecting to: ", address)

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input:", err)
			continue
		}

		// 去除输入字符串两端的空格和换行符
		input = strings.TrimSpace(input)

		// 如果输入为空，则继续下一次循环
		if input == "" {
			continue
		}

		// 退出 REPL 程序
		if input == "exit" {
			fmt.Println("Goodbye!")
			break
		}

		parseLine(os.Stdout, input)
	}
}

func parseLine(writer io.Writer, line string) {
	command := strings.Split(line, " ")
	if len(command) <= 0 {
		_, _ = fmt.Fprintln(writer, errInvalidCommand.Error())
	}
	ins := command[0]
	ins = strings.ToLower(ins)
	switch ins {
	case "set":
		parseSetCommand(writer, command)
	case "get":
		parseGetCommand(writer, command)
	}
}

func parseSetCommand(writer io.Writer, command []string) {
	if len(command) != 3 {
		_, _ = fmt.Fprintln(writer, errNumOfArguments.Error())
		return
	}
	err := cli.Set(command[1], command[2])
	if err == nil {
		_, _ = fmt.Fprintln(writer, "[OK]")
	}
}

func parseGetCommand(writer io.Writer, command []string) {
	if len(command) != 2 {
		_, _ = fmt.Fprintln(writer, errNumOfArguments.Error())
		return
	}
	val, _ := cli.Get(command[1])
	_, _ = fmt.Fprintln(writer, val)
}
