package main

import (
	"TikBase/pack/net/tcp/tiko"
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

var cli Client

var (
	errNumOfArguments = errors.New("invalid number of arguments")
	errInvalidCommand = errors.New("invalid command")

	address = "127.0.0.1:9999"
)

type Client interface {
	Get(key string) (string, error)
	Set(key, value string) error
	Del(key string) error
	Expire(key string, ttl int64) error
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	time.Sleep(time.Second)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		panic(err)
	}

	// 创建客户端
	cli = NewClient("tiko", conn)

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
			fmt.Println("bye!")
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
	case "del":
		parseDelCommand(writer, command)
	case "expire":
		parseExpireCommand(writer, command)
	default:
		Error(writer, errInvalidCommand)
	}
}

func parseSetCommand(writer io.Writer, command []string) {
	if len(command) != 3 {
		_, _ = fmt.Fprintln(writer, errNumOfArguments.Error())
		return
	}
	err := cli.Set(command[1], command[2])
	if err != nil {
		Error(writer, err)
		return
	}
	OK(writer)
}

func parseGetCommand(writer io.Writer, command []string) {
	if len(command) != 2 {
		Error(writer, errNumOfArguments)
		return
	}
	val, err := cli.Get(command[1])
	if err != nil {
		Error(writer, err)
		return
	}
	_, _ = fmt.Fprintln(writer, val)
}

func parseDelCommand(writer io.Writer, command []string) {
	if len(command) != 2 {
		Error(writer, errNumOfArguments)
		return
	}
	err := cli.Del(command[1])
	if err != nil {
		Error(writer, err)
		return
	}
	OK(writer)
}

func parseExpireCommand(writer io.Writer, command []string) {
	if len(command) != 3 {
		_, _ = fmt.Fprintln(writer, errNumOfArguments.Error())
		return
	}

	ttl, err := strconv.Atoi(command[2])
	if err != nil {
		Error(writer, err)
		return
	}
	err = cli.Expire(command[1], int64(ttl))
	if err != nil {
		Error(writer, err)
		return
	}
	OK(writer)
}

func Error(writer io.Writer, err error) {
	_, _ = fmt.Fprintln(writer, "["+strings.ToUpper(err.Error())+"]")
}

func OK(writer io.Writer) {
	_, _ = fmt.Fprintln(writer, "[OK]")
}

func NewClient(name string, conn net.Conn) Client {
	name = strings.ToLower(name)

	switch name {
	case "tiko":
		return tiko.NewClient(conn)
	default:
		panic("invalid name")
	}
}
