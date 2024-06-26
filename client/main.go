package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/T4t4KAU/TikBase/pkg/rpc/data"
	"github.com/T4t4KAU/TikBase/pkg/rpc/data/dataservice"
	"github.com/T4t4KAU/TikBase/pkg/utils"
	"github.com/cloudwego/kitex/client"

	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

var logo = " _____ _ _    ____                 \n|_   _(_) | _| __ )  __ _ ___  ___ \n  | | | | |/ /  _ \\ / _` / __|/ _ \\\n  | | | |   <| |_) | (_| \\__ \\  __/\n  |_| |_|_|\\_\\____/ \\__,_|___/\\___|\n"

var (
	errNumOfArguments = errors.New("invalid number of arguments")
	errInvalidCommand = errors.New("invalid command")

	address = "127.0.0.1:10081"
)

var cli dataservice.Client

func main() {
	reader := bufio.NewReader(os.Stdin)
	time.Sleep(time.Second)

	var err error
	// 创建客户端

	cli, err = dataservice.NewClient(address, client.WithHostPorts(address))
	if err != nil {
		panic(err)
	}

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

	ctx := context.Background()
	req := &data.SetReq{
		Key:   command[1],
		Value: utils.S2B(command[2]),
	}

	_, err := cli.Set(ctx, req)
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

	ctx := context.Background()
	req := &data.GetReq{
		Key: command[1],
	}

	resp, err := cli.Get(ctx, req)
	if err != nil {
		Error(writer, err)
		return
	}
	_, _ = fmt.Fprintln(writer, string(resp.Value))
}

func parseDelCommand(writer io.Writer, command []string) {
	if len(command) != 2 {
		Error(writer, errNumOfArguments)
		return
	}

	ctx := context.Background()
	req := &data.DelReq{
		Key: command[1],
	}

	_, err := cli.Del(ctx, req)
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

	ctx := context.Background()
	req := &data.ExpireReq{
		Key:  command[1],
		Time: int64(ttl),
	}

	_, err = cli.Expire(ctx, req)
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
