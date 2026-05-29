package main

import (
	"bufio"
	"fmt"
	"lab2/shared"
	"net/rpc"
	"os"
	"strings"
)

func put(server rpc.Client, key string, data string) {
	obj := shared.DBObject{
		Key:     key,
		Object:  data,
		Context: "",
	}

	var reply bool
	err := server.Call("Requests.SendPutRequest", obj, &reply)
	if err != nil {
		fmt.Println("Error sending Put request:", err)
	}

}

func get(server rpc.Client, key string) shared.DBObject {
	var reply shared.DBObject
	err := server.Call("Requests.SendGetRequest", key, &reply)
	if err != nil {
		fmt.Println("Error sending Get request:", err)
	}

	return reply
}

func main() {

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Enter text (Ctrl+D to quit on macOS/Linux, Ctrl+Z then Enter on Windows):")

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		cmd := scanner.Text()

		if cmd == "exit" {
			break
		}

		if strings.Contains(cmd, "get") {
			afterOpenParen := strings.Split(cmd, "(")[1]
			key, _, _ := strings.Cut(afterOpenParen, ")")
			result := get(*server, key)

			fmt.Printf("%v\n", result)
			continue
		}

		if strings.Contains(cmd, "put") {
			afterOpenParen := strings.Split(cmd, "(")[1]
			beforeCloseParen := strings.Split(afterOpenParen, ")")[0]
			args := strings.Split(beforeCloseParen, ",")

			key := strings.TrimSpace(args[0])
			obj := strings.TrimSpace(args[1])

			put(*server, key, obj)

			fmt.Println("")
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}
