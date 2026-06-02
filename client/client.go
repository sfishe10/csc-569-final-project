package main

import (
	"bufio"
	"fmt"
	"lab2/shared"
	"net/rpc"
	"os"
	"strings"
)

var contexts = make(map[string]shared.Context)

func put(server rpc.Client, key string, data string) {
	obj := shared.PutRequest{
		Key:     key,
		Object:  data,
		Context: contexts[key],
	}

	var reply bool
	err := server.Call("Requests.SendPutRequest", obj, &reply)
	if err != nil {
		fmt.Println("Error sending Put request:", err)
	}

}

func get(server rpc.Client, key string) shared.ObjectVersion {
	var reply shared.ObjectVersion
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

	fmt.Println("Usage:\nPUT <key> <obj>\nGET<key>")

	for {
		fmt.Print("> ")

		if !scanner.Scan() {
			break
		}

		cmd := scanner.Text()

		if cmd == "exit" {
			break
		}

		args := strings.Fields(cmd)

		if strings.Contains(strings.ToLower(args[0]), "get") {
			key := args[1]
			result := get(*server, key)

			fmt.Printf("%v\n", result)
			continue
		}

		if strings.Contains(strings.ToLower(args[0]), "put") {
			key := args[1]
			obj := args[2]

			put(*server, key, obj)

			fmt.Println("")
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}
