package main

import (
	"bufio"
	"fmt"
	"lab2/shared"
	"net/rpc"
	"os"
	"strings"
	"time"
)

const QUORUM_TIMEOUT = 15 * time.Second

var contexts = make(map[string]shared.Context)

func put(server rpc.Client, key string, data string) {
	obj := shared.PutRequest{
		Coord_id: -1, // this will be updated later
		Key:      key,
		Object:   data,
		Context:  contexts[key],
	}

	var reply bool
	err := server.Call("Requests.SendPutRequest", obj, &reply)
	if err != nil {
		fmt.Println("Error sending Put request:", err)
	}

}

func get(server rpc.Client, key string) []shared.ObjectVersion {
	var reply bool
	err := server.Call("Requests.SendGetRequest", key, &reply)
	if err != nil {
		fmt.Println("Error sending Get request:", err)
	}

	// wait for the response
	deadline := time.After(QUORUM_TIMEOUT)
	var res []shared.ObjectVersion
	for len(res) == 0 {
		select {
		case <-deadline:
			fmt.Println("GET FAILED")
			return res
		default:
			err = server.Call("Requests.ListenGetResults", key, &res)
			if err != nil {
				fmt.Println("Error listening for results:", err)
				return res
			}

			time.Sleep(50 * time.Millisecond)
		}
	}

	return res
}

func main() {

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("Usage:\nPUT <key> <obj>\nGET <key>")

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

			fmt.Println("PUT successful")
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}
