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

const QUORUM_TIMEOUT = 10 * time.Second

var contexts = make(map[string]shared.Context)

func put(server rpc.Client, key string, data string) bool {
	obj := shared.PutRequest{
		CoordID:  -1, // this will be updated later
		TargetID: -1,
		Key:      key,
		Object:   data,
		Context:  contexts[key],
	}

	var reply bool
	err := server.Call("Requests.SendPutRequest", obj, &reply)
	if err != nil {
		fmt.Println("Error sending Put request:", err)
	}

	// wait for the response
	deadline := time.After(QUORUM_TIMEOUT)
	var res bool
	for {
		select {
		case <-deadline:
			fmt.Println("PUT FAILED")
			return false
		default:
			err = server.Call("Requests.ListenPutResult", key, &res)
			if err != nil {
				fmt.Println("Error listening for result:", err)
				return res
			}

			if res {
				return true
			}

			time.Sleep(50 * time.Millisecond)
		}
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

	fmt.Println("Usage:\nPUT <key> <value>\nGET <key>")

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
			if len(args) < 2 {
				fmt.Println("Usage: GET <key>")
				continue
			}
			key := args[1]
			result := get(*server, key)

			shared.PrintVersions(result)
			continue
		}

		if strings.Contains(strings.ToLower(args[0]), "put") {
			if len(args) < 3 {
				fmt.Println("Usage: PUT <key> <value>")
				continue
			}

			key := args[1]
			obj := args[2]

			res := put(*server, key, obj)

			if res {
				fmt.Println("PUT succeeded")
			}
			continue
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}
