package main

import (
	"bufio"
	"fmt"
	"net/rpc"
	"os"
)

// Send the current membership table to a neighboring node with the provided ID
func put(server rpc.Client, key string, value string) {
	// use the current timestamp as the context?
}

// Read incoming messages from other nodes
func get(server rpc.Client, key string) string {
	return ""
}

func main() {

	// Connect to RPC server
	// server, _ := rpc.DialHTTP("tcp", "localhost:9005")

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

		fmt.Println("command:", cmd)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading input:", err)
	}
}
