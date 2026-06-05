package main

import (
	"fmt"
	"lab2/shared"
	"maps"
	"math/rand"
	"net/rpc"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

const (
	MAX_NODES      = 8
	X_TIME         = 1
	Y_TIME         = 2
	Z_TIME_MAX     = 100
	Z_TIME_MIN     = 10
	ROLE_FOLLOWER  = "FOLLOWER"
	ROLE_CANDIDATE = "CANDIDATE"
	ROLE_LEADER    = "LEADER"
	VOTE_TIME      = 8
	N              = 3 // how many nodes to replicate data on
	W              = 2 // write quorum
	R              = 2 // read quorum
	QUORUM_TIMEOUT = 5 * time.Second
)

var self_node shared.Node

var membership *shared.Membership

// the primary designated replicas
var preference_list []int

func findFailedNode(responsiveNodeIds []int) int {
	for _, id := range preference_list {
		if !slices.Contains(responsiveNodeIds, id) {
			return id
		}
	}

	return -1
}

func findNextHealthyNode() int {
	lastNode := preference_list[len(preference_list)-1]
	allNodes := membership.SortedNodes()

	if len(allNodes) == 0 {
		return -1
	}

	startIndex := -1
	for i, node := range allNodes {
		if node.ID == lastNode {
			startIndex = i
			break
		}
	}

	if startIndex == -1 {
		return -1
	}

	for offset := 1; offset <= len(allNodes); offset++ {
		i := (startIndex + offset) % len(allNodes)
		node := allNodes[i]

		if node.Alive {
			return node.ID
		}
	}

	return -1 // no healthy node found
}

func markNodeAsDead(id int) {
	var failedNode shared.Node
	membership.Get(id, &failedNode)
	failedNode.Alive = false
	membership.Update(failedNode, &failedNode)
}

// Send the current membership table to a neighboring node with the provided ID
func sendMessage(server rpc.Client, id int, membership shared.Membership) {
	request := shared.MembershipRequest{
		ID:    id,
		Table: membership,
	}

	var reply bool

	err := server.Call("Requests.AddMembership", request, &reply)
	if err != nil {
		fmt.Println("Error sending message:", err)
	}
}

func handleMemberships(server rpc.Client, id int, membership shared.Membership) *shared.Membership {
	var incomingMemberships []shared.MembershipRequest

	// Call RPC to check for messages
	err := server.Call("Requests.ListenMemberships", id, &incomingMemberships)
	if err != nil {
		fmt.Println("Error reading membership messages:", err)
		return &membership
	}

	// Merge all incoming tables into the current membership
	updated := &membership
	for _, request := range incomingMemberships {
		updated = shared.CombineTables(updated, &(request.Table))
	}

	// check for nodes whose heartbeat hasn't increased in the last 5 seconds
	for _, node := range updated.Members {
		if node.ID == id {
			continue
		}
		elapsed := (calcTime() - node.Time) / 1e9

		if elapsed > 20 {
			delete(updated.Members, node.ID)
			continue
		}

		if elapsed > 5 {
			node.Alive = false
			var reply shared.Node
			updated.Update(node, &reply)
			continue
		}

		// if this node has come back to life and we're storing temporary data for it, transfer the data back
		var currentNode shared.Node
		membership.Get(node.ID, &currentNode)

		if currentNode.Alive {
			// we already know it's alive
			continue
		}

		tempData := shared.DataTransfer{TargetID: node.ID, Data: self_node.Store.GetHintedData(node.ID)}
		if len(tempData.Data) == 0 {
			continue
		}

		fmt.Printf("Node %d is back. Transferring data.\n", node.ID)

		transferData(server, tempData)

	}

	return updated
}

func handleCoordPutRequest(server rpc.Client, id int) bool {
	var incomingCoordPutRequest shared.PutRequest

	err := server.Call("Requests.ListenCoordPutRequest", id, &incomingCoordPutRequest)
	if err != nil {
		fmt.Println("Error reading coord put request:", err)
		return false
	}

	if incomingCoordPutRequest.Key == "" {
		return true
	}

	fmt.Printf("COORD PUT REQUEST: %v -> %v\n", incomingCoordPutRequest.Key, incomingCoordPutRequest.Object)

	// write locally
	obj_version := shared.ObjectVersion{
		Object:  incomingCoordPutRequest.Object,
		Context: incomingCoordPutRequest.Context,
	}
	self_node.Store.Put(incomingCoordPutRequest.Key, obj_version)

	// the request has already been sent to the replicas, so wait for W - 1 responses
	deadline := time.After(QUORUM_TIMEOUT)
	acks := 1 // coordinator counts itself
	ack_ids := []int{}

	for acks < W {
		select {
		case <-deadline:
			// write failed
			fmt.Printf("WRITE FAILED: %v\n", incomingCoordPutRequest.Key)
			var reply bool
			err := server.Call("Requests.SendPutResultToClient", shared.Context{}, &reply)

			if err != nil {
				fmt.Println("Error sending failure to client:", err)
				return false
			}

			return false
		default:
			temp_ack_ids := []int{}
			err := server.Call("Requests.ListenReplicaPutResponses", id, &temp_ack_ids)
			if err != nil {
				fmt.Println("Error listening for responses:", err)
				return false
			}

			acks += len(temp_ack_ids)
			ack_ids = append(ack_ids, temp_ack_ids...)

			time.Sleep(50 * time.Millisecond)
		}
	}

	// the W quorum was reached, so return data to client
	fmt.Printf("PUT SUCCESSFUL: %v\n", incomingCoordPutRequest.Key)

	var reply bool
	err = server.Call("Requests.SendPutResultToClient", incomingCoordPutRequest.Context, &reply)

	if err != nil {
		fmt.Println("Error sending put result to client:", err)
		return false
	}

	// continue checking for the rest of the responses
	deadline = time.After(QUORUM_TIMEOUT)
	for acks <= N {
		select {
		case <-deadline:
			// One of the nodes is down
			failedNodeId := findFailedNode(ack_ids)
			// mark this node as dead
			markNodeAsDead(failedNodeId)

			// find the next available node
			subNode := findNextHealthyNode()

			if subNode == -1 {
				// no healthy node was found
				fmt.Println("No substitute node found.")
				return false
			}

			// transfer data to that node
			fmt.Printf("Node %d is down. Sending data to Node %d instead.\n", failedNodeId, subNode)
			incomingCoordPutRequest.TargetID = failedNodeId
			incomingCoordPutRequest.SubID = subNode

			var reply bool
			err := server.Call("Requests.SendPutRequest", incomingCoordPutRequest, &reply)
			if err != nil {
				fmt.Println("Error sending Put request:", err)
			}

			return false
		default:
			temp_ack_ids := []int{}
			err := server.Call("Requests.ListenReplicaPutResponses", id, &temp_ack_ids)
			if err != nil {
				fmt.Println("Error listening for responses:", err)
				return false
			}

			acks += len(temp_ack_ids)
			ack_ids = append(ack_ids, temp_ack_ids...)

			time.Sleep(50 * time.Millisecond)
		}
	}

	return true
}

func handleCoordGetRequest(server rpc.Client, id int) bool {
	var key string

	err := server.Call("Requests.ListenCoordGetRequest", id, &key)
	if err != nil {
		fmt.Println("Error reading coord get request:", err)
		return false
	}

	if key == "" {
		return true
	}

	fmt.Printf("COORD GET REQUEST: %v\n", key)

	// Collect coordinator's local versions first.
	collected := shared.NewStore()

	for _, version := range self_node.Store.Data[key] {
		collected.Put(key, version)
	}

	// the request has already been sent to the replicas, so wait for R - 1 responses
	deadline := time.After(QUORUM_TIMEOUT)
	acks := 1 // coordinator counts itself

	for acks < R {
		select {
		case <-deadline:
			fmt.Println("GET FAILED")
			return false
		default:
			responses := []shared.GetResponse{}
			err = server.Call("Requests.ListenReplicaGetResponses", id, &responses)
			if err != nil {
				fmt.Println("Error listening for responses:", err)
				return false
			}

			acks += len(responses)

			for _, response := range responses {
				for _, version := range response.Versions {
					collected.Put(key, version)
				}
			}

			time.Sleep(50 * time.Millisecond)
		}
	}

	// send all the collected versions back to the client
	finalVersions := collected.Data[key]

	fmt.Printf("GET %s returned %d version(s):\n", key, len(finalVersions))
	shared.PrintVersions(finalVersions)

	var reply bool
	err = server.Call("Requests.SendGetResultsToClient", finalVersions, &reply)
	if err != nil {
		fmt.Println("Error sending results to client:", err)
		return false
	}

	return true
}

func handleReplicaPutRequest(server rpc.Client, id int) {
	var incomingReplicaPutRequest shared.PutRequest

	err := server.Call("Requests.ListenReplicaPutRequest", id, &incomingReplicaPutRequest)
	if err != nil {
		fmt.Println("Error reading replica put request:", err)
		return
	}

	if incomingReplicaPutRequest.Key == "" {
		return
	}

	if incomingReplicaPutRequest.SubID != -1 {
		// the intended replica node is down, so this node is being used as substitute
		fmt.Printf("REPLICATING (temporary): %v -> %v\n", incomingReplicaPutRequest.Key, incomingReplicaPutRequest.Object)

		// write locally
		obj_version := shared.ObjectVersion{
			Object:  incomingReplicaPutRequest.Object,
			Context: incomingReplicaPutRequest.Context,
		}
		self_node.Store.AddHint(incomingReplicaPutRequest.TargetID, incomingReplicaPutRequest.Key, obj_version)

		// send a response so the coordinator knows it was written
		var reply bool
		err = server.Call("Requests.RespondToPutRequest", id, &reply)
		if err != nil {
			fmt.Println("Error responding to put request:", err)
			return
		}

		return
	}

	fmt.Printf("REPLICATING: %v -> %v\n", incomingReplicaPutRequest.Key, incomingReplicaPutRequest.Object)

	// write locally
	obj_version := shared.ObjectVersion{
		Object:  incomingReplicaPutRequest.Object,
		Context: incomingReplicaPutRequest.Context,
	}
	self_node.Store.Put(incomingReplicaPutRequest.Key, obj_version)

	// send a response so the coordinator knows it was written
	var reply bool
	err = server.Call("Requests.RespondToPutRequest", id, &reply)
	if err != nil {
		fmt.Println("Error responding to put request:", err)
		return
	}
}

func handleReplicaGetRequest(server rpc.Client, id int) {
	var key string

	err := server.Call("Requests.ListenReplicaGetRequest", id, &key)
	if err != nil {
		fmt.Println("Error reading replica get request:", err)
		return
	}

	if key == "" {
		return
	}

	localVersions := self_node.Store.Data[key]
	resp := shared.GetResponse{
		FromNodeID: id,
		Key:        key,
		Versions:   localVersions,
	}

	fmt.Printf("SENDING GET RESPONSE TO COORD: %v\n", resp)

	// send local versions to the coordinator
	var reply bool
	err = server.Call("Requests.RespondToGetRequest", resp, &reply)
	if err != nil {
		fmt.Println("Error responding to get request:", err)
		return
	}
}

// Read incoming messages from other nodes
func readMessages(server rpc.Client, id int, membership shared.Membership) *shared.Membership {
	updatedMembership := handleMemberships(server, id, membership)

	handleCoordPutRequest(server, id)
	handleReplicaPutRequest(server, id)
	handleCoordGetRequest(server, id)
	handleReplicaGetRequest(server, id)

	return updatedMembership
}

func transferData(server rpc.Client, dataTransfer shared.DataTransfer) {
	var reply bool

	err := server.Call("Requests.SendDataToRevivedNode", dataTransfer, &reply)
	if err != nil {
		fmt.Println("Error transferring data:", err)
		return
	}

	// make sure the node received the data before deleting
	deadline := time.After(QUORUM_TIMEOUT)
	var res bool
	for !res {
		select {
		case <-deadline:
			fmt.Println("TRANSFER FAILED")
			return
		default:
			err = server.Call("Requests.ListenDataTransferResult", dataTransfer, &res)
			if err != nil {
				fmt.Println("Error listening for transfer result:", err)
				return
			}

			time.Sleep(50 * time.Millisecond)
		}
	}

	// the node received the data, so delete it here
	fmt.Printf("Data transfered to node %d. Deleting from local store.\n", dataTransfer.TargetID)
	delete(self_node.Store.Hints, dataTransfer.TargetID)
}

func checkForDataTransfers(server rpc.Client) {
	var dataTransfers []shared.DataTransfer

	err := server.Call("Requests.CheckForDataTransfers", self_node.ID, &dataTransfers)
	if err != nil {
		fmt.Println("Error checking for data transfers:", err)
		return
	}

	for _, dt := range dataTransfers {
		maps.Copy(self_node.Store.Data, dt.Data)
	}
}

func calcTime() float64 {
	return float64(time.Now().UnixNano())
}

var wg = &sync.WaitGroup{}

func main() {
	rand.Seed(time.Now().UnixNano())
	// Z_TIME := rand.Intn(Z_TIME_MAX-Z_TIME_MIN) + Z_TIME_MIN

	// Connect to RPC server
	server, _ := rpc.DialHTTP("tcp", "localhost:9005")

	args := os.Args[1:]

	// Get ID from command line argument
	if len(args) == 0 {
		fmt.Println("No args given")
		return
	}
	id, err := strconv.Atoi(args[0])
	if err != nil {
		fmt.Println("Found Error", err)
	}

	// fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

	currTime := calcTime()
	// Construct self
	var store = shared.NewStore()
	self_node = shared.Node{ID: id, Hbcounter: 0, Time: currTime, Alive: true, Store: *store}
	var self_node_response shared.Node // Allocate space for a response to overwrite this

	preference_list = shared.GetPreferenceList(id)

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, &self_node_response); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	// clear out any stale messages in case the node is coming back from temporary failure
	clearStaleMessages(server)

	neighbors := self_node.InitializeNeighbors(id)
	fmt.Println("Neighbors:", neighbors)

	membership = shared.NewMembership()
	membership.Add(self_node, &self_node)

	sendMessage(*server, neighbors[0], *membership)

	// crashTime := self_node.CrashTime()

	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, &membership, id) })
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, &membership, id) })
	// time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	wg.Add(1)
	wg.Wait()
}

func clearStaleMessages(server *rpc.Client) {
	id := self_node.ID

	var reply bool
	err := server.Call("Requests.ClearStaleRequests", id, &reply)
	if err != nil {
		fmt.Println("Error clearing out stale requests:", err)
	}

}

func runAfterX(server *rpc.Client, node *shared.Node, membership **shared.Membership, id int) {
	if !self_node.Alive {
		return
	}

	// increase heartbeat counter
	node.Hbcounter += 1
	node.Time = calcTime()

	var reply shared.Node
	(*membership).Update(*node, &reply)

	time.AfterFunc(time.Second*X_TIME, func() {
		runAfterX(server, node, membership, id)
	})
}

func runAfterY(server *rpc.Client, neighbors [2]int, membership **shared.Membership, id int) {
	if !self_node.Alive {
		return
	}

	// read incoming messages
	*membership = readMessages(*server, id, **membership)
	// printMembership(**membership)

	// send table to neighbors
	sendMessage(*server, neighbors[0], **membership)
	sendMessage(*server, neighbors[1], **membership)

	time.AfterFunc(time.Second*Y_TIME, func() {
		runAfterY(server, neighbors, membership, id)
	})
}

func runAfterZ(server *rpc.Client, id int) {
	// this node fails
	fmt.Printf("NODE %d FAILED\n", id)
	self_node.Alive = false
	server.Close()
	wg.Done()
}

func printMembership(m shared.Membership) {
	for _, val := range m.Members {
		status := "is Alive"
		if !val.Alive {
			status = "is Dead"
		}
		fmt.Printf("Node %d has hb %d, time %.1f and %s\n", val.ID, val.Hbcounter, val.Time, status)
	}
	fmt.Println("")
}
