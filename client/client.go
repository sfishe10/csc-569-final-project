package main

import (
	"fmt"
	"lab2/shared"
	"math/rand"
	"net/rpc"
	"os"
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
)

var self_node shared.Node

var candidate_timer *time.Timer

// track whether this node has voted in this term
var voted bool = false

func resetCandidateTimer(server rpc.Client) {
	// wait at least 10 seconds to start holding elections (so I have time to start all the client processes)
	timeout := time.Duration(10000+rand.Intn(5001)) * time.Millisecond

	if candidate_timer != nil {
		candidate_timer.Stop()
	}

	candidate_timer = time.AfterFunc(timeout, func() {
		if !voted {
			self_node.Role = ROLE_CANDIDATE
			voted = true

			holdElection(server)
		}
	})
}

func holdElection(server rpc.Client) {
	fmt.Printf("Holding election for Node %d\n", self_node.ID)

	// vote for self
	var ballot = shared.NewBallot(self_node.ID)
	ballot.VoteYes()

	var reply bool

	// request votes from other nodes
	err := server.Call("Requests.SendBallot", ballot, &reply)
	if err != nil {
		fmt.Println("Error sending ballot:", err)
	}

	// wait to receive votes, then count
	time.AfterFunc(time.Second*VOTE_TIME, func() {
		var responses []shared.VoteResponse

		// Check for votes
		err := server.Call("Requests.ListenVoteResponses", self_node.ID, &responses)
		if err != nil {
			fmt.Println("Error listening for votes:", err)
		}

		for _, vote := range responses {
			if vote.Granted {
				ballot.VoteYes()
			} else {
				ballot.VoteNo()
			}
		}

		fmt.Printf("BALLOT: Yes(%d)\tNo(%d)\n", ballot.YesCount, ballot.NoCount)
		if ballot.HasMajority() {
			fmt.Printf("Node %d is now Leader\n", self_node.ID)
			self_node.Role = ROLE_LEADER
		} else if ballot.IsTie() {
			fmt.Printf("Election resulted in a tie\n")
			// wait a random amount of time and hold elections again
			waitTime := time.Duration(150+rand.Intn(151)) * time.Millisecond
			time.AfterFunc(waitTime, func() {
				holdElection(server)
			})
		} else {
			fmt.Printf("Node %d lost the election\n", self_node.ID)
			self_node.Role = ROLE_FOLLOWER
		}
	})
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

// Read incoming messages from other nodes
func readMessages(server rpc.Client, id int, membership shared.Membership) *shared.Membership {
	var incomingMemberships []shared.MembershipRequest
	var incomingBallots []shared.Ballot

	// Call RPC to check for messages
	err := server.Call("Requests.ListenMemberships", id, &incomingMemberships)
	if err != nil {
		fmt.Println("Error reading membership messages:", err)
		return &membership
	}

	err = server.Call("Requests.ListenBallots", id, &incomingBallots)
	if err != nil {
		fmt.Println("Error reading ballot messages:", err)
		return &membership
	}

	// Merge all incoming tables into the current membership
	updated := &membership
	for _, request := range incomingMemberships {
		updated = shared.CombineTables(updated, &(request.Table))
	}

	// check for nodes whose heartbeat hasn't increased in the last 18 seconds
	for _, node := range updated.Members {
		if node.ID == id {
			continue
		}
		if (calcTime()-node.Time)/1e9 > 18 {
			// mark as dead
			node.Alive = false
			var reply shared.Node
			updated.Update(node, &reply)

			// if the leader has died and we haven't already voted for a candidate, become a candidate
			if node.Role == ROLE_LEADER && !voted {
				self_node.Role = ROLE_CANDIDATE

				holdElection(server)
			}
		} else {
			if node.Role == ROLE_LEADER {
				resetCandidateTimer(server)
			}
		}
	}

	for _, request := range incomingBallots {
		if !voted {
			fmt.Printf("Node %d voted Yes for Node %d\n", self_node.ID, request.NodeID)
			response := shared.VoteResponse{
				CandidateID: request.NodeID,
				Granted:     true,
			}
			var reply bool
			server.Call("Requests.AddVote", response, &reply)
			voted = true
		} else {
			fmt.Printf("Node %d voted No for Node %d\n", self_node.ID, request.NodeID)
			response := shared.VoteResponse{
				CandidateID: request.NodeID,
				Granted:     false,
			}
			var reply bool
			server.Call("Requests.AddVote", response, &reply)
			request.VoteNo()
		}
	}

	return updated
}

func calcTime() float64 {
	return float64(time.Now().UnixNano())
}

var wg = &sync.WaitGroup{}

func main() {
	rand.Seed(time.Now().UnixNano())
	Z_TIME := rand.Intn(Z_TIME_MAX-Z_TIME_MIN) + Z_TIME_MIN

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

	fmt.Println("Node", id, "will fail after", Z_TIME, "seconds")

	currTime := calcTime()
	// Construct self
	self_node = shared.Node{ID: id, Hbcounter: 0, Time: currTime, Alive: true, Role: ROLE_FOLLOWER}
	var self_node_response shared.Node // Allocate space for a response to overwrite this

	// Add node with input ID
	if err := server.Call("Membership.Add", self_node, &self_node_response); err != nil {
		fmt.Println("Error:2 Membership.Add()", err)
	} else {
		fmt.Printf("Success: Node created with id= %d\n", id)
	}

	// start the countdown to when this node will become a candidate
	resetCandidateTimer(*server)

	neighbors := self_node.InitializeNeighbors(id)
	fmt.Println("Neighbors:", neighbors)

	membership := shared.NewMembership()
	membership.Add(self_node, &self_node)

	sendMessage(*server, neighbors[0], *membership)

	// crashTime := self_node.CrashTime()

	time.AfterFunc(time.Second*X_TIME, func() { runAfterX(server, &self_node, &membership, id) })
	time.AfterFunc(time.Second*Y_TIME, func() { runAfterY(server, neighbors, &membership, id) })
	time.AfterFunc(time.Second*time.Duration(Z_TIME), func() { runAfterZ(server, id) })

	wg.Add(1)
	wg.Wait()
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
	printMembership(**membership)

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
		fmt.Printf("Node %d (%s) has hb %d, time %.1f and %s\n", val.ID, val.Role, val.Hbcounter, val.Time, status)
	}
	fmt.Println("")
}
