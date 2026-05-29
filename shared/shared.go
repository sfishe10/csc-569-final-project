package shared

import (
	"errors"
	"fmt"
	"math/rand"
	"time"
)

const (
	MAX_NODES      = 8
	ROLE_FOLLOWER  = "FOLLOWER"
	ROLE_CANDIDATE = "CANDIDATE"
	ROLE_LEADER    = "LEADER"
	VOTE_TIME      = 8
)

// Node struct represents a computing node.
type Node struct {
	ID        int
	Hbcounter int
	Time      float64
	Alive     bool
	Role      string
}

// Generate random crash time from 10-60 seconds
func (n Node) CrashTime() int {
	rand.Seed(time.Now().UnixNano())
	max := 60
	min := 10
	return rand.Intn(max-min) + min
}

func (n Node) InitializeNeighbors(id int) [2]int {
	neighbor1 := (id % MAX_NODES) + 1
	neighbor2 := ((id + 1) % MAX_NODES) + 1

	return [2]int{neighbor1, neighbor2}
}

func RandInt() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(MAX_NODES-1+1) + 1
}

/*---------------*/

// Membership struct represents participanting nodes
type Membership struct {
	Members map[int]Node
}

// Returns a new instance of a Membership (pointer).
func NewMembership() *Membership {
	return &Membership{
		Members: make(map[int]Node),
	}
}

// Adds a node to the membership list.
func (m *Membership) Add(payload Node, reply *Node) error {
	m.Members[payload.ID] = payload
	*reply = payload
	return nil
}

// Updates a node in the membership list.
func (m *Membership) Update(payload Node, reply *Node) error {
	m.Members[payload.ID] = payload
	*reply = payload
	return nil
}

// Returns a node with specific ID.
func (m *Membership) Get(payload int, reply *Node) error {
	node := m.Members[payload]
	if node.ID == 0 {
		error := fmt.Sprintf("node with ID %d not found", payload)
		return errors.New(error)
	}
	*reply = node
	return nil
}

/*---------------*/

// MembershipRequest struct represents a new message request to a client
type MembershipRequest struct {
	ID    int
	Table Membership
}

type BallotRequest struct {
	ID     int
	Ballot Ballot
}

type VoteResponse struct {
	CandidateID int
	Granted     bool
}

// Requests struct represents pending message requests
type Requests struct {
	PendingMemberships map[int][]MembershipRequest
	PendingBallots     map[int][]BallotRequest
	PendingVotes       map[int][]VoteResponse
}

// Returns a new instance of a Requests (pointer).
func NewRequests() *Requests {
	return &Requests{
		PendingMemberships: make(map[int][]MembershipRequest),
		PendingBallots:     make(map[int][]BallotRequest),
		PendingVotes:       make(map[int][]VoteResponse),
	}
}

func (req *Requests) AddMembership(payload MembershipRequest, reply *bool) error {
	req.PendingMemberships[payload.ID] = append(req.PendingMemberships[payload.ID], payload)
	*reply = true
	return nil
}

func (req *Requests) AddBallot(payload BallotRequest, reply *bool) error {
	req.PendingBallots[payload.ID] = append(req.PendingBallots[payload.ID], payload)
	*reply = true
	return nil
}

func (req *Requests) AddVote(payload VoteResponse, reply *bool) error {
	req.PendingVotes[payload.CandidateID] = append(req.PendingVotes[payload.CandidateID], payload)
	*reply = true
	return nil
}

// Listens to communication from neighboring nodes.
func (req *Requests) ListenMemberships(ID int, reply *[]MembershipRequest) error {
	// Check if there's a pending message for this node
	if requests, exists := req.PendingMemberships[ID]; exists {
		*reply = requests
		delete(req.PendingMemberships, ID) // consume the message
	} else {
		// No message - return empty array
		*reply = []MembershipRequest{}
	}
	return nil
}

// checks for ballot requests
func (req *Requests) ListenBallots(ID int, reply *[]Ballot) error {
	// Check if there's a pending message for this node
	if requests, exists := req.PendingBallots[ID]; exists {
		var ballots []Ballot
		for _, ballot := range requests {
			ballots = append(ballots, ballot.Ballot)
		}
		*reply = ballots
		delete(req.PendingBallots, ID) // consume the message
	} else {
		// No message - return empty array
		*reply = []Ballot{}
	}
	return nil
}

// checks for vote responses
func (req *Requests) ListenVoteResponses(ID int, reply *[]VoteResponse) error {
	// Check if there's pending votes for this node
	if votes, exists := req.PendingVotes[ID]; exists {
		var responses []VoteResponse
		for _, vote := range votes {
			responses = append(responses, vote)
		}
		*reply = responses
		delete(req.PendingVotes, ID) // consume the message
	} else {
		// No message - return empty array
		*reply = []VoteResponse{}
	}
	return nil
}

func CombineTables(table1 *Membership, table2 *Membership) *Membership {
	newMembership := NewMembership()

	// Add all the values from the first table
	for _, value := range table1.Members {
		var reply Node
		newMembership.Add(value, &reply)
	}

	// Merge in the values from the second table, keeping the most up-to-date records
	for id, incomingNode := range table2.Members {
		existingNode, exists := newMembership.Members[id]

		if !exists || existingNode.ID == 0 {
			var reply Node
			newMembership.Add(incomingNode, &reply)
			continue
		}

		if incomingNode.Hbcounter > existingNode.Hbcounter {
			incomingNode.Time = float64(time.Now().UnixNano())
			var reply Node
			newMembership.Update(incomingNode, &reply)
		}

	}

	return newMembership
}

/*---------------*/

type Ballot struct {
	NodeID   int
	YesCount int
	NoCount  int
}

func NewBallot(nodeId int) *Ballot {
	return &Ballot{
		NodeID:   nodeId,
		YesCount: 0,
		NoCount:  0,
	}
}

func (ballot *Ballot) VoteYes() {
	ballot.YesCount += 1
}

func (ballot *Ballot) VoteNo() {
	ballot.NoCount += 1
}

func (ballot *Ballot) HasMajority() bool {
	return ballot.YesCount > MAX_NODES/2
}

func (ballot *Ballot) IsTie() bool {
	return ballot.YesCount == ballot.NoCount
}

func (m *Requests) SendBallot(payload Ballot, reply *bool) error {
	for id := 1; id <= MAX_NODES; id++ {
		if id != payload.NodeID {
			var reply bool
			m.AddBallot(BallotRequest{ID: id, Ballot: payload}, &reply)
		}
	}

	return nil
}

/*---------------*/

type DBObject struct {
	Key     string
	Object  string
	Context string // change later to hold vector clocks + history
}

func (req *Requests) SendPutRequest(obj DBObject, reply *bool) error {
	// todo

	return nil
}

func (req *Requests) SendGetRequest(key string, reply *DBObject) error {
	// todo

	return nil
}
