package shared

import (
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	MAX_NODES      = 8
	ROLE_FOLLOWER  = "FOLLOWER"
	ROLE_CANDIDATE = "CANDIDATE"
	ROLE_LEADER    = "LEADER"
	VOTE_TIME      = 8
	N              = 3 // how many nodes to replicate data on
)

// Node struct represents a computing node.
type Node struct {
	ID        int
	Hbcounter int
	Time      float64
	Alive     bool
	Store     Store
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

func (m *Membership) SortedNodes() []Node {
	nodes := make([]Node, 0, len(m.Members))

	for _, node := range m.Members {
		nodes = append(nodes, node)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return nodes
}

/*---------------*/

// MembershipRequest struct represents a new message request to a client
type MembershipRequest struct {
	ID    int
	Table Membership
}

// Requests struct represents pending message requests
type Requests struct {
	mu                       sync.Mutex
	PendingMemberships       map[int][]MembershipRequest
	PendingCoordGetRequest   map[int]string
	PendingReplicaGetRequest map[int]string
	PendingCoordPutRequest   map[int]PutRequest
	PendingReplicaPutRequest map[int]PutRequest
	ReplicaPutResponses      []int
	ReplicaGetResponses      []GetResponse
	GetResults               []ObjectVersion
	PutResult                Context
	Ring                     []RingEntry
	PendingDataTransfers     map[int][]DataTransfer
}

// Returns a new instance of a Requests (pointer).
func NewRequests() *Requests {
	var Ring []RingEntry

	for id := 1; id <= MAX_NODES; id++ {
		Ring = append(Ring, RingEntry{
			Hash:   NodePosition(id),
			NodeID: id,
		})
	}

	sort.Slice(Ring, func(i, j int) bool {
		return Ring[i].Hash < Ring[j].Hash
	})

	return &Requests{
		PendingMemberships:       make(map[int][]MembershipRequest),
		PendingCoordGetRequest:   make(map[int]string),
		PendingReplicaGetRequest: make(map[int]string),
		PendingCoordPutRequest:   make(map[int]PutRequest),
		PendingReplicaPutRequest: make(map[int]PutRequest),
		ReplicaPutResponses:      []int{},
		ReplicaGetResponses:      []GetResponse{},
		GetResults:               []ObjectVersion{},
		Ring:                     Ring,
		PendingDataTransfers:     make(map[int][]DataTransfer),
	}
}

func (req *Requests) AddMembership(payload MembershipRequest, reply *bool) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	req.PendingMemberships[payload.ID] = append(req.PendingMemberships[payload.ID], payload)
	*reply = true
	return nil
}

// Listens to communication from neighboring nodes.
func (req *Requests) ListenMemberships(ID int, reply *[]MembershipRequest) error {
	req.mu.Lock()
	defer req.mu.Unlock()

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
			// this node isn't in the current table
			// if the incoming node is marked as dead, assume we've already deleted it
			if incomingNode.Alive {
				var reply Node
				newMembership.Add(incomingNode, &reply)
				continue
			}
		}

		if incomingNode.Hbcounter > existingNode.Hbcounter && incomingNode.Alive {
			incomingNode.Time = float64(time.Now().UnixNano())
			var reply Node
			newMembership.Update(incomingNode, &reply)
		}

	}

	return newMembership
}

/*---------------*/

type PutRequest struct {
	CoordID  int
	TargetID int
	SubID    int
	Key      string
	Object   string
	Context  Context
}

type DataTransfer struct {
	// the node that is transferring the data
	FromNodeID int
	// the revived node that is receiveing the data
	TargetID int
	Data     map[string][]ObjectVersion
}

type GetResponse struct {
	FromNodeID int
	Key        string
	Versions   []ObjectVersion
}

type ObjectVersion struct {
	Object  string
	Context Context
}

type Context struct {
	VectorClock map[int]int
}

func (c Context) String() string {
	var parts []string

	ids := make([]int, 0, len(c.VectorClock))
	for id := range c.VectorClock {
		ids = append(ids, id)
	}
	sort.Ints(ids)

	for _, id := range ids {
		parts = append(parts,
			fmt.Sprintf("%d:%d", id, c.VectorClock[id]))
	}

	return "{" + strings.Join(parts, ", ") + "}"
}

type Store struct {
	Data  map[string][]ObjectVersion
	Hints map[int]map[string][]ObjectVersion
}

func NewStore() *Store {
	return &Store{
		Data:  make(map[string][]ObjectVersion),
		Hints: make(map[int]map[string][]ObjectVersion),
	}
}

type ClockRelation int

const (
	Older ClockRelation = iota
	Newer
	Concurrent
	Equal
)

func CompareContexts(a, b Context) ClockRelation {
	aGreater := false
	bGreater := false

	// Check entries in a
	for nodeID, aCount := range a.VectorClock {
		bCount := b.VectorClock[nodeID]

		if aCount > bCount {
			aGreater = true
		} else if aCount < bCount {
			bGreater = true
		}
	}

	// Check entries that exist only in b
	for nodeID, bCount := range b.VectorClock {
		if _, exists := a.VectorClock[nodeID]; !exists {
			if bCount > 0 {
				bGreater = true
			}
		}
	}

	if aGreater && bGreater {
		return Concurrent
	}
	if aGreater {
		return Newer
	}
	if bGreater {
		return Older
	}
	return Equal
}

func (s *Store) Put(key string, incoming ObjectVersion) {
	existing := s.Data[key]

	// no versions stored yet
	if len(existing) == 0 {
		s.Data[key] = []ObjectVersion{incoming}
		return
	}

	newVersions := []ObjectVersion{}

	for _, existingVersion := range existing {
		relation := CompareContexts(incoming.Context, existingVersion.Context)

		switch relation {
		case Older:
			// Incoming version is stale; ignore it entirely
			return

		case Equal:
			// Same version already exists; ignore duplicate
			return

		case Newer:
			// Incoming supersedes this existing version, so don't keep old one
			continue

		case Concurrent:
			// Keep existing sibling
			newVersions = append(newVersions, existingVersion)
		}
	}

	// Add incoming version after removing versions it superseded.
	newVersions = append(newVersions, incoming)
	s.Data[key] = newVersions
}

func (s *Store) AddHint(intendedNodeID int, key string, version ObjectVersion) {
	if s.Hints[intendedNodeID] == nil {
		s.Hints[intendedNodeID] =
			make(map[string][]ObjectVersion)
	}

	s.Hints[intendedNodeID][key] = append(s.Hints[intendedNodeID][key], version)
}

func (s *Store) GetHintedData(intendedNodeID int) map[string][]ObjectVersion {
	if hint, exists := s.Hints[intendedNodeID]; exists {
		return hint
	}

	return make(map[string][]ObjectVersion)
}

func IncrementContext(ctx Context, nodeID int) Context {
	newClock := make(map[int]int)

	// copy all the existing values
	maps.Copy(newClock, ctx.VectorClock)

	// increment the coordinator node's counter
	newClock[nodeID]++

	return Context{VectorClock: newClock}
}

func (req *Requests) SendPutRequest(putReq PutRequest, reply *bool) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	// first clear out any old responses from the last put request
	req.ReplicaPutResponses = []int{}

	if putReq.SubID != -1 {
		// the request is being sent to a substitute node (hinted handoff)
		req.PendingReplicaPutRequest[putReq.SubID] = putReq
		return nil
	}

	coordinator_node := req.FindCoordinator(HashString(putReq.Key))
	coord_id := coordinator_node.NodeID

	// attach the coordinator's ID to the request
	putReq.CoordID = coord_id
	req.PendingCoordPutRequest[coord_id] = putReq

	return nil
}

func (req *Requests) SendDataToRevivedNode(data DataTransfer, reply *bool) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	req.PendingDataTransfers[data.TargetID] = append(req.PendingDataTransfers[data.TargetID], data)

	return nil
}

func (req *Requests) CheckForDataTransfers(id int, reply *[]DataTransfer) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	*reply = req.PendingDataTransfers[id]

	// consume the request
	delete(req.PendingDataTransfers, id)

	return nil
}

func (req *Requests) ListenDataTransferResult(sentDt DataTransfer, reply *bool) error {
	fromNodeId := sentDt.FromNodeID
	targetId := sentDt.TargetID

	dts := req.PendingDataTransfers[targetId]

	for _, dt := range dts {
		if dt.FromNodeID == fromNodeId {
			// the transfer is still pending
			*reply = false
			return nil
		}
	}

	*reply = true
	return nil
}

func (req *Requests) ListenCoordPutRequest(coord_id int, reply *PutRequest) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	putReq := req.PendingCoordPutRequest[coord_id]

	// there are no pending requests
	if putReq.Key == "" {
		return nil
	}

	new_context := IncrementContext(putReq.Context, coord_id)
	// fmt.Printf("Old context: %v\nNew context: %v\n", putReq.Context, new_context)

	putReq.Context = new_context

	// send it to the replicas
	for i := 1; i <= N; i++ {
		id := ((coord_id + i - 1) % MAX_NODES) + 1 // wrap around
		req.PendingReplicaPutRequest[id] = putReq
	}

	*reply = putReq

	// consume the request
	delete(req.PendingCoordPutRequest, coord_id)

	return nil
}

func (req *Requests) ListenReplicaPutRequest(replica_id int, reply *PutRequest) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	putReq := req.PendingReplicaPutRequest[replica_id]

	*reply = putReq

	// consume the request
	delete(req.PendingReplicaPutRequest, replica_id)

	return nil
}

func (req *Requests) RespondToPutRequest(replica_id int, reply *bool) error {
	req.ReplicaPutResponses = append(req.ReplicaPutResponses, replica_id)

	*reply = true

	return nil
}

func (req *Requests) ListenReplicaPutResponses(coord_id int, reply *[]int) error {
	responses := req.ReplicaPutResponses

	*reply = responses

	// consume the response
	req.ReplicaPutResponses = []int{}

	return nil
}

func (req *Requests) SendPutResultToClient(result Context, reply *bool) error {
	req.PutResult = result

	status := "FAILED"
	if len(result.VectorClock) > 0 {
		status = "SUCCEEDED"
	}
	fmt.Printf("Sending result to client: PUT %v\n", status)

	*reply = true

	return nil
}

func (req *Requests) SendGetRequest(key string, reply *bool) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	// first clear out any leftover results from the last get request
	req.ReplicaGetResponses = []GetResponse{}
	req.GetResults = []ObjectVersion{}

	coordinator_node := req.FindCoordinator(HashString(key))
	coord_id := coordinator_node.NodeID

	req.PendingCoordGetRequest[coord_id] = key

	*reply = true

	return nil
}

func (req *Requests) RespondToGetRequest(resp GetResponse, reply *bool) error {
	req.ReplicaGetResponses = append(req.ReplicaGetResponses, resp)

	*reply = true

	return nil
}

func (req *Requests) SendGetResultsToClient(results []ObjectVersion, reply *bool) error {
	req.GetResults = results

	fmt.Printf("Sending results to client: %v\n", results)

	*reply = true

	return nil
}

func (req *Requests) ListenGetResults(key string, reply *[]ObjectVersion) error {
	results := req.GetResults

	*reply = results

	// consume the results
	req.GetResults = []ObjectVersion{}

	return nil
}

func (req *Requests) ListenPutResult(key string, reply *Context) error {
	result := req.PutResult

	*reply = result

	// consume the result
	req.PutResult = Context{}

	return nil
}

func (req *Requests) ListenCoordGetRequest(coord_id int, reply *string) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	getReq := req.PendingCoordGetRequest[coord_id]

	// there are no pending requests
	if getReq == "" {
		return nil
	}

	// send it to the replicas
	for i := 1; i <= N; i++ {
		id := ((coord_id + i - 1) % MAX_NODES) + 1 // wrap around
		req.PendingReplicaGetRequest[id] = getReq
	}

	*reply = getReq

	// consume the request
	delete(req.PendingCoordGetRequest, coord_id)

	return nil
}

func (req *Requests) ListenReplicaGetRequest(replica_id int, reply *string) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	getReq := req.PendingReplicaGetRequest[replica_id]

	*reply = getReq

	// consume the request
	delete(req.PendingReplicaGetRequest, replica_id)

	return nil
}

func (req *Requests) ListenReplicaGetResponses(coord_id int, reply *[]GetResponse) error {
	responses := req.ReplicaGetResponses

	*reply = responses

	// consume the response
	req.ReplicaGetResponses = []GetResponse{}

	return nil
}

func (req *Requests) ClearStaleRequests(nodeId int, reply *bool) error {
	req.mu.Lock()
	defer req.mu.Unlock()

	delete(req.PendingMemberships, nodeId)
	delete(req.PendingReplicaGetRequest, nodeId)
	delete(req.PendingReplicaPutRequest, nodeId)
	delete(req.PendingCoordGetRequest, nodeId)
	delete(req.PendingCoordPutRequest, nodeId)

	return nil
}

type RingEntry struct {
	Hash   uint64
	NodeID int
}

func HashString(s string) uint64 {
	hash := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint64(hash[:8])
}

func NodePosition(id int) uint64 {
	return HashString(fmt.Sprintf("node-%d", id))
}

func (req *Requests) FindCoordinator(hash uint64) RingEntry {
	for _, node := range req.Ring {
		if hash <= node.Hash {
			return node
		}
	}

	return req.Ring[0] // wrap around
}

func GetPreferenceList(id int) []int {
	replicaIds := []int{}

	for i := 1; i <= N; i++ {
		id := ((id + i - 1) % MAX_NODES) + 1 // wrap around
		replicaIds = append(replicaIds, id)
	}

	return replicaIds
}

func PrintVersions(versions []ObjectVersion) {
	for i, version := range versions {
		fmt.Printf(
			"[%d] value=%q clock=%s\n",
			i+1,
			version.Object,
			version.Context,
		)
	}
}
