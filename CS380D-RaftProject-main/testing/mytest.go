package main

import (
	"context"
	"fmt"
	"os/exec"
	"strconv"
	"sync"
	"time"

	pb "github.com/asmitalim/distpro/generatedfiles"
	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TestingStruct struct {
	mu          sync.Mutex
	kvServers   []int
	numServers  int
	portBlocked map[[2]int]bool
}

func ExecBashCommand(bash_cmd string) {
	cmd := exec.Command("bash", "-c", bash_cmd)
	err := cmd.Start()

	if err != nil {
		fmt.Printf("Failure! %v \n", err.Error())
		return
	}
}

func (ts *TestingStruct) Put(key string, value string, clientId int) (string, bool) {
	putreq := &pb.KeyValue{
		Key:       key,
		Value:     value,
		ClientId:  int64(clientId),
		RequestId: 0,
	}
	fmt.Printf("\nAttempting PUT: key=%s, value=%s, clientId=%d\n", key, value, clientId)

	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", strconv.Itoa(8001)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v \n", err)
		return "", false
	}
	defer conn.Close()
	client := pb.NewFrontEndClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	putres, err := client.Put(ctx, putreq)
	if putres == nil || err != nil {
		//fmt.Printf("could not put: %v \n", err)
		fmt.Printf("PUT failed: %v\n", err)
		return "", false
	}
	fmt.Printf("PUT success: returned value=%s\n", putres.GetValue())
	return putres.GetValue(), true
}
func (ts *TestingStruct) Get(key string, clientId int) (string, bool) {
	fmt.Printf("\nAttempting GET: key=%s, clientId=%d\n", key, clientId)
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", strconv.Itoa(8001)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v \n", err)
		return "", false
	}
	defer conn.Close()
	client := pb.NewFrontEndClient(conn)

	getreq := &pb.GetKey{
		Key:       key,
		ClientId:  int64(clientId),
		RequestId: 0,
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	getres, err := client.Get(ctx2, getreq)
	if getres == nil || err != nil {
		//fmt.Printf("could not get: %v \n", err)
		fmt.Printf("GET failed: %v\n", err)
		return "", false
	}
	fmt.Printf("GET success: returned value=%s\n", getres.GetValue())
	return getres.GetValue(), true

}

func (ts *TestingStruct) BlockPort(srcidx int, destidx int) {
	dst_port_number := strconv.Itoa(9001 + destidx)
	srcport := 7001 + ts.numServers*srcidx + destidx
	src_port_number := strconv.Itoa(srcport)
	value, exists := ts.portBlocked[[2]int{srcidx, destidx}]
	if !exists || !value {
		cmd := "sudo iptables -I INPUT -p tcp --dport " + dst_port_number + " --sport " + src_port_number + " -i lo -j DROP"
		//fmt.Printf("%v \n", cmd)
		ExecBashCommand(cmd)
		ts.portBlocked[[2]int{srcidx, destidx}] = true
	}

}

func (ts *TestingStruct) UnBlockPort(srcidx int, destidx int) {
	dst_port_number := strconv.Itoa(9001 + destidx)
	srcport := 7001 + ts.numServers*srcidx + destidx
	src_port_number := strconv.Itoa(srcport)
	value, exists := ts.portBlocked[[2]int{srcidx, destidx}]
	if exists && value {
		cmd := "sudo iptables -D INPUT -p tcp --dport " + dst_port_number + " --sport " + src_port_number + " -i lo -j DROP"
		//fmt.Printf("%v \n", cmd)
		ExecBashCommand(cmd)
		ts.portBlocked[[2]int{srcidx, destidx}] = false
	}
}

func (ts *TestingStruct) GetLastLeader() (int, bool) {
	termsToLeaders := make(map[int][]int)
	for idx, port_number := range ts.kvServers {
		conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", strconv.Itoa(port_number)), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("did not connect: %v \n", err)
			continue
		}
		defer conn.Close()

		client := pb.NewKeyValueStoreClient(conn)
		req := &pb.Empty{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		res, err := client.GetState(ctx, req)
		if err != nil {
			fmt.Printf("could not get state: %v \n", err)
			continue
		}

		term := int(res.GetTerm())
		isLeader := res.GetIsLeader()
		//fmt.Printf("Term %d IsLeader %v Idx %d PortNumber %d \n", term, isLeader, idx, port_number)
		if isLeader {
			termsToLeaders[term] = append(termsToLeaders[term], idx)
		}
	}

	lastTerm := -1
	for term, leadersForTerm := range termsToLeaders {
		if len(leadersForTerm) > 1 {
			return -1, false //we have multiple leaders for a term!
		}
		if term > lastTerm {
			lastTerm = term
		}
	}

	if len(termsToLeaders) != 0 {
		return termsToLeaders[lastTerm][0], true
	}

	return -1, true //no leader elected yet
}

func (ts *TestingStruct) GetStateOfServer(server int) (int, bool) {
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", strconv.Itoa(server+9001)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v \n", err)
		return -1, false
	}
	defer conn.Close()

	client := pb.NewKeyValueStoreClient(conn)
	req := &pb.Empty{}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.GetState(ctx, req)
	if err != nil {
		fmt.Printf("could not get state: %v \n", err)
		return -1, false
	}

	term := int(res.GetTerm())
	isLeader := res.GetIsLeader()

	return term, isLeader
}

func (ts *TestingStruct) StartRaft(numServers int) bool {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.KillAllServers()
	req := &pb.IntegerArg{
		Arg: int32(numServers),
	}
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%s", strconv.Itoa(8001)), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Printf("did not connect: %v \n", err)
		return false
	}
	defer conn.Close()

	client := pb.NewFrontEndClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := client.StartRaft(ctx, req)
	if err != nil || res.GetError() != "" {
		fmt.Printf("could not start raft: %v \n", err)
		return false
	}
	ts.kvServers = make([]int, 0)
	for i := range numServers {
		ts.kvServers = append(ts.kvServers, 9001+i)
	}
	ts.numServers = numServers
	ts.portBlocked = make(map[[2]int]bool)
	return true

}

func (ts *TestingStruct) KillAllServers() {
	var wg sync.WaitGroup

	for i := range ts.numServers {
		name := strconv.Itoa(i + 1)
		app := "pkill -9 -f raftserver"
		bash_cmd := app + name + " "
		//fmt.Println(bash_cmd)
		wg.Add(1)
		go func() {
			cmd := exec.Command("bash", "-c", bash_cmd)
			err := cmd.Start()

			if err != nil {
				fmt.Println("Failure!" + err.Error())
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func (ts *TestingStruct) KillOneNode(node int) bool {

	name := strconv.Itoa(node + 1)
	app := "pkill -9 -f raftserver"
	bash_cmd := app + name + " "
	fmt.Println(bash_cmd)

	cmd := exec.Command("bash", "-c", bash_cmd)
	err := cmd.Start()

	if err != nil {
		fmt.Println("Failure!" + err.Error())
		return false
	}

	return true

}

func (ts *TestingStruct) KillLeader() bool {
	leader, oneLeader := ts.GetLastLeader()
	if !oneLeader {
		fmt.Printf("GetLastLeader() failed! \n")
		return false
	}
	ok := ts.KillOneNode(leader)
	if !ok {
		fmt.Printf("Failed in killing leader! \n")
		return false
	}
	time.Sleep(5 * time.Second) //wait 5 secs after node failures
	newLeader, oneLeader := ts.GetLastLeader()
	if !oneLeader || newLeader == leader {
		fmt.Printf("Failed. The new leader is %d \n", newLeader)
		return false
	}
	fmt.Printf("The new leader is %d \n", newLeader)
	return true

}

func (ts *TestingStruct) TestStartRaft(numServers int) bool {
	ts.StartRaft(numServers)
	time.Sleep(5 * time.Second)
	if leader, ok := ts.GetLastLeader(); ok {
		fmt.Printf("The last leader is %d \n", leader)
		return true
	}
	return false
}

func (ts *TestingStruct) TestKillLeader() bool {
	ok := ts.KillLeader()
	return ok
}

func (ts *TestingStruct) loadDataset(thread_id int, keys []int, load_vals []int, num_threads int, wg *sync.WaitGroup) {

	defer wg.Done()
	start_idx := int((len(keys) / num_threads) * thread_id)
	end_idx := int(start_idx + (int((len(keys) / num_threads))))

	fmt.Printf("\nThread %d loading data from index %d to %d\n", thread_id, start_idx, end_idx)

	for idx := start_idx; idx < end_idx; idx++ {
		keyStr := strconv.Itoa(keys[idx])
		valStr := strconv.Itoa(load_vals[idx])
		fmt.Printf("Thread %d attempting to store: key=%s, value=%s\n", thread_id, keyStr, valStr)
		_, ok := ts.Put(keyStr, valStr, thread_id)
		if !ok {
			fmt.Printf("[Error in thread %d] put request fail, key = %d, val = %d", thread_id, keys[idx], load_vals[idx])
			return
		}
		time.Sleep(1 * time.Second)
	}
}

func (ts *TestingStruct) runWorkload(threadID int,
	keys []int, loadVals []int, runVals []int, numThreads int, numRequests int,
	putRatio int, testConsistency int, keyRangeDuplication int, perKeyLocks []sync.Mutex,
	perKeyVals []int, crashServer int, addServerFlag int, removeServer int, wg *sync.WaitGroup) {

	defer wg.Done()
	requestCount := 0
	startIdx := (len(keys) / numThreads) * threadID
	endIdx := startIdx + (len(keys) / numThreads)

	if testConsistency == 1 {
		if keyRangeDuplication == 0 {
			// Consistency test with unique keys
			for requestCount < numRequests {
				idx := rand.Intn(endIdx-startIdx) + startIdx

				// Random value for testing
				newVal := rand.Intn(1000000)

				// Put request
				_, ok := ts.Put(strconv.Itoa(keys[idx]), strconv.Itoa(newVal), threadID)
				if !ok {
					fmt.Printf("[Error in thread %d] Put request failed, key = %d, value = %d\n", threadID, keys[idx], newVal)
					//return
				}
				time.Sleep(1 * time.Second)

				// Get request
				out2, ok := ts.Get(strconv.Itoa(keys[idx]), threadID)
				if !ok {
					fmt.Printf("[Error in thread %d] Get request failed, key = %d\n", threadID, keys[idx])
					//return
				}

				// Validate the result
				if out2 != strconv.Itoa(newVal) {
					fmt.Printf("[Error in thread %d] request = (%v, %v), return = (%v, %v)\n", threadID, keys[idx], newVal, keys[idx], out2)
					//return
				}

				// Increment request count
				requestCount++
				if threadID == 0 {
					fmt.Printf("Request count = %d\n", requestCount)
				}
			}
		} else {
			// Consistency test with key range duplication
			for requestCount < numRequests {
				idx := rand.Intn(keyRangeDuplication)

				// Lock per key for concurrency
				perKeyLocks[idx].Lock()
				newVal := perKeyVals[idx] + 1
				perKeyVals[idx]++

				// Put request
				_, ok := ts.Put(strconv.Itoa(keys[idx]), strconv.Itoa(newVal), threadID)
				if !ok {
					fmt.Printf("[Error in thread %d] Put request failed, key = %d, value = %d\n", threadID, keys[idx], newVal)
					perKeyLocks[idx].Unlock()
					return
				}

				time.Sleep(1 * time.Second)
				// Get request
				out2, ok := ts.Get(strconv.Itoa(keys[idx]), threadID)
				if !ok {
					fmt.Printf("[Error in thread %d] Get request failed, key = %d\n", threadID, keys[idx])
					//return
				}

				// Validate the result
				if out2 != strconv.Itoa(newVal) {
					fmt.Printf("[Error in thread %d] request = (%v, %v), return = (%v, %v)\n", threadID, keys[idx], newVal, keys[idx], out2)
					//return
				}
				perKeyLocks[idx].Unlock()
				// Increment request count
				requestCount++
				if threadID == 0 {
					fmt.Printf("Request count = %d\n", requestCount)
				}
			}
		}
	} else {
		// Non-consistency test mode
		opType := make([]string, 100)
		for i := 0; i < 100; i++ {
			if i%100 < putRatio {
				opType[i] = "Put"
			} else {
				opType[i] = "Get"
			}
		}

		// Shuffle the operation types
		rand.Shuffle(len(opType), func(i, j int) {
			opType[i], opType[j] = opType[j], opType[i]
		})

		// Perform the operations
		for requestCount < numRequests {
			for idx := startIdx; idx < endIdx; idx++ {
				if requestCount == numRequests {
					break
				}

				if opType[idx%100] == "Put" {
					// Put request
					fmt.Printf("\n[Thread %d] Attempting PUT: key=%d, value=%d\n",
						threadID, keys[idx], runVals[idx])
					_, ok := ts.Put(strconv.Itoa(keys[idx]), strconv.Itoa(runVals[idx]), threadID)
					if !ok {
						fmt.Printf("[Error in thread %d] Put request failed, key = %d, value = %d\n", threadID, keys[idx], runVals[idx])
						//return
					}
				} else if opType[idx%100] == "Get" {
					// Get request
					out2, ok := ts.Get(strconv.Itoa(keys[idx]), threadID)
					if !ok {
						fmt.Printf("[Error in thread %d] Get request failed, key = %d\n", threadID, keys[idx])
						//return
					}

					// Validate the result
					if out2 != strconv.Itoa(loadVals[idx]) {
						fmt.Printf("[Error in thread %d] request = (%v, %v), return = (%v, %v)\n", threadID, keys[idx], loadVals[idx], keys[idx], out2)
						//return
					}
				} else {
					fmt.Printf("[Error] unknown operation type\n")
					return
				}

				// Increment request count
				requestCount++
				if threadID == 0 {
					fmt.Printf("Request count = %d\n", requestCount)
				}
			}
		}
	}
}

func (ts *TestingStruct) testKVS(numKeys int, numThreads int,

	numRequests int, putRatio int, testConsistency int, keyRangeDuplication int,
	crashServer int, addServer int, removeServer int) {

	keys := make([]int, numKeys)
	loadVals := make([]int, numKeys)
	runVals := make([]int, numKeys)

	// Initialize keys and values
	for i := 0; i < numKeys; i++ {
		keys[i] = i
		loadVals[i] = i
		runVals[i] = numKeys + i
	}

	// Shuffle the keys and values
	rand.Shuffle(numKeys, func(i, j int) {
		keys[i], keys[j] = keys[j], keys[i]
	})
	rand.Shuffle(numKeys, func(i, j int) {
		loadVals[i], loadVals[j] = loadVals[j], loadVals[i]
	})
	rand.Shuffle(numKeys, func(i, j int) {
		runVals[i], runVals[j] = runVals[j], runVals[i]
	})

	var perKeyLocks []sync.Mutex
	var perKeyVals []int
	if keyRangeDuplication != 0 {
		for i := 0; i < numKeys; i++ {
			perKeyLocks = append(perKeyLocks, sync.Mutex{})
			perKeyVals = append(perKeyVals, 0)
		}
	}

	var wg sync.WaitGroup
	// Loading phase
	start := time.Now()
	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go ts.loadDataset(threadID, keys, loadVals, numThreads, &wg)
	}
	wg.Wait()
	end := time.Now()
	fmt.Printf("Load throughput = %.1f ops/sec\n", float64(numKeys)/(end.Sub(start).Seconds()))

	// Running phase
	start = time.Now()
	for threadID := 0; threadID < numThreads; threadID++ {
		wg.Add(1)
		go ts.runWorkload(threadID, keys, loadVals, runVals, numThreads, numRequests/numThreads, putRatio, testConsistency, keyRangeDuplication, perKeyLocks, perKeyVals, crashServer, addServer, removeServer, &wg)
	}
	wg.Wait()
	end = time.Now()
	fmt.Printf("Run throughput = %.1f ops/sec\n", float64(numRequests)/(end.Sub(start).Seconds()))
}

func (ts *TestingStruct) TestOperations() {
	numKeys := 10
	numThreads := 10
	numRequests := 10
	putRatio := 25
	testConsistency := 0
	keyRangeDuplication := 0
	crashServer := 0
	addServer := 0
	removeServer := 0
	ts.testKVS(numKeys, numThreads, numRequests, putRatio, testConsistency, keyRangeDuplication, crashServer, addServer, removeServer)
}

func (ts *TestingStruct) TestLinearizability() {
	numKeys := 100
	numThreads := 10
	numRequests := 100
	putRatio := 25
	testConsistency := 1
	keyRangeDuplication := 0
	crashServer := 0
	addServer := 0
	removeServer := 0
	ts.testKVS(numKeys, numThreads, numRequests, putRatio, testConsistency, keyRangeDuplication, crashServer, addServer, removeServer)
}

func (ts *TestingStruct) TestLinearizabilityWithKeyRangeDuplication() {
	numKeys := 100
	numThreads := 10
	numRequests := 100
	putRatio := 25
	testConsistency := 1
	keyRangeDuplication := 1
	crashServer := 0
	addServer := 0
	removeServer := 0
	ts.testKVS(numKeys, numThreads, numRequests, putRatio, testConsistency, keyRangeDuplication, crashServer, addServer, removeServer)
}

func (ts *TestingStruct) DisconnectLeader() bool {
	success := true
	leader, ok := ts.GetLastLeader()
	if !ok {
		fmt.Println("DisconnectLeader: Failed to get leader")
		success = false
	}
	fmt.Printf("Leader before killing is %v \n", leader)

	// Kill the leader
	ok = ts.KillOneNode(leader)
	if !ok {
		fmt.Printf("Failed to kill leader %d\n", leader)
		success = false
	}

	time.Sleep(1 * time.Second)
	ts.Put("1", "6558", 1) //main thread
	time.Sleep(1 * time.Second)

	newleader, ok := ts.GetLastLeader()
	if !ok {
		fmt.Println("DisconnectLeader: Failed to get new leader")
		success = false
	}

	fmt.Printf("Leader after killing is %v \n", newleader)

	val, ok := ts.Get("1", 1)
	if !ok {
		fmt.Println("DisconnectLeader: Failed to perform get operation")
		success = false
	}
	if val != "6558" {
		fmt.Printf("Incorrect value received from leader!")
		success = false
	}

	// Always start one node (since we killed one)
	if !success {
		ts.StartRaft(1)
	}
	return success
}

func (ts *TestingStruct) DisconnectMinority() bool {
	success := true
	nodes := ts.numServers
	nodesToKill := 0  //we want to kill a minority
	if nodes%2 == 1 { //odd
		nodesToKill = nodes / 2
	} else { //even
		nodesToKill = (nodes / 2) - 1
	}

	// Kill minority of servers
	for serverToKill := 0; serverToKill < nodesToKill; serverToKill++ {
		ok := ts.KillOneNode(serverToKill)
		if !ok {
			fmt.Printf("Failed to kill server %d\n", serverToKill)
			success = false
		}
	}

	time.Sleep(1 * time.Second)
	ts.Put("2", "6445", 1) //main thread
	time.Sleep(1 * time.Second)

	val, ok := ts.Get("2", 1)
	if !ok {
		fmt.Println("DisconnectMinority: Failed to perform Get operation")
		success = false
	}
	if val != "6445" {
		fmt.Printf("Incorrect value received from leader!")
		success = false
	}

	// Always start the same number of nodes we killed
	if !success {
		ts.StartRaft(nodesToKill)
	}
	return success
}

func (ts *TestingStruct) DisconnectMajority() bool {
	success := true
	nodes := ts.numServers
	nodesToKill := 0  // we want to kill a majority
	if nodes%2 == 1 { // odd
		nodesToKill = nodes/2 + 1
	} else { // even
		nodesToKill = nodes / 2
	}

	// Kill majority of servers
	for serverToKill := 0; serverToKill < nodesToKill; serverToKill++ {
		ok := ts.KillOneNode(serverToKill)
		if !ok {
			fmt.Printf("Failed to kill server %d\n", serverToKill)
			success = false
		}
	}

	time.Sleep(1 * time.Second)
	_, ok := ts.Put("1", "2034", 1) // main thread
	time.Sleep(1 * time.Second)

	if ok {
		fmt.Println("We made progress despite having a minority!")
		// Progress shouldn't happen with majority down
		success = false
	}

	// Always start the same number of nodes we killed
	if !success {
		ts.StartRaft(nodesToKill)
	}
	// time.Sleep(5 * time.Second)
	return success
}

func main() {
	ts := TestingStruct{}
	defer ts.KillAllServers()
	ok := ts.TestStartRaft(5)
	if !ok {
		fmt.Printf("Error while starting RAFT. \n")
	}
	_, ok = ts.GetLastLeader()
	if !ok {
		fmt.Printf("Error while getting leader. \n")
	}

	ts.TestLinearizability()

	ok = ts.DisconnectLeader()
	if !ok {
		fmt.Printf("Error while disconnecting leader. \n")
	}

	ok = ts.DisconnectMinority()
	if !ok {
		fmt.Printf("Error while disconnecting minority: Progress is still expected to be made. \n")
	}

	ok = ts.DisconnectMajority()
	if !ok {
		fmt.Printf("Error while disconnecting majority: No progress should be made. \n")
	}
	//ts.TestLinearizabilityWithKeyRangeDuplication()
	//ts.TestOperations()

	/*ok = ts.TestKillLeader()
	if !ok {
		fmt.Printf("Error while testing kill leader. \n")
	}
	//ts.KillOneNode(2) //random node killed
	time.Sleep(1 * time.Second)
	ts.TestLinearizability()*/
}
