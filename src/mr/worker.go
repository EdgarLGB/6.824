package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for true {
		input, success := getTask()
		if input.Paths == nil {
			time.Sleep(1 * time.Second)
			continue	// The task is empty, try to get another one
		}
		if !success {
			return
		}
		run(input, mapf, reducef)
	}
}

func run(input TaskInput, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if input.NReduce > 0 {	// It is a map task
		// Read map input and generate intermediate
		intermediate := loadFilesAndMap(mapf, input.Paths)

		// TODO: reducef is not correct, should accumulate the occurence, not the length of array
		//intermediate = doReduce(intermediate, reducef)

		// Put intermediate into buckets
		intermediateByBucket := make(map[int][]KeyValue)
		for _, kv := range intermediate {
			which := ihash(kv.Key) % input.NReduce
			if intermediateByBucket[which] == nil {
				intermediateByBucket[which] = []KeyValue{kv}
			} else {
				intermediateByBucket[which] = append(intermediateByBucket[which], kv)
			}
		}

		// Write bucket files
		directory, _ := ioutil.TempDir("", "intermediate")
		for i := 0; i < input.NReduce; i++ {
			oname := fmt.Sprintf("%v/bucket-%v",directory, i)
			file, err := os.Create(oname)
			check(err)
			for _, inter := range intermediateByBucket[i] {
				_, err := fmt.Fprintf(file, "%v %v\n", inter.Key, inter.Value)
				check(err)
			}
			err2 := file.Close()
			check(err2)
		}

		// Return map task output
		output := TaskOutput{Id: input.Id, Directory: directory}
		finishTask(output)

	} else {
		// Find all the bucketPaths which should be read by this reducer
		var bucketPaths []string
		for _, directory := range input.Paths {
			bucketPath := fmt.Sprintf("%v/bucket-%v", directory, input.Id)
			if _, err := os.Stat(bucketPath); err == nil {
				bucketPaths = append(bucketPaths, bucketPath)
			}
		}

		// Read the intermediate
		intermediate := loadIntermediate(bucketPaths)

		// Create output file
		oname := fmt.Sprintf("mr-out-%v", input.Id)
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-X.
		//
		intermediate = doReduce(intermediate, reducef)

		for _, kv := range intermediate {
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}

		ofile.Close()

		output := TaskOutput{Id: input.Id}
		finishTask(output)
	}
}

func doReduce(intermediate []KeyValue, reducef func(string, []string) string) []KeyValue {
	sort.Sort(ByKey(intermediate))

	var res []KeyValue
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		res = append(res, KeyValue{intermediate[i].Key, output})

		i = j
	}

	return res
}

func loadIntermediate(inputFiles []string) []KeyValue {
	var intermediate []KeyValue
	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		check(err)

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			values := strings.Split(scanner.Text(), " ")
			intermediate = append(intermediate, KeyValue{values[0], values[1]})
		}

		file.Close()
	}
	return intermediate
}

func loadFilesAndMap(mapf func(string, string) []KeyValue, inputFiles []string) []KeyValue {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	var intermediate []KeyValue
	for _, filename := range inputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	return intermediate
}

func getTask() (TaskInput, bool) {
	reply := TaskInput{}
	args := ExampleArgs{}
	if call("Coordinator.GetTask", &args, &reply) {
		return reply, true
	}
	return reply, false
}

func finishTask(output TaskOutput) bool {
	reply := ExampleReply{}
	return call("Coordinator.FinishTask", &output, &reply)
}

func check(e error) {
    if e != nil {
        panic(e)
    }
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
