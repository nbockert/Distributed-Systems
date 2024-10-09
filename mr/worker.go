package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
// reduce: ("the",[A,B])
// should we make a worker for each call?
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var buckets []Bucket

	for {
		arg := MapArg{}
		reply := Task{}
		//put mutex
		call("Coordinator.Assign", &arg, &reply)

		//switch reply.Task
		if reply.Finish {
			break
		} else if reply.Type == "Map" {
			rep := MapReply{}
			if reply.Id == 0 {
				reply.Id = 1
			}
			arg.Id = reply.Id
			call("Coordinator.ReqMap", &arg, &rep)

			//case for first call
			fmt.Println("id:", reply.Id)
			if reply.Type == "Map" {
				buckets = make([]Bucket, reply.Numreduce)

				// if reply.Id == 1 {
				// 	buckets = make([]Bucket, reply.Numreduce)

				// 	//maybe don't make them all at once
				// 	//need to do this so that it only writes these files once i.e. take out of default case

				// }

				file, err := os.Open(rep.Title)

				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				content, err := io.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", file)
				}

				part := mapf(rep.Title, string(content))
				file.Close()
				//fmt.Println(part)

				for _, kv := range part {
					reducetask := ihash(kv.Key) % reply.Numreduce
					buckets[reducetask].Files = append(buckets[reducetask].Files, kv)
					buckets[reducetask].TaskID = reply.Id

				}
				//fmt.Println(buckets)
				fmt.Println("buckets", len(buckets))
				for i, bucket := range buckets {
					fileName := fmt.Sprintf("mr-%d-%d.json", reply.Id, i)
					fmt.Println(fileName)

					tempFile, _ := os.CreateTemp("./", fileName)

					//filed, _ := os.CreateTemp("mr-temp", fileName)
					if err != nil {
						panic(err)
					}
					//fmt.Println(tempFile.Name())
					os.Open(tempFile.Name())
					enc := json.NewEncoder(tempFile)

					//why does this not encode the files
					for _, kv := range bucket.Files {

						if err := enc.Encode(&kv); err != nil {
							log.Fatalf("Failed to encode to temp file: %v", err)
						}
						//rename once it is completly written to so that it deletes

						// if err != nil {
						// 	log.Fatalf("cannot read %v", fileName)
						// }

					}
					if err = os.Rename(tempFile.Name(), fileName); err != nil {
						log.Fatalf("cannot rename %v", err)
					}

					tempFile.Close()

				}

				l := Arg{
					Id: reply.Id,
				}
				r := Reply{}
				call("Coordinator.Completed", &l, &r)
			}
		} else if reply.Type == "Reduce" {
			redarg := Arg{
				Id: reply.Id,
			}
			redrep := Reply{}
			call("Coordinator.ReqRed", &redarg, &redrep)

			//sort.Sort(ByKey(reply.files))
			//send files
			intermediate := make(map[string][]string)
			//iterate through and decode then sort kv[Key] then steal code from mr-sequential
			var oname string

			oname = fmt.Sprintf("mr-out-%d.txt", reply.Id)
			ofile, _ := os.Create(oname)

			// Perform the search in the current directory (".")
			//filepath:="."
			// Adjust the directory as needed, e.g., "./data" if the files are in a "data" subdirectory

			for i := 1; i < reply.Nummaptask+1; i++ {
				filename := fmt.Sprintf("mr-%d-%d.json", i, reply.Id)
				fmt.Println(filename)

				file, _ := os.Open(filename)
				//the issue is something else like why is this being int
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break

					}
					intermediate[kv.Key] = append(intermediate[kv.Key], kv.Value)
					//fmt.Println(intermediate)

				}
				file.Close()

			}

			keys := []KeyValue{}
			for key, value := range intermediate {
				for k := 0; k < len(value); k++ {
					//fmt.Println(key[k])
					keys = append(keys, KeyValue{key, string(value[k])})

				}
			}

			sort.Sort(ByKey(keys))

			i := 0
			for i < len(keys) {
				j := i + 1
				for j < len(keys) && keys[j].Key == keys[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, keys[k].Value)
				}
				output := reducef(keys[i].Key, values)
				//fmt.Println(values)
				//fmt.Println("reducef: ", output)

				//fmt.Println(i)
				fmt.Fprintf(ofile, "%v %v\n", keys[i].Key, output)
				i = j

			}
			l := Arg{
				Id: reply.Id,
			}
			r := Reply{}
			call("Coordinator.CompletedRed", &l, &r)
			fmt.Println("Called the coordinator")
			fmt.Println(r.Finish)
			if r.Finish {
				break
			}
			time.Sleep(1 * time.Second)

		}
	}
}

//decode all files and then read files in partition and send key values from intermediate to
//rename files rather than write to them

// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
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

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}

func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
