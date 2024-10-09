package mr

//is there a way to call the worker function from this side? LIke what are the parameters and can you use the call function?

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	Id         int
	Type       string
	Numreduce  int
	Nummaptask int
	Finish     bool
}
type Coordinator struct {
	// Your definitions here.
	Total       int
	Numreduce   int
	Files       []string
	Task        string
	Pending     []*Task
	Inprogress  []*Task
	Complete    []*Task
	RPending    []*Task
	RInprogress []*Task
	RComplete   []*Task
	Boolval     IsDone
	Alldone     IsDone
	Mu          sync.Mutex
}

func (c *Coordinator) Completed(l *Arg, r *Reply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	log.Println("Entering completed loop")
	for i := 0; i < len(c.Inprogress); i++ {
		if c.Inprogress[i].Id == l.Id {
			fmt.Println(i)
			c.Inprogress = append(c.Inprogress[:i], c.Inprogress[i+1:]...)
			task := Task{
				Id:         l.Id,
				Type:       "Map",
				Numreduce:  c.Numreduce,
				Nummaptask: len(c.Files),
			}
			c.Complete = append(c.Complete, &task)

			//c.RInprogress = append(c.RInprogress[:i], c.RInprogress[i-1]

			break
		}
	}
	log.Println("num complete", len(c.Complete))

	if len(c.Complete) == len(c.Files) {
		r.Type = "Reduce"
		c.Boolval = IsDone{
			Finished: true,
		}

	} else {
		r.Type = "Map"

	}

	return nil

}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Assign(arg *MapArg, reply *Task) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	//defer never happens if you don't return
	//task and just give it
	if c.Alldone.Finished {
		reply.Finish = true

		return nil

	} else if c.Boolval.Finished {

		//fmt.Println("In the reduce")
		if len(c.RPending) > 0 {
			task := c.RPending[0]
			//log.Println("task:", task)
			c.RPending = c.RPending[1:]
			*reply = *task
			//log.Println("is the line above causing errors")
			//fmt.Println("RPending:", c.RPending)

			c.RInprogress = append(c.RInprogress, task)
			reply.Finish = false
			fmt.Println(c.RInprogress)

		}
		return nil

	} else {
		if len(c.Pending) > 0 {
			task := c.Pending[0]
			c.Pending = c.Pending[1:]
			*reply = *task
			c.Inprogress = append(c.Inprogress, task)

			reply.Finish = false
			fmt.Println(len(c.Complete))

		}
		return nil

	}

}

func (c *Coordinator) ReqMap(arg *MapArg, rep *MapReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	rep.Title = c.Files[arg.Id-1]
	rep.Id = arg.Id
	rep.Nummaptask = len(c.Files)

	go handle_reply(arg, rep, c)

	if c.Boolval.Finished {
		rep.Tasktype = "Reduce"
	}
	return nil
}

func handle_reply(arg *MapArg, reply *MapReply, c *Coordinator) {
	fmt.Println("in the ticker")
	ticker := time.NewTicker(10 * time.Second)
	//defer ticker.Stop() // Ensure the ticker is stopped when done
	//make an infinate for loop

	<-ticker.C
	//c.Boolval = true
	c.Mu.Lock()
	defer c.Mu.Unlock()
	log.Println("Entering the ticker loop")
	for _, j := range c.Complete {
		if j.Id != reply.Id {
			for i := 0; i < len(c.Inprogress); i++ {
				if j.Id != c.Inprogress[i].Id {
					if c.Inprogress[i].Id == reply.Id {
						task := Task{
							Id:         reply.Id,
							Type:       "Map",
							Numreduce:  c.Numreduce,
							Nummaptask: len(c.Files),
						}
						c.Inprogress = append(c.Inprogress[:i], c.Inprogress[i+1:]...)
						c.Pending = append(c.Pending, &task)

						fmt.Println("in go routine")

						break
					}
				}
			}
		}
	}

	//do something with queue

}

func (c *Coordinator) ReqRed(arg *Arg, rep *Reply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	rep.Id = arg.Id

	go handle_replyred(arg, rep, c)

	return nil
}

func handle_replyred(arg *Arg, reply *Reply, c *Coordinator) {
	fmt.Println("in the ticker for reduce")
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop() // Ensure the ticker is stopped when done
	//make an infinate for loop
	select {
	case <-ticker.C:
		//c.Boolval = true
		//fmt.Println(c.RInprogress)
		//fmt.Println("pls reach for loop")
		c.Mu.Lock()
		defer c.Mu.Unlock()
		fmt.Println(c.RInprogress)
		for _, j := range c.RComplete {
			if j.Id != reply.Id {
				for i := 0; i < len(c.RInprogress); i++ {
					if j.Id != c.RInprogress[i].Id {
						if c.RInprogress[i].Id == reply.Id {
							c.RInprogress = append(c.RInprogress[:i], c.RInprogress[i+1:]...)
							task := Task{
								Id:         reply.Id,
								Type:       "Reduce",
								Numreduce:  c.Numreduce,
								Nummaptask: len(c.Files),
							}
							c.RPending = append(c.RPending, &task)

							fmt.Println("in go routine")

							break
						}
					}
				}
			}
		}

		//do something with queue
	}

}

func (c *Coordinator) CompletedRed(l *Arg, r *Reply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	for i := 0; i < len(c.RInprogress); i++ {
		if c.RInprogress[i].Id == l.Id {
			c.RInprogress = append(c.RInprogress[:i], c.RInprogress[i+1:]...)
			task := Task{
				Id:         l.Id,
				Type:       "Reduce",
				Numreduce:  c.Numreduce,
				Nummaptask: len(c.Files),
			}
			c.RComplete = append(c.RComplete, &task)
			break
		}
	}

	log.Println("num complete", len(c.RComplete))

	if len(c.RComplete) == c.Numreduce {
		fmt.Println("hitting the done case")
		r.Type = "Quit"
		c.Alldone = IsDone{
			Finished: true,
		}
		r.Finish = true
		return nil

	} else {
		r.Type = "Reduce"
		r.Finish = false

	}

	return nil

}

// set timeout for worker call once assigned to new worker set new timer
// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	ret := false
	if c.Alldone.Finished {
		ret = true
	}
	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	fmt.Println("files:", len(files))
	var mutex sync.Mutex
	log.Println("Made it here")
	q := make([]*Task, 0)
	r := make([]*Task, 0)
	p := make([]*Task, 0)
	a := make([]*Task, 0)
	b := make([]*Task, 0)
	g := make([]*Task, 0)

	gar := IsDone{
		Finished: false,
	}
	bar := IsDone{
		Finished: false,
	}

	c := Coordinator{
		Total:       0,
		Numreduce:   nReduce,
		Files:       files,
		Task:        "Map",
		Pending:     q,
		Inprogress:  p,
		Complete:    r,
		RPending:    a,
		RInprogress: b,
		RComplete:   g,
		Boolval:     gar,
		Alldone:     bar,
		Mu:          mutex,
	}
	// task := Task{
	// 	Type: "head",
	// }
	// c.RComplete = append(c.Complete, &task)

	for i := 1; i <= len(c.Files); i++ {
		repp := Task{
			Id:         i,
			Type:       "Map",
			Numreduce:  c.Numreduce,
			Nummaptask: len(c.Files),
		}
		c.Pending = append(c.Pending, &repp)
	}
	for i := 0; i < nReduce; i++ {
		nrepp := Task{
			Id:         i,
			Type:       "Reduce",
			Numreduce:  c.Numreduce,
			Nummaptask: len(c.Files),
		}
		c.RPending = append(c.RPending, &nrepp)
	}
	//fmt.Println("RPending:", c.RPending)

	// Your code here.

	c.server()
	return &c
}

// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//done channel: recieve replies, check if you are done doing reduces and if you recieve something from the channel youre done
