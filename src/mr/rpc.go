package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//
const (
	Map        = "Map"
	Reduce     = "Reduce"
	WaitForMap = "WaitForMap"
)

type RequestJobArgs struct {
}
type RequestJobReply struct {
	Phase     string
	I         int
	NumMap    int
	NumReduce int
	FileName  string

	Done bool
}

type SendJobStatusArgs struct {
	I string
}

type SendJobStatusReply struct {
}

// Add your RPC definitions here.
