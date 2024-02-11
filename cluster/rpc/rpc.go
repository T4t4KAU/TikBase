package rpc

import (
	"github.com/T4t4KAU/TikBase/cluster/raft"
	"net/rpc"
)

func AddNode(nodeId, joinAddr, raftAddr string) (bool, error) {
	cli, err := rpc.Dial("tcp", joinAddr)
	if err != nil {
		return false, err
	}

	args := raft.Args{
		NodeId:   nodeId,
		JoinAddr: joinAddr,
		RaftAddr: raftAddr,
	}

	reply := &raft.Reply{}
	err = cli.Call("RaftService.AddNode", args, reply)
	return reply.Success, err
}
