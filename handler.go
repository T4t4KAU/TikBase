package main

import (
	"context"
	replica "github.com/T4t4KAU/TikBase/pkg/rpc/replica"
)

// ReplicaServiceImpl implements the last service interface defined in the IDL.
type ReplicaServiceImpl struct{}

// Join implements the ReplicaServiceImpl interface.
func (s *ReplicaServiceImpl) Join(ctx context.Context, req *replica.JoinReq) (resp *replica.JoinResp, err error) {
	// TODO: Your code here...
	return
}

// LeaderAddr implements the ReplicaServiceImpl interface.
func (s *ReplicaServiceImpl) LeaderAddr(ctx context.Context, req *replica.LeaderAddrReq) (resp *replica.LeaderAddrResp, err error) {
	// TODO: Your code here...
	return
}
