namespace go replica

struct JoinReq {
    1: required string raft_addr
    2: required string service_addr
    3: required string node_id
}

struct JoinResp {
    1: required string message
}

struct GetIdReq {}

struct GetIdResp {
    1: required string node_id
}

struct LeaderAddrReq {}

struct LeaderAddrResp {
    1: required string address
}

service ReplicaService {
    JoinResp Join(1: JoinReq req)
    LeaderAddrResp LeaderAddr(1: LeaderAddrReq req)
    GetIdResp GetId(1: GetIdReq req)
}