namespace go consis

struct JoinReq {
    1: required string raft_addr
    2: required string service_addr
    3: required string node_id
}

struct JoinResp {
    1: required string message
}

service ConsisService {
    JoinResp Join(1: JoinReq req)
}