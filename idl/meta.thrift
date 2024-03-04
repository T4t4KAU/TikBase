namespace go meta

struct RegionListReq {}

struct RegionListResp {
    1: required string message
}

struct RegionStatusReq {
    1: required string name // 分区名称
}

struct RegionStatusResp {
    1: required string name
    2: required string address
    3: required i64 replica_count
    4: required string message
}

struct ReplicaListReq {}

struct ReplicaListResp {
    1: required string message
}

struct ReplicaStatusReq {
    1: required string name
}

struct ReplicaStatusResp {
    1: required string name
    2: required string address
    3: required string message
}

service MetaService {
    RegionListResp RegionList(1: RegionListReq req)
    RegionStatusResp RegionStatus(1: RegionStatusReq req)
    ReplicaListResp ReplicaList(1: ReplicaListReq req)
    ReplicaStatusResp ReplicaStatus(1: ReplicaStatusReq req)
}