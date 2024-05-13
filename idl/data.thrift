namespace go data

struct SetReq {
    1: required string key
    2: required binary value
}

struct SetResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct GetReq {
    1: required string key
}

struct GetResp {
    1: required bool success
    2: required binary value
    3: required string message
    4: required i32 status_code
}

struct DelReq {
    1: required string key
}

struct DelResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct ExpireReq {
    1: required string key
    2: required i64 time
}

struct ExpireResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct HSetReq {
    1: required string key
    2: required binary field
    3: required binary value
}

struct HSetResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct HGetReq {
    1: required string key
    2: required binary field
}

struct HGetResp {
    1: required bool success
    2: required binary value
    3: required string message
    4: required i32 status_code
}

struct HDelReq {
    1: required string key
    2: required binary field
}

struct HDelResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct LPushReq {
    1: required string key
    2: required binary element
}

struct LPushResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct RPushReq {
    1: required string key
    2: required binary element
}

struct RPushResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct LPopReq {
    1: required string key
}

struct LPopResp {
    1: required bool success
    2: required binary element
    3: required string message
    4: required i32 status_code
}

struct RPopReq {
    1: required string key
}

struct RPopResp {
    1: required bool success
    2: required binary element
    3: required string message
    4: required i32 status_code
}

struct SAddReq {
    1: required string key
    2: required binary element
}

struct SAddResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct SRemReq {
    1: required string key
    2: required binary element
}

struct SRemResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct ZAddReq {
    1: required string key
    2: required binary element
}

struct ZAddResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

struct ZRemReq {
    1: required string key
    2: required binary element
}

struct ZRemResp {
    1: required bool success
    2: required string message
    3: required i32 status_code
}

service DataService {
    GetResp Get(1: GetReq req)
    SetResp Set(1: SetReq req)
    DelResp Del(1: DelReq req)
    ExpireResp Expire(1: ExpireReq req)
    HSetResp HSet(1: HSetReq req)
    HGetResp HGet(1: HGetReq req)
    HDelResp HDel(1: HDelReq req)
    LPushResp LPush(1: LPushReq req)
    RPushResp RPush(1: RPushReq req)
    LPopResp LPop(1: LPopReq req)
    RPopResp RPop(1: RPopReq req)
    SAddResp SAdd(1: SAddReq req)
    SRemResp SRem(1: SRemReq req)
    ZAddResp ZAdd(1: ZAddReq req)
    ZRemResp ZRem(1: ZRemReq req)
}