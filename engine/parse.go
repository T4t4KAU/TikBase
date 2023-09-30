package engine

import "TikBase/pack/utils"

func parseSetStringArgs(args [][]byte) string {
	return string(args[1])
}

func parseExpireKeyArgs(args [][]byte) int64 {
	return int64(utils.BytesToInt(args[1]))
}
