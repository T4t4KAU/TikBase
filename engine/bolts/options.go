package bolts

type Options struct {
	DirPath  string
	MaxLevel int

	SSTSize          uint64
	SSTNumPerLevel   int
	SSTDataBlockSize int
	SSTFooterSize    int
}
