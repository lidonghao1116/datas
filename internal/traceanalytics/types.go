package traceanalytics

import (
	"context"
	"encoding/json"
	"time"
)

const Version = "call-v1"

type Candidate struct {
	ChainID          uint64
	BlockNumber      uint64
	BlockHash        string
	BlockTime        time.Time
	TransactionHash  string
	TransactionIndex uint32
	WalletAddress    string
	TargetAddress    string
	Input            string
	PoolAddresses    []string
	AttemptCount     uint32
}

type CallLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type CallFrame struct {
	Type         string      `json:"type"`
	From         string      `json:"from"`
	To           string      `json:"to"`
	Value        string      `json:"value"`
	Gas          string      `json:"gas"`
	GasUsed      string      `json:"gasUsed"`
	Input        string      `json:"input"`
	Output       string      `json:"output"`
	Error        string      `json:"error"`
	RevertReason string      `json:"revertReason"`
	Logs         []CallLog   `json:"logs"`
	Calls        []CallFrame `json:"calls"`
}

type Call struct {
	TraceID            string
	TraceAddress       []uint32
	ParentTraceAddress []uint32
	Depth              uint32
	CallType           string
	FromAddress        string
	ToAddress          string
	ValueRaw           string
	GasRaw             string
	GasUsedRaw         string
	Input              string
	Output             string
	FunctionSelector   string
	FunctionName       string
	Error              string
	RevertReason       string
	EmittedLogCount    uint32
	Success            bool
	IsPoolCall         bool
	IsRouterCall       bool
	IsMulticall        bool
}

type Result struct {
	Candidate
	Calls              []Call
	RootSelector       string
	RootFunction       string
	FrameCount         uint32
	MaxDepth           uint32
	FailedCallCount    uint32
	DelegatecallCount  uint32
	PoolCallCount      uint32
	RouterAddresses    []string
	MulticallSelectors []string
	RawTrace           json.RawMessage
	TracedAt           time.Time
}

type Client interface {
	TraceTransaction(ctx context.Context, transactionHash string) (CallFrame, json.RawMessage, error)
}

type Store interface {
	TraceCandidates(
		ctx context.Context,
		version string,
		chainID uint64,
		startBlock uint64,
		limit int,
	) ([]Candidate, error)
	InsertTrace(ctx context.Context, result Result) error
	RecordTraceFailure(
		ctx context.Context,
		candidate Candidate,
		version string,
		attemptCount uint32,
		nextRetryAt time.Time,
		status string,
		lastError string,
	) error
}
