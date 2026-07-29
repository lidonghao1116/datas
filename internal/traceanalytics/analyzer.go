package traceanalytics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

var knownSelectors = map[string]string{
	"0xac9650d8": "multicall(bytes[])",
	"0x5ae401dc": "multicall(uint256,bytes[])",
	"0x1f0464d1": "multicall(bytes32,bytes[])",
	"0x252dba42": "aggregate((address,bytes)[])",
	"0xbce38bd7": "tryAggregate(bool,(address,bytes)[])",
	"0x82ad56cb": "aggregate3((address,bool,bytes)[])",
	"0xc3077fa9": "blockAndAggregate((address,bytes)[])",
	"0x3593564c": "execute(bytes,bytes[])",
	"0x24856bc3": "execute(bytes,bytes[],uint256)",
	"0x414bf389": "exactInputSingle",
	"0xc04b8d59": "exactInput",
	"0xdb3e2198": "exactOutputSingle",
	"0xf28c0498": "exactOutput",
	"0x38ed1739": "swapExactTokensForTokens",
	"0x8803dbee": "swapTokensForExactTokens",
	"0x7ff36ab5": "swapExactETHForTokens",
	"0x18cbafe5": "swapExactTokensForETH",
	"0x791ac947": "swapExactTokensForETHSupportingFeeOnTransferTokens",
	"0xb6f9de95": "swapExactETHForTokensSupportingFeeOnTransferTokens",
}

var multicallSelectors = map[string]struct{}{
	"0xac9650d8": {},
	"0x5ae401dc": {},
	"0x1f0464d1": {},
	"0x252dba42": {},
	"0xbce38bd7": {},
	"0x82ad56cb": {},
	"0xc3077fa9": {},
	"0x3593564c": {},
	"0x24856bc3": {},
}

func Analyze(candidate Candidate, root CallFrame, raw []byte, tracedAt time.Time) (Result, error) {
	if !common.IsHexAddress(candidate.WalletAddress) ||
		len(candidate.TransactionHash) != 66 ||
		tracedAt.IsZero() {
		return Result{}, fmt.Errorf("invalid trace candidate")
	}
	pools := make(map[string]struct{}, len(candidate.PoolAddresses))
	for _, address := range candidate.PoolAddresses {
		if common.IsHexAddress(address) {
			pools[normalizeAddress(address)] = struct{}{}
		}
	}
	result := Result{
		Candidate: candidate,
		RawTrace:  append([]byte(nil), raw...),
		TracedAt:  tracedAt.UTC(),
	}
	routers := make(map[string]struct{})
	multicalls := make(map[string]struct{})
	flattenFrame(
		candidate.TransactionHash,
		root,
		nil,
		pools,
		&result,
		routers,
		multicalls,
	)
	if len(result.Calls) == 0 {
		return Result{}, fmt.Errorf("trace contains no call frames")
	}
	result.FrameCount = uint32(len(result.Calls))
	result.RootSelector = result.Calls[0].FunctionSelector
	result.RootFunction = result.Calls[0].FunctionName
	result.RouterAddresses = sortedKeys(routers)
	result.MulticallSelectors = sortedKeys(multicalls)
	return result, nil
}

func flattenFrame(
	transactionHash string,
	frame CallFrame,
	path []uint32,
	pools map[string]struct{},
	result *Result,
	routers map[string]struct{},
	multicalls map[string]struct{},
) bool {
	to := normalizeOptionalAddress(frame.To)
	_, isPool := pools[to]
	subtreeTouchesPool := isPool
	childPoolTouches := make([]bool, len(frame.Calls))
	for index := range frame.Calls {
		childPoolTouches[index] = frameTouchesPool(frame.Calls[index], pools)
		subtreeTouchesPool = subtreeTouchesPool || childPoolTouches[index]
	}
	selector := selector(frame.Input)
	_, isMulticall := multicallSelectors[selector]
	isRouter := !isPool && subtreeTouchesPool && to != ""
	call := Call{
		TraceID:            traceID(transactionHash, path),
		TraceAddress:       append([]uint32(nil), path...),
		ParentTraceAddress: parentPath(path),
		Depth:              uint32(len(path)),
		CallType:           strings.ToUpper(strings.TrimSpace(frame.Type)),
		FromAddress:        normalizeOptionalAddress(frame.From),
		ToAddress:          to,
		ValueRaw:           normalizeQuantity(frame.Value),
		GasRaw:             normalizeQuantity(frame.Gas),
		GasUsedRaw:         normalizeQuantity(frame.GasUsed),
		Input:              strings.ToLower(frame.Input),
		Output:             strings.ToLower(frame.Output),
		FunctionSelector:   selector,
		FunctionName:       knownSelectors[selector],
		Error:              frame.Error,
		RevertReason:       frame.RevertReason,
		EmittedLogCount:    uint32(len(frame.Logs)),
		Success:            frame.Error == "",
		IsPoolCall:         isPool,
		IsRouterCall:       isRouter,
		IsMulticall:        isMulticall,
	}
	result.Calls = append(result.Calls, call)
	if call.Depth > result.MaxDepth {
		result.MaxDepth = call.Depth
	}
	if !call.Success {
		result.FailedCallCount++
	}
	if call.CallType == "DELEGATECALL" {
		result.DelegatecallCount++
	}
	if isPool {
		result.PoolCallCount++
	}
	if isRouter {
		routers[to] = struct{}{}
	}
	if isMulticall {
		multicalls[selector] = struct{}{}
	}
	for index, child := range frame.Calls {
		childPath := append(append([]uint32(nil), path...), uint32(index))
		flattenFrame(
			transactionHash,
			child,
			childPath,
			pools,
			result,
			routers,
			multicalls,
		)
	}
	return subtreeTouchesPool
}

func frameTouchesPool(frame CallFrame, pools map[string]struct{}) bool {
	if _, found := pools[normalizeOptionalAddress(frame.To)]; found {
		return true
	}
	for _, child := range frame.Calls {
		if frameTouchesPool(child, pools) {
			return true
		}
	}
	return false
}

func selector(input string) string {
	input = strings.ToLower(strings.TrimSpace(input))
	if len(input) < 10 || !strings.HasPrefix(input, "0x") {
		return ""
	}
	return input[:10]
}

func traceID(transactionHash string, path []uint32) string {
	if len(path) == 0 {
		return strings.ToLower(transactionHash) + ":root"
	}
	parts := make([]string, len(path))
	for index, item := range path {
		parts[index] = strconv.FormatUint(uint64(item), 10)
	}
	return strings.ToLower(transactionHash) + ":" + strings.Join(parts, ".")
}

func parentPath(path []uint32) []uint32 {
	if len(path) == 0 {
		return []uint32{}
	}
	return append([]uint32(nil), path[:len(path)-1]...)
}

func normalizeAddress(address string) string {
	return strings.ToLower(common.HexToAddress(address).Hex())
}

func normalizeOptionalAddress(address string) string {
	if !common.IsHexAddress(address) {
		return ""
	}
	return normalizeAddress(address)
}

func normalizeQuantity(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return "0x0"
	}
	return value
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
