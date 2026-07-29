package gmgn

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/basewatch/base-analytics/internal/walletenrichment"
)

func decodeStats(
	raw json.RawMessage,
	walletAddress string,
	period string,
) (walletenrichment.Stats, error) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return walletenrichment.Stats{}, fmt.Errorf("GMGN wallet stats data is empty")
	}
	if raw[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil || len(items) == 0 {
			return walletenrichment.Stats{}, fmt.Errorf("decode GMGN wallet stats array")
		}
		raw = items[0]
	}
	var item map[string]json.RawMessage
	if err := json.Unmarshal(raw, &item); err != nil {
		return walletenrichment.Stats{}, fmt.Errorf("decode GMGN wallet stats: %w", err)
	}
	var pnl map[string]json.RawMessage
	_ = json.Unmarshal(item["pnl_stat"], &pnl)
	var common map[string]json.RawMessage
	_ = json.Unmarshal(item["common"], &common)

	stats := walletenrichment.Stats{
		WalletAddress:       walletAddress,
		Period:              period,
		NativeBalanceRaw:    scalar(item["native_balance"]),
		RealizedProfitRaw:   scalar(item["realized_profit"]),
		UnrealizedProfitRaw: scalar(item["unrealized_profit"]),
		PnLRaw: firstScalar(
			item["pnl"],
			item["realized_profit_pnl"],
		),
		WinRateRaw:        firstScalar(item["winrate"], pnl["winrate"]),
		TotalCostRaw:      firstScalar(item["total_cost"], item["bought_cost"]),
		BuyCount:          unsigned(item["buy"]),
		SellCount:         unsigned(item["sell"]),
		TokenCount:        unsigned(pnl["token_num"]),
		AvgHoldingSeconds: unsigned(pnl["avg_holding_period"]),
		SourceUpdatedAt:   unixTime(item["last_timestamp"]),
		RawJSON:           append(json.RawMessage(nil), raw...),
	}
	stats.Identity = walletenrichment.Identity{
		DisplayName:       firstScalar(common["name"], common["nick_name"]),
		ENS:               scalar(common["ens"]),
		PrimaryTag:        scalar(common["tag"]),
		Tags:              stringSlice(common["tags"]),
		TwitterUsername:   scalar(common["twitter_username"]),
		TwitterName:       scalar(common["twitter_name"]),
		TwitterFollowers:  firstUnsigned(common["followers_count"], common["twitter_fans_num"]),
		IsBlueVerified:    boolean(common["is_blue_verified"]),
		CreatedTokenCount: unsigned(common["created_token_count"]),
		WalletCreatedAt:   unixTime(common["created_at"]),
		FundFrom:          scalar(common["fund_from"]),
		FundFromAddress:   scalar(common["fund_from_address"]),
		FundAmountRaw:     scalar(common["fund_amount"]),
	}
	return stats, nil
}

func firstScalar(values ...json.RawMessage) string {
	for _, value := range values {
		if result := scalar(value); result != "" {
			return result
		}
	}
	return ""
}

func scalar(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var text string
	if raw[0] == '"' && json.Unmarshal(raw, &text) == nil {
		return text
	}
	return string(raw)
}

func unsigned(raw json.RawMessage) uint64 {
	value := scalar(raw)
	if value == "" {
		return 0
	}
	if integer, err := strconv.ParseUint(value, 10, 64); err == nil {
		return integer
	}
	if decimal, err := strconv.ParseFloat(value, 64); err == nil && decimal > 0 {
		return uint64(decimal)
	}
	return 0
}

func firstUnsigned(values ...json.RawMessage) uint64 {
	for _, value := range values {
		if result := unsigned(value); result > 0 {
			return result
		}
	}
	return 0
}

func boolean(raw json.RawMessage) bool {
	switch strings.ToLower(scalar(raw)) {
	case "true", "1":
		return true
	default:
		return false
	}
}

func stringSlice(raw json.RawMessage) []string {
	var values []string
	if json.Unmarshal(raw, &values) == nil {
		return values
	}
	return []string{}
}

func unixTime(raw json.RawMessage) time.Time {
	value := scalar(raw)
	if value == "" {
		return time.Time{}
	}
	seconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || seconds <= 0 {
		return time.Time{}
	}
	if seconds > 10_000_000_000 {
		seconds /= 1000
	}
	return time.Unix(seconds, 0).UTC()
}
