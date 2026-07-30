package devanalytics

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

var evidenceWeights = map[string]float64{
	"deployer":       1.00,
	"gmgn_funder":    0.90,
	"native_funder":  0.75,
	"trace_caller":   0.65,
	"trace_callee":   0.50,
	"erc20_sender":   0.45,
	"erc20_receiver": 0.35,
}

type Calculator struct{}

func NewCalculator() *Calculator {
	return &Calculator{}
}

func (c *Calculator) Calculate(input Input, calculatedAt time.Time) (Result, error) {
	if input.ChainID == 0 ||
		!validNonZeroAddress(input.TokenAddress) ||
		!validNonZeroAddress(input.PrimaryDeployer) ||
		len(input.DeploymentTransaction) != 66 ||
		input.DeployedAt.IsZero() ||
		calculatedAt.IsZero() {
		return Result{}, fmt.Errorf("invalid Dev analysis input")
	}
	input.TokenAddress = normalizeAddress(input.TokenAddress)
	input.PrimaryDeployer = normalizeAddress(input.PrimaryDeployer)
	relationships := make([]Relationship, 0, len(input.Evidence)+1)
	relationships = append(relationships, Relationship{
		Evidence: Evidence{
			RelatedAddress:          input.PrimaryDeployer,
			AddressKind:             "wallet",
			RelationType:            "deployer",
			Direction:               "self",
			EvidenceCount:           1,
			FirstObservedAt:         input.DeployedAt.UTC(),
			LastObservedAt:          input.DeployedAt.UTC(),
			SampleTransactionHashes: []string{strings.ToLower(input.DeploymentTransaction)},
			Source:                  "canonical_receipt",
		},
		AnalysisVersion: Version,
		ChainID:         input.ChainID,
		TokenAddress:    input.TokenAddress,
		PrimaryDeployer: input.PrimaryDeployer,
		ConfidenceRaw:   "1",
		CalculatedAt:    calculatedAt.UTC(),
	})
	sourceUpdatedAt := input.DeployedAt.UTC()
	sourceSet := map[string]struct{}{"canonical_receipt": {}}
	typeSet := map[string]struct{}{"deployer": {}}
	relatedSet := make(map[string]struct{})
	strongSet := make(map[string]struct{})
	seen := make(map[string]struct{})
	for _, evidence := range input.Evidence {
		evidence.RelatedAddress = normalizeOptionalAddress(evidence.RelatedAddress)
		if evidence.RelatedAddress == "" ||
			evidence.RelatedAddress == input.PrimaryDeployer ||
			evidence.RelatedAddress == input.TokenAddress ||
			evidence.EvidenceCount == 0 ||
			evidence.FirstObservedAt.IsZero() ||
			evidence.LastObservedAt.IsZero() {
			continue
		}
		weight, known := evidenceWeights[evidence.RelationType]
		if !known {
			continue
		}
		key := strings.Join([]string{
			evidence.RelatedAddress,
			evidence.RelationType,
			evidence.Direction,
		}, ":")
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		confidence := math.Min(
			0.99,
			weight+math.Min(0.15, float64(evidence.EvidenceCount-1)*0.03),
		)
		if evidence.AddressKind == "" {
			evidence.AddressKind = "unknown"
		}
		evidence.SampleTransactionHashes = normalizeHashes(
			evidence.SampleTransactionHashes,
			3,
		)
		relationships = append(relationships, Relationship{
			Evidence:        evidence,
			AnalysisVersion: Version,
			ChainID:         input.ChainID,
			TokenAddress:    input.TokenAddress,
			PrimaryDeployer: input.PrimaryDeployer,
			ConfidenceRaw:   formatDecimal(confidence),
			CalculatedAt:    calculatedAt.UTC(),
		})
		relatedSet[evidence.RelatedAddress] = struct{}{}
		if confidence >= 0.70 {
			strongSet[evidence.RelatedAddress] = struct{}{}
		}
		sourceSet[evidence.Source] = struct{}{}
		typeSet[evidence.RelationType] = struct{}{}
		if evidence.LastObservedAt.After(sourceUpdatedAt) {
			sourceUpdatedAt = evidence.LastObservedAt.UTC()
		}
	}
	sort.Slice(relationships, func(i, j int) bool {
		if relationships[i].RelatedAddress != relationships[j].RelatedAddress {
			return relationships[i].RelatedAddress < relationships[j].RelatedAddress
		}
		if relationships[i].RelationType != relationships[j].RelationType {
			return relationships[i].RelationType < relationships[j].RelationType
		}
		return relationships[i].Direction < relationships[j].Direction
	})
	profile := Profile{
		Candidate:           input.Candidate,
		AnalysisVersion:     Version,
		RelatedAddressCount: uint64(len(relatedSet)),
		StrongRelatedCount:  uint64(len(strongSet)),
		RelationshipTypes:   sortedKeys(typeSet),
		CalculatedAt:        calculatedAt.UTC(),
		SourceUpdatedAt:     sourceUpdatedAt,
	}
	riskKnown := false
	for _, risk := range input.DeploymentRisks {
		if !validNonZeroAddress(risk.TokenAddress) {
			continue
		}
		profile.DeployedTokenCount++
		if risk.IsHoneypot || risk.HasBlackMethod || risk.HasMintMethod || risk.IsProxy {
			profile.RiskyDeployedTokenCount++
		}
		if risk.IsHoneypot {
			profile.HoneypotTokenCount++
		}
		if risk.HasBlackMethod {
			profile.BlackMethodTokenCount++
		}
		if risk.HasMintMethod {
			profile.MintMethodTokenCount++
		}
		if risk.IsProxy {
			profile.ProxyTokenCount++
		}
		riskKnown = riskKnown || risk.RiskKnown
		if risk.UpdatedAt.After(profile.SourceUpdatedAt) {
			profile.SourceUpdatedAt = risk.UpdatedAt.UTC()
		}
	}
	score := riskScore(profile)
	profile.RiskScoreRaw = formatDecimal(score)
	profile.RiskLevel = riskLevel(score)
	confidence := 0.40 + math.Min(0.45, float64(len(sourceSet)-1)*0.15)
	if riskKnown {
		confidence += 0.10
	}
	profile.ConfidenceRaw = formatDecimal(math.Min(0.95, confidence))
	profile.TokenAddress = input.TokenAddress
	profile.PrimaryDeployer = input.PrimaryDeployer
	return Result{Profile: profile, Relationships: relationships}, nil
}

func riskScore(profile Profile) float64 {
	score := 0.0
	if profile.HoneypotTokenCount > 0 {
		score += 45
	}
	if profile.BlackMethodTokenCount > 0 {
		score += 20
	}
	if profile.MintMethodTokenCount > 0 {
		score += 10
	}
	if profile.ProxyTokenCount > 0 {
		score += 5
	}
	if profile.DeployedTokenCount > 1 {
		score += math.Min(15, float64(profile.DeployedTokenCount-1)*5)
	}
	score += math.Min(5, float64(profile.StrongRelatedCount))
	return math.Min(100, score)
}

func riskLevel(score float64) string {
	switch {
	case score >= 70:
		return "critical"
	case score >= 45:
		return "high"
	case score >= 20:
		return "medium"
	default:
		return "low"
	}
}

func formatDecimal(value float64) string {
	return strconv.FormatFloat(value, 'f', 6, 64)
}

func normalizeHashes(hashes []string, limit int) []string {
	result := make([]string, 0, limit)
	seen := make(map[string]struct{})
	for _, hash := range hashes {
		hash = strings.ToLower(strings.TrimSpace(hash))
		if len(hash) != 66 {
			continue
		}
		if _, found := seen[hash]; found {
			continue
		}
		seen[hash] = struct{}{}
		result = append(result, hash)
		if len(result) == limit {
			break
		}
	}
	return result
}

func sortedKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func validNonZeroAddress(address string) bool {
	return common.IsHexAddress(address) &&
		common.HexToAddress(address) != (common.Address{})
}

func normalizeAddress(address string) string {
	return strings.ToLower(common.HexToAddress(address).Hex())
}

func normalizeOptionalAddress(address string) string {
	if !validNonZeroAddress(address) {
		return ""
	}
	return normalizeAddress(address)
}
