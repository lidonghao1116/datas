package devanalytics

import (
	"testing"
	"time"
)

func TestCalculatorBuildsExplainableRelationshipsAndRisk(t *testing.T) {
	now := time.Now().UTC()
	input := Input{
		Candidate: Candidate{
			ChainID:               8453,
			TokenAddress:          "0x1111111111111111111111111111111111111111",
			PrimaryDeployer:       "0x2222222222222222222222222222222222222222",
			DeploymentTransaction: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			DeploymentBlock:       10,
			DeployedAt:            now.Add(-time.Hour),
		},
		Evidence: []Evidence{{
			RelatedAddress:  "0x3333333333333333333333333333333333333333",
			RelationType:    "gmgn_funder",
			Direction:       "inbound",
			EvidenceCount:   1,
			FirstObservedAt: now.Add(-2 * time.Hour),
			LastObservedAt:  now,
			Source:          "gmgn",
		}, {
			RelatedAddress:  "0x4444444444444444444444444444444444444444",
			RelationType:    "erc20_sender",
			Direction:       "inbound",
			EvidenceCount:   3,
			FirstObservedAt: now.Add(-time.Hour),
			LastObservedAt:  now,
			Source:          "erc20_transfer",
		}},
		DeploymentRisks: []DeploymentRisk{{
			TokenAddress:  "0x1111111111111111111111111111111111111111",
			IsHoneypot:    true,
			HasMintMethod: true,
			RiskKnown:     true,
			UpdatedAt:     now,
		}, {
			TokenAddress: "0x5555555555555555555555555555555555555555",
			RiskKnown:    true,
			UpdatedAt:    now,
		}},
	}
	result, err := NewCalculator().Calculate(input, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Relationships) != 3 {
		t.Fatalf("unexpected relationships: %+v", result.Relationships)
	}
	if result.Profile.RelatedAddressCount != 2 ||
		result.Profile.StrongRelatedCount != 1 {
		t.Fatalf("unexpected relationship summary: %+v", result.Profile)
	}
	if result.Profile.RiskScoreRaw != "61.000000" ||
		result.Profile.RiskLevel != "high" {
		t.Fatalf("unexpected risk summary: %+v", result.Profile)
	}
	if result.Profile.ConfidenceRaw != "0.800000" {
		t.Fatalf("unexpected confidence: %s", result.Profile.ConfidenceRaw)
	}
}

func TestCalculatorRejectsRecursiveAndUnknownEvidence(t *testing.T) {
	now := time.Now().UTC()
	candidate := Candidate{
		ChainID:               8453,
		TokenAddress:          "0x1111111111111111111111111111111111111111",
		PrimaryDeployer:       "0x2222222222222222222222222222222222222222",
		DeploymentTransaction: "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		DeploymentBlock:       10,
		DeployedAt:            now,
	}
	result, err := NewCalculator().Calculate(Input{
		Candidate: candidate,
		Evidence: []Evidence{{
			RelatedAddress:  candidate.PrimaryDeployer,
			RelationType:    "native_funder",
			EvidenceCount:   1,
			FirstObservedAt: now,
			LastObservedAt:  now,
		}, {
			RelatedAddress:  "0x3333333333333333333333333333333333333333",
			RelationType:    "unsupported_relation",
			EvidenceCount:   1,
			FirstObservedAt: now,
			LastObservedAt:  now,
		}},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Relationships) != 1 ||
		result.Relationships[0].RelationType != "deployer" {
		t.Fatalf("unexpected filtered relationships: %+v", result.Relationships)
	}
}
