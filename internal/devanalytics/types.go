package devanalytics

import (
	"context"
	"time"
)

const Version = "dev-v1"

type Candidate struct {
	ChainID               uint64
	TokenAddress          string
	PrimaryDeployer       string
	DeploymentTransaction string
	DeploymentBlock       uint64
	DeployedAt            time.Time
}

type Evidence struct {
	RelatedAddress          string
	AddressKind             string
	RelationType            string
	Direction               string
	EvidenceCount           uint64
	FirstObservedAt         time.Time
	LastObservedAt          time.Time
	SampleTransactionHashes []string
	Source                  string
}

type DeploymentRisk struct {
	TokenAddress   string
	IsHoneypot     bool
	HasBlackMethod bool
	HasMintMethod  bool
	IsProxy        bool
	RiskKnown      bool
	UpdatedAt      time.Time
}

type Input struct {
	Candidate
	Evidence        []Evidence
	DeploymentRisks []DeploymentRisk
}

type Relationship struct {
	Evidence
	AnalysisVersion string
	ChainID         uint64
	TokenAddress    string
	PrimaryDeployer string
	ConfidenceRaw   string
	CalculatedAt    time.Time
}

type Profile struct {
	Candidate
	AnalysisVersion         string
	RelatedAddressCount     uint64
	StrongRelatedCount      uint64
	RelationshipTypes       []string
	DeployedTokenCount      uint64
	RiskyDeployedTokenCount uint64
	HoneypotTokenCount      uint64
	BlackMethodTokenCount   uint64
	MintMethodTokenCount    uint64
	ProxyTokenCount         uint64
	RiskScoreRaw            string
	RiskLevel               string
	ConfidenceRaw           string
	SourceUpdatedAt         time.Time
	CalculatedAt            time.Time
}

type Result struct {
	Profile       Profile
	Relationships []Relationship
}

type Store interface {
	DevAnalysisCandidates(
		ctx context.Context,
		version string,
		chainID uint64,
		refreshBefore time.Time,
		limit int,
	) ([]Candidate, error)
	LoadDevAnalysis(
		ctx context.Context,
		candidate Candidate,
		evidenceLimit int,
	) (Input, error)
	InsertDevAnalysis(ctx context.Context, result Result) error
}
