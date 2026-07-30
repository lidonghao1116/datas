package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/basewatch/base-analytics/internal/gateway"
)

func (s *EventStore) TokenDevProfile(
	ctx context.Context,
	chainID uint64,
	address string,
) (gateway.TokenDevProfile, error) {
	var profile gateway.TokenDevProfile
	err := s.conn.QueryRow(ctx, `
		SELECT
			analysis_version, chain_id, token_address, primary_deployer,
			deployment_transaction, deployment_block, deployed_at,
			related_address_count, strong_related_count, relationship_types,
			deployed_token_count, risky_deployed_token_count,
			honeypot_token_count, black_method_token_count,
			mint_method_token_count, proxy_token_count, risk_score_raw,
			risk_level, confidence_raw, source_updated_at, calculated_at
		FROM token_dev_profiles FINAL
		WHERE chain_id = ? AND token_address = ?
		ORDER BY calculated_at DESC
		LIMIT 1`,
		chainID,
		address,
	).Scan(
		&profile.AnalysisVersion,
		&profile.ChainID,
		&profile.TokenAddress,
		&profile.PrimaryDeployer,
		&profile.DeploymentTransaction,
		&profile.DeploymentBlock,
		&profile.DeployedAt,
		&profile.RelatedAddressCount,
		&profile.StrongRelatedCount,
		&profile.RelationshipTypes,
		&profile.DeployedTokenCount,
		&profile.RiskyDeployedTokenCount,
		&profile.HoneypotTokenCount,
		&profile.BlackMethodTokenCount,
		&profile.MintMethodTokenCount,
		&profile.ProxyTokenCount,
		&profile.RiskScoreRaw,
		&profile.RiskLevel,
		&profile.ConfidenceRaw,
		&profile.SourceUpdatedAt,
		&profile.CalculatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return gateway.TokenDevProfile{}, gateway.ErrNotFound
	}
	if err != nil {
		return gateway.TokenDevProfile{}, fmt.Errorf("query Token Dev profile: %w", err)
	}
	rows, err := s.conn.Query(ctx, `
		SELECT
			primary_deployer, related_address, address_kind, relation_type,
			direction, evidence_count, confidence_raw, first_observed_at,
			last_observed_at, sample_transaction_hashes, evidence_source
		FROM token_dev_relationships FINAL
		WHERE analysis_version = ?
		  AND chain_id = ?
		  AND token_address = ?
		ORDER BY
			toFloat64OrZero(confidence_raw) DESC,
			relation_type,
			related_address`,
		profile.AnalysisVersion,
		chainID,
		address,
	)
	if err != nil {
		return gateway.TokenDevProfile{}, fmt.Errorf("query Token Dev relationships: %w", err)
	}
	defer rows.Close()
	profile.Relationships = make([]gateway.TokenDevRelationship, 0)
	for rows.Next() {
		var relationship gateway.TokenDevRelationship
		if err := rows.Scan(
			&relationship.PrimaryDeployer,
			&relationship.RelatedAddress,
			&relationship.AddressKind,
			&relationship.RelationType,
			&relationship.Direction,
			&relationship.EvidenceCount,
			&relationship.ConfidenceRaw,
			&relationship.FirstObservedAt,
			&relationship.LastObservedAt,
			&relationship.SampleTransactionHashes,
			&relationship.EvidenceSource,
		); err != nil {
			return gateway.TokenDevProfile{}, fmt.Errorf("scan Token Dev relationship: %w", err)
		}
		profile.Relationships = append(profile.Relationships, relationship)
	}
	if err := rows.Err(); err != nil {
		return gateway.TokenDevProfile{}, fmt.Errorf("iterate Token Dev relationships: %w", err)
	}
	return profile, nil
}
