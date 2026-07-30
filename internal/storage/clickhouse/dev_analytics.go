package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/basewatch/base-analytics/internal/devanalytics"
)

func (s *EventStore) DevAnalysisCandidates(
	ctx context.Context,
	version string,
	chainID uint64,
	refreshBefore time.Time,
	limit int,
) ([]devanalytics.Candidate, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			traded_tokens AS (
				SELECT
					chain_id,
					arrayJoin([token0_address, token1_address]) AS token_address
				FROM dex_pool_swaps FINAL
				WHERE chain_id = ? AND is_canonical = 1
				GROUP BY chain_id, token_address
			),
			deployments AS (
				SELECT
					chain_id,
					lower(contract_address) AS token_address,
					argMin(transaction_hash, block_number) AS deployment_transaction,
					min(block_number) AS deployment_block,
					argMin(block_time, block_number) AS deployed_at
				FROM raw_receipts FINAL
				WHERE chain_id = ?
				  AND is_canonical = 1
				  AND status = 1
				  AND contract_address != ''
				GROUP BY chain_id, token_address
			),
			transactions AS (
				SELECT
					chain_id,
					transaction_hash,
					argMax(lower(from_address), observed_at) AS deployer
				FROM raw_transactions FINAL
				WHERE chain_id = ? AND is_canonical = 1
				GROUP BY chain_id, transaction_hash
			),
			latest_profiles AS (
				SELECT
					token_address,
					max(calculated_at) AS latest_calculated_at
				FROM token_dev_profiles
				WHERE analysis_version = ? AND chain_id = ?
				GROUP BY token_address
			)
		SELECT
			d.chain_id,
			d.token_address,
			t.deployer,
			d.deployment_transaction,
			d.deployment_block,
			d.deployed_at
		FROM deployments AS d
		INNER JOIN traded_tokens AS tokens
			ON tokens.chain_id = d.chain_id
			AND tokens.token_address = d.token_address
		INNER JOIN transactions AS t
			ON t.chain_id = d.chain_id
			AND t.transaction_hash = d.deployment_transaction
		LEFT JOIN latest_profiles AS p
			ON p.token_address = d.token_address
		WHERE t.deployer != ''
		  AND (
			p.token_address = ''
			OR p.latest_calculated_at <= ?
		  )
		ORDER BY d.deployment_block, d.token_address
		LIMIT ?`,
		chainID,
		chainID,
		chainID,
		version,
		chainID,
		refreshBefore,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query Dev analysis candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]devanalytics.Candidate, 0, limit)
	for rows.Next() {
		var candidate devanalytics.Candidate
		if err := rows.Scan(
			&candidate.ChainID,
			&candidate.TokenAddress,
			&candidate.PrimaryDeployer,
			&candidate.DeploymentTransaction,
			&candidate.DeploymentBlock,
			&candidate.DeployedAt,
		); err != nil {
			return nil, fmt.Errorf("scan Dev analysis candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate Dev analysis candidates: %w", err)
	}
	return candidates, nil
}

func (s *EventStore) LoadDevAnalysis(
	ctx context.Context,
	candidate devanalytics.Candidate,
	evidenceLimit int,
) (devanalytics.Input, error) {
	input := devanalytics.Input{Candidate: candidate}
	if err := s.appendGMGNFundingEvidence(ctx, &input); err != nil {
		return devanalytics.Input{}, err
	}
	if err := s.appendNativeFundingEvidence(ctx, &input, evidenceLimit); err != nil {
		return devanalytics.Input{}, err
	}
	if err := s.appendTransferEvidence(ctx, &input, evidenceLimit); err != nil {
		return devanalytics.Input{}, err
	}
	if err := s.appendTraceEvidence(ctx, &input, evidenceLimit); err != nil {
		return devanalytics.Input{}, err
	}
	if err := s.classifyDevEvidenceAddresses(ctx, &input); err != nil {
		return devanalytics.Input{}, err
	}
	risks, err := s.deployerTokenRisks(ctx, candidate)
	if err != nil {
		return devanalytics.Input{}, err
	}
	input.DeploymentRisks = risks
	return input, nil
}

func (s *EventStore) classifyDevEvidenceAddresses(
	ctx context.Context,
	input *devanalytics.Input,
) error {
	addresses := make([]string, 0, len(input.Evidence))
	seen := make(map[string]struct{})
	for _, evidence := range input.Evidence {
		if evidence.RelatedAddress == "" {
			continue
		}
		if _, found := seen[evidence.RelatedAddress]; found {
			continue
		}
		seen[evidence.RelatedAddress] = struct{}{}
		addresses = append(addresses, evidence.RelatedAddress)
	}
	if len(addresses) == 0 {
		return nil
	}
	rows, err := s.conn.Query(ctx, `
		WITH known_contracts AS (
			SELECT lower(contract_address) AS address
			FROM raw_receipts FINAL
			WHERE chain_id = ?
			  AND contract_address != ''
			  AND has(?, lower(contract_address))
			UNION ALL
			SELECT pool_address AS address
			FROM dex_pool_swaps FINAL
			WHERE chain_id = ?
			  AND has(?, pool_address)
			GROUP BY pool_address
			UNION ALL
			SELECT address
			FROM (
				SELECT arrayJoin([token0_address, token1_address]) AS address
				FROM dex_pool_swaps FINAL
				WHERE chain_id = ?
			)
			WHERE has(?, address)
			GROUP BY address
		)
		SELECT address
		FROM known_contracts
		GROUP BY address`,
		input.ChainID,
		addresses,
		input.ChainID,
		addresses,
		input.ChainID,
		addresses,
	)
	if err != nil {
		return fmt.Errorf("classify Dev relationship addresses: %w", err)
	}
	defer rows.Close()
	contracts := make(map[string]struct{})
	for rows.Next() {
		var address string
		if err := rows.Scan(&address); err != nil {
			return fmt.Errorf("scan Dev relationship address kind: %w", err)
		}
		contracts[address] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate Dev relationship address kinds: %w", err)
	}
	for index := range input.Evidence {
		if _, found := contracts[input.Evidence[index].RelatedAddress]; found {
			input.Evidence[index].AddressKind = "contract"
		}
	}
	return nil
}

func (s *EventStore) appendGMGNFundingEvidence(
	ctx context.Context,
	input *devanalytics.Input,
) error {
	var evidence devanalytics.Evidence
	evidence.AddressKind = "unknown"
	evidence.RelationType = "gmgn_funder"
	evidence.Direction = "inbound"
	evidence.Source = "gmgn"
	err := s.conn.QueryRow(ctx, `
		SELECT
			argMax(lower(fund_from_address), fetched_at),
			count(),
			min(fetched_at),
			max(fetched_at)
		FROM gmgn_wallet_profile_snapshots
		WHERE chain_id = ?
		  AND wallet_address = ?
		  AND fund_from_address != ''`,
		input.ChainID,
		input.PrimaryDeployer,
	).Scan(
		&evidence.RelatedAddress,
		&evidence.EvidenceCount,
		&evidence.FirstObservedAt,
		&evidence.LastObservedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("query GMGN Dev funding evidence: %w", err)
	}
	if evidence.EvidenceCount > 0 {
		input.Evidence = append(input.Evidence, evidence)
	}
	return nil
}

func (s *EventStore) appendNativeFundingEvidence(
	ctx context.Context,
	input *devanalytics.Input,
	limit int,
) error {
	rows, err := s.conn.Query(ctx, `
		SELECT
			lower(from_address),
			count(),
			min(block_time),
			max(block_time),
			groupUniqArray(3)(transaction_hash)
		FROM raw_transactions FINAL
		WHERE chain_id = ?
		  AND is_canonical = 1
		  AND lower(to_address) = ?
		  AND lower(from_address) != ?
		  AND match(lower(value_raw), '^0x0*$') = 0
		  AND block_time <= ?
		GROUP BY lower(from_address)
		ORDER BY count() DESC, max(block_time) DESC
		LIMIT ?`,
		input.ChainID,
		input.PrimaryDeployer,
		input.PrimaryDeployer,
		input.DeployedAt.Add(30*24*time.Hour),
		limit,
	)
	if err != nil {
		return fmt.Errorf("query native Dev funding evidence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		evidence := devanalytics.Evidence{
			AddressKind:  "unknown",
			RelationType: "native_funder",
			Direction:    "inbound",
			Source:       "canonical_transaction",
		}
		if err := rows.Scan(
			&evidence.RelatedAddress,
			&evidence.EvidenceCount,
			&evidence.FirstObservedAt,
			&evidence.LastObservedAt,
			&evidence.SampleTransactionHashes,
		); err != nil {
			return fmt.Errorf("scan native Dev funding evidence: %w", err)
		}
		input.Evidence = append(input.Evidence, evidence)
	}
	return rows.Err()
}

func (s *EventStore) appendTransferEvidence(
	ctx context.Context,
	input *devanalytics.Input,
	limit int,
) error {
	rows, err := s.conn.Query(ctx, `
		SELECT
			if(from_address = ?, to_address, from_address) AS related_address,
			if(to_address = ?, 'erc20_sender', 'erc20_receiver') AS relation_type,
			if(to_address = ?, 'inbound', 'outbound') AS direction,
			count(),
			min(block_time),
			max(block_time),
			groupUniqArray(3)(transaction_hash)
		FROM erc20_transfers FINAL
		WHERE chain_id = ?
		  AND is_canonical = 1
		  AND (from_address = ? OR to_address = ?)
		  AND from_address != to_address
		  AND block_time >= ?
		  AND block_time <= ?
		GROUP BY related_address, relation_type, direction
		ORDER BY count() DESC, max(block_time) DESC
		LIMIT ?`,
		input.PrimaryDeployer,
		input.PrimaryDeployer,
		input.PrimaryDeployer,
		input.ChainID,
		input.PrimaryDeployer,
		input.PrimaryDeployer,
		input.DeployedAt,
		input.DeployedAt.Add(30*24*time.Hour),
		limit,
	)
	if err != nil {
		return fmt.Errorf("query ERC-20 Dev relationship evidence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		evidence := devanalytics.Evidence{
			AddressKind: "unknown",
			Source:      "erc20_transfer",
		}
		if err := rows.Scan(
			&evidence.RelatedAddress,
			&evidence.RelationType,
			&evidence.Direction,
			&evidence.EvidenceCount,
			&evidence.FirstObservedAt,
			&evidence.LastObservedAt,
			&evidence.SampleTransactionHashes,
		); err != nil {
			return fmt.Errorf("scan ERC-20 Dev relationship evidence: %w", err)
		}
		input.Evidence = append(input.Evidence, evidence)
	}
	return rows.Err()
}

func (s *EventStore) appendTraceEvidence(
	ctx context.Context,
	input *devanalytics.Input,
	limit int,
) error {
	rows, err := s.conn.Query(ctx, `
		SELECT
			if(to_address = ?, from_address, to_address) AS related_address,
			if(to_address = ?, 'trace_caller', 'trace_callee') AS relation_type,
			if(to_address = ?, 'inbound', 'outbound') AS direction,
			count(),
			min(block_time),
			max(block_time),
			groupUniqArray(3)(transaction_hash)
		FROM transaction_call_traces FINAL
		WHERE chain_id = ?
		  AND success = 1
		  AND (from_address = ? OR to_address = ?)
		  AND from_address != to_address
		GROUP BY related_address, relation_type, direction
		ORDER BY count() DESC, max(block_time) DESC
		LIMIT ?`,
		input.PrimaryDeployer,
		input.PrimaryDeployer,
		input.PrimaryDeployer,
		input.ChainID,
		input.PrimaryDeployer,
		input.PrimaryDeployer,
		limit,
	)
	if err != nil {
		return fmt.Errorf("query Trace Dev relationship evidence: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		evidence := devanalytics.Evidence{
			AddressKind: "unknown",
			Source:      "call_trace",
		}
		if err := rows.Scan(
			&evidence.RelatedAddress,
			&evidence.RelationType,
			&evidence.Direction,
			&evidence.EvidenceCount,
			&evidence.FirstObservedAt,
			&evidence.LastObservedAt,
			&evidence.SampleTransactionHashes,
		); err != nil {
			return fmt.Errorf("scan Trace Dev relationship evidence: %w", err)
		}
		input.Evidence = append(input.Evidence, evidence)
	}
	return rows.Err()
}

func (s *EventStore) deployerTokenRisks(
	ctx context.Context,
	candidate devanalytics.Candidate,
) ([]devanalytics.DeploymentRisk, error) {
	rows, err := s.conn.Query(ctx, `
		WITH
			traded_tokens AS (
				SELECT
					chain_id,
					arrayJoin([token0_address, token1_address]) AS token_address
				FROM dex_pool_swaps FINAL
				WHERE chain_id = ? AND is_canonical = 1
				GROUP BY chain_id, token_address
			),
			deployments AS (
				SELECT
					r.chain_id,
					lower(r.contract_address) AS token_address,
					argMin(lower(t.from_address), r.block_number) AS deployer
				FROM raw_receipts AS r FINAL
				INNER JOIN raw_transactions AS t FINAL
					ON t.chain_id = r.chain_id
					AND t.transaction_hash = r.transaction_hash
				WHERE r.chain_id = ?
				  AND r.is_canonical = 1
				  AND r.status = 1
				  AND r.contract_address != ''
				GROUP BY r.chain_id, token_address
			),
			latest_risk AS (
				SELECT
					token_address,
					argMax(ifNull(is_honeypot, 0), fetched_at) AS latest_is_honeypot,
					argMax(ifNull(has_black_method, 0), fetched_at) AS latest_has_black_method,
					argMax(ifNull(has_mint_method, 0), fetched_at) AS latest_has_mint_method,
					argMax(ifNull(is_proxy, 0), fetched_at) AS latest_is_proxy,
					count() AS risk_count,
					max(fetched_at) AS latest_risk_at
				FROM token_risk_snapshots
				WHERE chain_id = ?
				GROUP BY token_address
			)
		SELECT
			d.token_address,
			r.latest_is_honeypot,
			r.latest_has_black_method,
			r.latest_has_mint_method,
			r.latest_is_proxy,
			r.risk_count,
			r.latest_risk_at
		FROM deployments AS d
		INNER JOIN traded_tokens AS tokens
			ON tokens.chain_id = d.chain_id
			AND tokens.token_address = d.token_address
		LEFT JOIN latest_risk AS r
			ON r.token_address = d.token_address
		WHERE d.deployer = ?
		ORDER BY d.token_address`,
		candidate.ChainID,
		candidate.ChainID,
		candidate.ChainID,
		candidate.PrimaryDeployer,
	)
	if err != nil {
		return nil, fmt.Errorf("query deployer Token risks: %w", err)
	}
	defer rows.Close()
	risks := make([]devanalytics.DeploymentRisk, 0)
	for rows.Next() {
		var risk devanalytics.DeploymentRisk
		var honeypot, black, mint, proxy uint8
		var riskCount uint64
		if err := rows.Scan(
			&risk.TokenAddress,
			&honeypot,
			&black,
			&mint,
			&proxy,
			&riskCount,
			&risk.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan deployer Token risk: %w", err)
		}
		risk.IsHoneypot = honeypot == 1
		risk.HasBlackMethod = black == 1
		risk.HasMintMethod = mint == 1
		risk.IsProxy = proxy == 1
		risk.RiskKnown = riskCount > 0
		risks = append(risks, risk)
	}
	return risks, rows.Err()
}

func (s *EventStore) InsertDevAnalysis(
	ctx context.Context,
	result devanalytics.Result,
) error {
	if len(result.Relationships) > 0 {
		batch, err := s.conn.PrepareBatch(ctx, `
			INSERT INTO token_dev_relationships (
				analysis_version, chain_id, token_address, primary_deployer,
				related_address, address_kind, relation_type, direction,
				evidence_count, confidence_raw, first_observed_at,
				last_observed_at, sample_transaction_hashes, evidence_source,
				calculated_at
			)`)
		if err != nil {
			return fmt.Errorf("prepare Dev relationship batch: %w", err)
		}
		for _, relationship := range result.Relationships {
			if err := batch.Append(
				relationship.AnalysisVersion,
				relationship.ChainID,
				relationship.TokenAddress,
				relationship.PrimaryDeployer,
				relationship.RelatedAddress,
				relationship.AddressKind,
				relationship.RelationType,
				relationship.Direction,
				relationship.EvidenceCount,
				relationship.ConfidenceRaw,
				relationship.FirstObservedAt,
				relationship.LastObservedAt,
				relationship.SampleTransactionHashes,
				relationship.Source,
				relationship.CalculatedAt,
			); err != nil {
				return fmt.Errorf("append Dev relationship: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("send Dev relationship batch: %w", err)
		}
	}
	profile := result.Profile
	if err := s.conn.Exec(ctx, `
		INSERT INTO token_dev_profiles (
			analysis_version, chain_id, token_address, primary_deployer,
			deployment_transaction, deployment_block, deployed_at,
			related_address_count, strong_related_count, relationship_types,
			deployed_token_count, risky_deployed_token_count,
			honeypot_token_count, black_method_token_count,
			mint_method_token_count, proxy_token_count, risk_score_raw,
			risk_level, confidence_raw, source_updated_at, calculated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		profile.AnalysisVersion,
		profile.ChainID,
		profile.TokenAddress,
		profile.PrimaryDeployer,
		profile.DeploymentTransaction,
		profile.DeploymentBlock,
		profile.DeployedAt,
		profile.RelatedAddressCount,
		profile.StrongRelatedCount,
		profile.RelationshipTypes,
		profile.DeployedTokenCount,
		profile.RiskyDeployedTokenCount,
		profile.HoneypotTokenCount,
		profile.BlackMethodTokenCount,
		profile.MintMethodTokenCount,
		profile.ProxyTokenCount,
		profile.RiskScoreRaw,
		profile.RiskLevel,
		profile.ConfidenceRaw,
		profile.SourceUpdatedAt,
		profile.CalculatedAt,
	); err != nil {
		return fmt.Errorf("insert Dev profile: %w", err)
	}
	return nil
}
