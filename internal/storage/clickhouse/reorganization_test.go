package clickhouse

import (
	"reflect"
	"testing"

	"github.com/basewatch/base-analytics/internal/domain"
)

func TestOrphanedHashesRemovesEmptyAndDuplicateHashes(t *testing.T) {
	reorganization := &domain.ChainReorganization{
		OrphanedBlocks: []domain.BlockReference{
			{BlockNumber: 10, BlockHash: "0xa"},
			{BlockNumber: 11, BlockHash: ""},
			{BlockNumber: 12, BlockHash: "0xa"},
			{BlockNumber: 13, BlockHash: "0xb"},
		},
	}

	actual := orphanedHashes(reorganization)
	expected := []string{"0xa", "0xb"}
	if !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}
