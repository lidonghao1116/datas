package registry

import "time"

type Factory struct {
	ChainID         uint64
	Address         string
	Protocol        string
	ProtocolVersion string
	ProtocolFamily  string
	Verified        bool
	Source          string
}

type Pool struct {
	ChainID         uint64
	Address         string
	FactoryAddress  string
	Protocol        string
	ProtocolVersion string
	ProtocolFamily  string
	Token0Address   string
	Token1Address   string
	DiscoveredBlock uint64
	ObservedAt      time.Time
}

type Token struct {
	ChainID       uint64
	Address       string
	Symbol        string
	Decimals      uint8
	SymbolKnown   bool
	DecimalsKnown bool
	ObservedAt    time.Time
}
