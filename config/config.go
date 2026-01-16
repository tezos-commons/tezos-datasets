package config

import (
	"fmt"
	"strconv"
	"strings"
)

// Config holds all configuration options for tezos-datasets
type Config struct {
	NodeAddresses   []string // Parsed from --node (expand port range)
	NumFetchers     int      // -n flag (default 16)
	OutputDir       string   // --output-dir
	EnableBlocks    bool     // --blocks
	EnableContracts bool     // --contracts
	EnableIPFS      bool     // --ipfs-metadata
	IPFSNodes       []string // --ipfs-node (multiple)
	StartFrom       int64    // --start-from (optional, -1 means use saved state)
}

// ParseNodeAddress parses a node address that may contain a port range.
// For example: "localhost:8730-8750" expands to ["localhost:8730", "localhost:8731", ..., "localhost:8750"]
// A simple address like "localhost:8732" returns ["localhost:8732"]
// Full URLs like "https://node.example.com" are returned as-is.
// Addresses without a port like "node.example.com" are returned as-is.
func ParseNodeAddress(addr string) ([]string, error) {
	// If it's a full URL with scheme, return as-is (no port range expansion)
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return []string{addr}, nil
	}

	// Find the last colon to separate host from port
	lastColon := strings.LastIndex(addr, ":")
	if lastColon == -1 {
		// No port specified, return as-is
		return []string{addr}, nil
	}

	host := addr[:lastColon]
	portPart := addr[lastColon+1:]

	// Check if it's a port range (contains a dash)
	if strings.Contains(portPart, "-") {
		parts := strings.Split(portPart, "-")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid port range format: %s", portPart)
		}

		startPort, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid start port: %s", parts[0])
		}

		endPort, err := strconv.Atoi(parts[1])
		if err != nil {
			return nil, fmt.Errorf("invalid end port: %s", parts[1])
		}

		if startPort > endPort {
			return nil, fmt.Errorf("start port %d is greater than end port %d", startPort, endPort)
		}

		var addresses []string
		for port := startPort; port <= endPort; port++ {
			addresses = append(addresses, fmt.Sprintf("%s:%d", host, port))
		}
		return addresses, nil
	}

	// Single port
	return []string{addr}, nil
}

// ParseNodeAddresses parses multiple node address specifications and returns all expanded addresses
func ParseNodeAddresses(addrs []string) ([]string, error) {
	var allAddresses []string
	for _, addr := range addrs {
		expanded, err := ParseNodeAddress(addr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse node address %q: %w", addr, err)
		}
		allAddresses = append(allAddresses, expanded...)
	}
	return allAddresses, nil
}
