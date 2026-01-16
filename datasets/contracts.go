package datasets

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"tezos-datasets/node"
	"tezos-datasets/parser"

	"github.com/Jeffail/gabs/v2"
)

var (
	// contractAddressRegex matches Tezos contract addresses (KT1...)
	contractAddressRegex = regexp.MustCompile(`KT1[a-zA-Z0-9]{33}`)
)

// ContractsDataset handles extracting and storing contract scripts
type ContractsDataset struct {
	db         *sql.DB
	client     *node.Client
	mu         sync.Mutex
	knownAddrs map[string]bool
}

// NewContractsDataset creates a new contracts dataset
func NewContractsDataset(outputDir string, client *node.Client) (*ContractsDataset, error) {
	contractsDir := filepath.Join(outputDir, "contracts")
	if err := os.MkdirAll(contractsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create contracts directory: %w", err)
	}

	dbPath := filepath.Join(contractsDir, "contract_scripts.sqlite")
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS contracts (
			address TEXT PRIMARY KEY,
			script BLOB
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Load existing addresses into memory
	knownAddrs := make(map[string]bool)
	rows, err := db.Query("SELECT address FROM contracts")
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to query existing contracts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to scan address: %w", err)
		}
		knownAddrs[addr] = true
	}

	log.Printf("Loaded %d existing contract addresses", len(knownAddrs))

	return &ContractsDataset{
		db:         db,
		client:     client,
		knownAddrs: knownAddrs,
	}, nil
}

// Close closes the database connection
func (d *ContractsDataset) Close() error {
	return d.db.Close()
}

// ProcessBlock extracts contract addresses from a block and fetches their scripts
func (d *ContractsDataset) ProcessBlock(ctx context.Context, blockData []byte) error {
	container, err := gabs.ParseJSON(blockData)
	if err != nil {
		return fmt.Errorf("failed to parse block JSON: %w", err)
	}

	// Extract all string values and find contract addresses
	strings := parser.ExtractStrings(container)
	addresses := make(map[string]bool)

	for _, s := range strings {
		matches := contractAddressRegex.FindAllString(s, -1)
		for _, addr := range matches {
			addresses[addr] = true
		}
	}

	// Process new addresses
	for addr := range addresses {
		d.mu.Lock()
		known := d.knownAddrs[addr]
		if !known {
			d.knownAddrs[addr] = true
		}
		d.mu.Unlock()

		if !known {
			if err := d.fetchAndStoreContract(ctx, addr); err != nil {
				log.Printf("Contracts: failed to fetch %s: %v", addr, err)
				// Continue with other contracts
			}
		}
	}

	return nil
}

// fetchAndStoreContract fetches a contract's script and stores it in the database
func (d *ContractsDataset) fetchAndStoreContract(ctx context.Context, address string) error {
	log.Printf("Contracts: fetching script for %s", address)

	script, err := d.client.GetContractScript(ctx, address)
	if err != nil {
		return fmt.Errorf("failed to fetch script: %w", err)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	_, err = d.db.ExecContext(ctx, "INSERT OR IGNORE INTO contracts (address, script) VALUES (?, ?)", address, script)
	if err != nil {
		return fmt.Errorf("failed to insert contract: %w", err)
	}

	log.Printf("Contracts: saved %s (%d bytes)", address, len(script))
	return nil
}

// Count returns the number of contracts in the database
func (d *ContractsDataset) Count() (int, error) {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM contracts").Scan(&count)
	return count, err
}
