package datasets

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"tezos-datasets/ipfs"
	"tezos-datasets/parser"

	"github.com/Jeffail/gabs/v2"
)

const (
	// IPFSFetchWorkers is the number of workers for fetching IPFS content
	IPFSFetchWorkers = 64
	// IPFSDiscoveryWorkers is the number of workers for CID discovery/queueing
	IPFSDiscoveryWorkers = 16
	// MaxContentSize is the maximum content size to store (512KB)
	MaxContentSize = 512 * 1024
	// IPFSFetchTimeout is the timeout for IPFS fetch requests
	IPFSFetchTimeout = 30 * time.Second
	// CIDCacheSize is the LRU cache size for known CIDs
	CIDCacheSize = 50000
	// CoordinatorBatchSize is how many pending CIDs to fetch per query
	CoordinatorBatchSize = 100
	// CoordinatorPollInterval is how often to check for pending CIDs
	CoordinatorPollInterval = 5 * time.Second
)

var (
	// ipfsProtocolRegex matches ipfs:// URIs
	ipfsProtocolRegex = regexp.MustCompile(`ipfs://([a-zA-Z0-9]+(?:/[^"'\s]*)?)`)
	// ipfsGatewayRegex matches common IPFS gateway URLs
	ipfsGatewayRegex = regexp.MustCompile(`https?://(?:ipfs\.io|gateway\.pinata\.cloud|cloudflare-ipfs\.com|dweb\.link)/ipfs/([a-zA-Z0-9]+(?:/[^"'\s]*)?)`)
	// cidRegex matches raw CIDs (Qm... or bafy...)
	cidRegex = regexp.MustCompile(`\b(Qm[a-zA-Z0-9]{44}|bafy[a-zA-Z0-9]{50,})\b`)
)

// IPFSDataset handles extracting and storing IPFS metadata
type IPFSDataset struct {
	db           *sql.DB
	dbMu         sync.Mutex // Protects database writes
	node         *ipfs.Node
	cidCache     *LRUCache
	todoChan     chan string
	discoverChan chan []byte
	ctx          context.Context
	cancel       context.CancelFunc
	workerWg     sync.WaitGroup
}

// NewIPFSDataset creates a new IPFS metadata dataset
func NewIPFSDataset(outputDir string, node *ipfs.Node) (*IPFSDataset, error) {
	ipfsDir := filepath.Join(outputDir, "ipfs")
	if err := os.MkdirAll(ipfsDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create ipfs directory: %w", err)
	}

	dbPath := filepath.Join(ipfsDir, "metadata.sqlite")
	// Configure SQLite for concurrent access:
	// - _journal_mode=WAL: Write-Ahead Logging for better concurrency
	// - _busy_timeout=10000: Wait up to 10 seconds if database is locked
	// - _synchronous=NORMAL: Good balance of safety and performance
	// - _cache_size=-64000: 64MB cache
	dsn := fmt.Sprintf("%s?_journal_mode=WAL&_busy_timeout=10000&_synchronous=NORMAL&_cache_size=-64000", dbPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Limit connections to 1 for writes to avoid lock contention
	// SQLite only supports one writer at a time anyway
	db.SetMaxOpenConns(1)

	// Create table if not exists
	// content_length = 0 AND data IS NULL means pending
	// content_length > 0 AND data IS NULL means too large
	// content_length > 0 AND data IS NOT NULL means fetched (CAR data)
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS metadata (
			cid TEXT PRIMARY KEY,
			content_length INTEGER DEFAULT 0,
			data BLOB
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	// Create LRU cache and seed with recent completed CIDs from DB
	cidCache := NewLRUCache(CIDCacheSize)
	rows, err := db.Query("SELECT cid FROM metadata WHERE content_length > 0 ORDER BY rowid DESC LIMIT ?", CIDCacheSize)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to query existing CIDs: %w", err)
	}
	defer rows.Close()

	var loadedCount int
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			db.Close()
			return nil, fmt.Errorf("failed to scan CID: %w", err)
		}
		cidCache.Add(cid)
		loadedCount++
	}

	// Count pending CIDs
	var pendingCount int
	db.QueryRow("SELECT COUNT(*) FROM metadata WHERE content_length = 0 AND data IS NULL").Scan(&pendingCount)

	log.Printf("IPFS: loaded %d completed CIDs into cache, %d pending", loadedCount, pendingCount)

	ctx, cancel := context.WithCancel(context.Background())

	return &IPFSDataset{
		db:           db,
		node:         node,
		cidCache:     cidCache,
		todoChan:     make(chan string, IPFSFetchWorkers*2),
		discoverChan: make(chan []byte, IPFSDiscoveryWorkers*2),
		ctx:          ctx,
		cancel:       cancel,
	}, nil
}

// Start starts the IPFS fetcher workers and coordinator
func (d *IPFSDataset) Start() {
	// Start fetch workers
	for i := 0; i < IPFSFetchWorkers; i++ {
		d.workerWg.Add(1)
		go d.fetchWorker()
	}

	// Start discovery workers
	for i := 0; i < IPFSDiscoveryWorkers; i++ {
		d.workerWg.Add(1)
		go d.discoveryWorker()
	}

	// Start coordinator
	d.workerWg.Add(1)
	go d.coordinator()
}

// Stop stops all workers
func (d *IPFSDataset) Stop() {
	log.Printf("IPFS: stopping workers...")
	d.cancel()
	close(d.discoverChan)

	// Wait for workers with timeout
	done := make(chan struct{})
	go func() {
		d.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Printf("IPFS: all workers stopped")
	case <-time.After(5 * time.Second):
		log.Printf("IPFS: timeout waiting for workers, continuing shutdown")
	}

	close(d.todoChan)
}

// Close closes the database connection
func (d *IPFSDataset) Close() error {
	return d.db.Close()
}

// coordinator queries DB for pending CIDs and sends them to workers
func (d *IPFSDataset) coordinator() {
	defer d.workerWg.Done()

	log.Printf("IPFS: coordinator started")

	// Initial dispatch immediately
	d.dispatchPending()

	ticker := time.NewTicker(CoordinatorPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			log.Printf("IPFS: coordinator stopping")
			return
		case <-ticker.C:
			d.dispatchPending()
		}
	}
}

// dispatchPending queries for pending CIDs and sends them to workers
func (d *IPFSDataset) dispatchPending() {
	// Debug: check total rows and pending rows
	var total, pending int
	d.db.QueryRow("SELECT COUNT(*) FROM metadata").Scan(&total)
	d.db.QueryRow("SELECT COUNT(*) FROM metadata WHERE content_length = 0 AND data IS NULL").Scan(&pending)
	log.Printf("IPFS: DB has %d total rows, %d pending", total, pending)

	rows, err := d.db.QueryContext(d.ctx,
		"SELECT cid FROM metadata WHERE content_length = 0 AND data IS NULL ORDER BY RANDOM() LIMIT ?",
		CoordinatorBatchSize)
	if err != nil {
		if d.ctx.Err() == nil {
			log.Printf("IPFS: failed to query pending CIDs: %v", err)
		}
		return
	}
	defer rows.Close()

	dispatched := 0
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			continue
		}

		select {
		case d.todoChan <- cid:
			dispatched++
		case <-d.ctx.Done():
			return
		}
	}

	log.Printf("IPFS: coordinator dispatched %d pending CIDs", dispatched)
}

// fetchWorker fetches IPFS content
func (d *IPFSDataset) fetchWorker() {
	defer d.workerWg.Done()

	for {
		select {
		case <-d.ctx.Done():
			return
		case cid, ok := <-d.todoChan:
			if !ok {
				return
			}
			d.fetchCID(cid)
		}
	}
}

// discoveryWorker processes blocks to discover CIDs
func (d *IPFSDataset) discoveryWorker() {
	defer d.workerWg.Done()

	for {
		select {
		case <-d.ctx.Done():
			return
		case blockData, ok := <-d.discoverChan:
			if !ok {
				return
			}
			d.discoverCIDs(blockData)
		}
	}
}

// ProcessBlock queues a block for CID discovery
func (d *IPFSDataset) ProcessBlock(blockData []byte) {
	select {
	case d.discoverChan <- blockData:
	case <-d.ctx.Done():
	}
}

// discoverCIDs extracts CIDs from a block
func (d *IPFSDataset) discoverCIDs(blockData []byte) {
	container, err := gabs.ParseJSON(blockData)
	if err != nil {
		log.Printf("IPFS: failed to parse block JSON: %v", err)
		return
	}

	strings := parser.ExtractStrings(container)
	cids := make(map[string]bool)

	for _, s := range strings {
		// Check ipfs:// protocol
		if matches := ipfsProtocolRegex.FindAllStringSubmatch(s, -1); matches != nil {
			for _, m := range matches {
				cids[normalizeCID(m[1])] = true
			}
		}

		// Check gateway URLs
		if matches := ipfsGatewayRegex.FindAllStringSubmatch(s, -1); matches != nil {
			for _, m := range matches {
				cids[normalizeCID(m[1])] = true
			}
		}

		// Check raw CIDs
		if matches := cidRegex.FindAllString(s, -1); matches != nil {
			for _, m := range matches {
				cids[m] = true
			}
		}

		// Try hex decoding
		if decoded := tryHexDecode(s); decoded != "" {
			// Check decoded string for IPFS patterns
			if matches := ipfsProtocolRegex.FindAllStringSubmatch(decoded, -1); matches != nil {
				for _, m := range matches {
					cids[normalizeCID(m[1])] = true
				}
			}
			if matches := cidRegex.FindAllString(decoded, -1); matches != nil {
				for _, m := range matches {
					cids[m] = true
				}
			}
		}
	}

	// Insert new CIDs as pending
	for cid := range cids {
		if cid == "" {
			continue
		}

		// Check cache first (completed CIDs)
		if d.cidCache.Contains(cid) {
			continue
		}

		// Try to insert as pending (content_length=0, data=NULL)
		// INSERT OR IGNORE will skip if already exists
		d.dbMu.Lock()
		result, err := d.db.Exec("INSERT OR IGNORE INTO metadata (cid, content_length) VALUES (?, 0)", cid)
		d.dbMu.Unlock()
		if err != nil {
			continue
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			log.Printf("IPFS: discovered new CID %s", cid)
		}
	}
}

// normalizeCID extracts the CID from a path like "CID/path/to/file"
func normalizeCID(cidPath string) string {
	parts := strings.SplitN(cidPath, "/", 2)
	return parts[0]
}

// tryHexDecode attempts to decode a hex string
func tryHexDecode(s string) string {
	// Remove common prefixes
	s = strings.TrimPrefix(s, "0x")

	// Must be even length and contain only hex chars
	if len(s)%2 != 0 || len(s) < 10 {
		return ""
	}

	decoded, err := hex.DecodeString(s)
	if err != nil {
		return ""
	}

	return string(decoded)
}

// fetchCID fetches content from the embedded IPFS node and stores as CAR
func (d *IPFSDataset) fetchCID(cidStr string) {
	// Check if we're shutting down before starting expensive operation
	select {
	case <-d.ctx.Done():
		return
	default:
	}

	if d.node == nil {
		log.Printf("IPFS: node not initialized, skipping %s", cidStr)
		return
	}

	// Double-check if still pending
	var contentLength int64
	err := d.db.QueryRow("SELECT content_length FROM metadata WHERE cid = ?", cidStr).Scan(&contentLength)
	if err != nil || contentLength > 0 {
		// Already fetched or doesn't exist
		return
	}

	log.Printf("IPFS: fetching %s (connected to %d peers)", cidStr, d.node.PeerCount())

	// Fetch as CAR data from the embedded node
	carData, err := d.node.FetchCAR(cidStr)
	if err != nil {
		log.Printf("IPFS: failed to fetch %s: %v (will retry)", cidStr, err)
		return
	}

	fetchedLength := int64(len(carData))
	var dataToStore []byte

	// If CAR data exceeds MaxContentSize, store metadata only
	if fetchedLength > MaxContentSize {
		dataToStore = nil
	} else {
		dataToStore = carData
	}

	// Update in database
	d.dbMu.Lock()
	_, err = d.db.Exec(
		"UPDATE metadata SET content_length = ?, data = ? WHERE cid = ?",
		fetchedLength, dataToStore, cidStr,
	)
	d.dbMu.Unlock()
	if err != nil {
		log.Printf("IPFS: failed to store %s: %v", cidStr, err)
		return
	}

	// Add to cache
	d.cidCache.Add(cidStr)

	if dataToStore != nil {
		log.Printf("IPFS: fetched %s as CAR (%d bytes)", cidStr, fetchedLength)
	} else {
		log.Printf("IPFS: fetched %s (too large: %d bytes, stored metadata only)", cidStr, fetchedLength)
	}
}

// Count returns the number of completed CIDs in the database
func (d *IPFSDataset) Count() (int, error) {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM metadata WHERE content_length > 0").Scan(&count)
	return count, err
}

// PendingCount returns the number of pending CIDs
func (d *IPFSDataset) PendingCount() (int, error) {
	var count int
	err := d.db.QueryRow("SELECT COUNT(*) FROM metadata WHERE content_length = 0 AND data IS NULL").Scan(&count)
	return count, err
}
