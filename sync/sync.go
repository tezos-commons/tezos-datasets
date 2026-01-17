package sync

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"tezos-datasets/config"
	"tezos-datasets/datasets"
	"tezos-datasets/fetcher"
	"tezos-datasets/ipfs"
	"tezos-datasets/node"
)

const (
	// BatchSize is the number of blocks per batch
	BatchSize = 100
	// PollInterval is the interval between checking for new blocks
	PollInterval = 10 * time.Second
	// MinBlocksAhead is the minimum blocks ahead of last processed before syncing
	MinBlocksAhead = 110
)

// Syncer coordinates the block syncing process
type Syncer struct {
	cfg          *config.Config
	client       *node.Client
	fetcherPool  *fetcher.FetcherPool
	blocksDS     *datasets.BlocksDataset
	contractsDS  *datasets.ContractsDataset
	ipfsDS       *datasets.IPFSDataset
	ipfsNode     *ipfs.Node
	outputDir    string
	lastBlockN   int64
}

// NewSyncer creates a new syncer
func NewSyncer(cfg *config.Config) (*Syncer, error) {
	client := node.NewClient(cfg.NodeAddresses)

	var blocksDS *datasets.BlocksDataset
	var contractsDS *datasets.ContractsDataset
	var ipfsDS *datasets.IPFSDataset
	var ipfsNode *ipfs.Node
	var err error

	if cfg.EnableBlocks {
		blocksDS, err = datasets.NewBlocksDataset(cfg.OutputDir)
		if err != nil {
			return nil, fmt.Errorf("failed to create blocks dataset: %w", err)
		}
	}

	if cfg.EnableContracts {
		contractsDS, err = datasets.NewContractsDataset(cfg.OutputDir, client)
		if err != nil {
			return nil, fmt.Errorf("failed to create contracts dataset: %w", err)
		}
	}

	if cfg.EnableIPFS {
		// Create embedded IPFS node
		nodeConfig := ipfs.DefaultNodeConfig()
		if len(cfg.IPFSBootstrapPeers) > 0 {
			nodeConfig.BootstrapPeers = cfg.IPFSBootstrapPeers
		}

		ipfsNode, err = ipfs.NewNode(context.Background(), nodeConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create IPFS node: %w", err)
		}

		ipfsDS, err = datasets.NewIPFSDataset(cfg.OutputDir, ipfsNode)
		if err != nil {
			ipfsNode.Close()
			return nil, fmt.Errorf("failed to create IPFS dataset: %w", err)
		}
	}

	fetcherPool := fetcher.NewFetcherPool(client, cfg.NumFetchers)

	syncer := &Syncer{
		cfg:         cfg,
		client:      client,
		fetcherPool: fetcherPool,
		blocksDS:    blocksDS,
		contractsDS: contractsDS,
		ipfsDS:      ipfsDS,
		ipfsNode:    ipfsNode,
		outputDir:   cfg.OutputDir,
	}

	// Set starting block: use --start-from if specified, otherwise load from state file
	if cfg.StartFrom >= 0 {
		syncer.lastBlockN = cfg.StartFrom
	} else {
		syncer.lastBlockN = syncer.loadLastBlock()
	}

	return syncer, nil
}

// Run runs the main sync loop
func (s *Syncer) Run(ctx context.Context) error {
	log.Printf("Starting sync from block %d", s.lastBlockN)
	log.Printf("Using %d Tezos nodes with %d fetcher workers", len(s.cfg.NodeAddresses), s.cfg.NumFetchers)

	// Start the fetcher pool
	s.fetcherPool.Start()
	defer s.fetcherPool.Stop()

	// Start IPFS workers if enabled
	if s.ipfsDS != nil {
		s.ipfsDS.Start()
		defer s.ipfsDS.Stop()
	}

	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()

	// Initial check
	if err := s.checkAndProcess(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		log.Printf("Error during initial check: %v", err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down syncer...")
			return nil
		case <-ticker.C:
			if err := s.checkAndProcess(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				log.Printf("Error during sync: %v", err)
			}
		}
	}
}

// checkAndProcess checks for new blocks and processes them
func (s *Syncer) checkAndProcess(ctx context.Context) error {
	height, err := s.client.GetBlockHeight(ctx)
	if err != nil {
		return fmt.Errorf("failed to get block height: %w", err)
	}

	log.Printf("Current chain height: %d, last processed: %d", height, s.lastBlockN)

	// Process batches as long as we're far enough behind
	for height >= s.lastBlockN+MinBlocksAhead {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := s.processBatch(ctx, s.lastBlockN, s.lastBlockN+BatchSize); err != nil {
			return fmt.Errorf("failed to process batch %d-%d: %w", s.lastBlockN, s.lastBlockN+BatchSize, err)
		}

		s.lastBlockN += BatchSize
		s.saveLastBlock(s.lastBlockN)

		log.Printf("Completed batch, now at block %d", s.lastBlockN)
	}

	return nil
}

// processBatch processes a batch of blocks
func (s *Syncer) processBatch(ctx context.Context, startLevel, endLevel int64) error {
	log.Printf("Processing batch %d-%d", startLevel, endLevel)

	// Fetch all blocks in the batch
	todos, err := s.fetcherPool.FetchBatch(startLevel, endLevel)
	if err != nil {
		return fmt.Errorf("failed to fetch blocks: %w", err)
	}

	// Process each block
	blockData := make([][]byte, 0, len(todos))
	for _, todo := range todos {
		blockData = append(blockData, todo.Data)

		// Process for contracts
		if s.contractsDS != nil {
			if err := s.contractsDS.ProcessBlock(ctx, todo.Data); err != nil {
				log.Printf("Warning: failed to process block %d for contracts: %v", todo.Level, err)
			}
		}

		// Queue for IPFS discovery
		if s.ipfsDS != nil {
			s.ipfsDS.ProcessBlock(todo.Data)
		}
	}

	// Write blocks dataset
	if s.blocksDS != nil {
		if err := s.blocksDS.WriteBatch(startLevel, blockData); err != nil {
			return fmt.Errorf("failed to write blocks: %w", err)
		}
	}

	return nil
}

// loadLastBlock loads the last processed block from the state file
func (s *Syncer) loadLastBlock() int64 {
	stateFile := filepath.Join(s.outputDir, "last_block_n")
	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return 0
		}
		log.Printf("Warning: failed to read state file: %v", err)
		return 0
	}

	n, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		log.Printf("Warning: failed to parse state file: %v", err)
		return 0
	}

	return n
}

// saveLastBlock saves the last processed block to the state file
func (s *Syncer) saveLastBlock(n int64) {
	stateFile := filepath.Join(s.outputDir, "last_block_n")

	// Ensure output directory exists
	if err := os.MkdirAll(s.outputDir, 0755); err != nil {
		log.Printf("Warning: failed to create output directory: %v", err)
		return
	}

	if err := os.WriteFile(stateFile, []byte(strconv.FormatInt(n, 10)), 0644); err != nil {
		log.Printf("Warning: failed to save state file: %v", err)
	}
}

// Close closes all resources
func (s *Syncer) Close() {
	if s.contractsDS != nil {
		s.contractsDS.Close()
	}
	if s.ipfsDS != nil {
		s.ipfsDS.Close()
	}
	if s.ipfsNode != nil {
		s.ipfsNode.Close()
	}
}
