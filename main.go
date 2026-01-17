package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"tezos-datasets/config"
	"tezos-datasets/sync"
)

var (
	nodeAddrs          []string
	numFetchers        int
	outputDir          string
	enableBlocks       bool
	enableContracts    bool
	enableIPFS         bool
	ipfsBootstrapPeers []string
	startFrom          int64
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "tezos-datasets",
		Short: "Scan Tezos blocks and generate datasets",
		Long: `A CLI tool that scans Tezos blocks and generates datasets including:
- Blocks: LZ4 compressed block data files
- Contracts: SQLite database of contract scripts
- IPFS Metadata: SQLite database of IPFS content referenced in blocks`,
		RunE: run,
	}

	rootCmd.Flags().StringArrayVar(&nodeAddrs, "node", []string{"localhost:8732"}, "Tezos node address(es). Supports port ranges like localhost:8730-8750")
	rootCmd.Flags().IntVarP(&numFetchers, "fetchers", "n", 16, "Number of block fetcher workers")
	rootCmd.Flags().StringVar(&outputDir, "output-dir", "./output", "Output directory for datasets")
	rootCmd.Flags().BoolVar(&enableBlocks, "blocks", false, "Enable blocks dataset (LZ4 compressed)")
	rootCmd.Flags().BoolVar(&enableContracts, "contracts", false, "Enable contracts dataset (SQLite)")
	rootCmd.Flags().BoolVar(&enableIPFS, "ipfs-metadata", false, "Enable IPFS metadata dataset (embedded IPFS node)")
	rootCmd.Flags().StringArrayVar(&ipfsBootstrapPeers, "ipfs-bootstrap", nil, "Custom IPFS bootstrap peers (optional, uses defaults if not specified)")
	rootCmd.Flags().Int64Var(&startFrom, "start-from", -1, "Start from specific block level (default: resume from saved state)")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	// Parse node addresses (expand port ranges)
	expandedAddrs, err := config.ParseNodeAddresses(nodeAddrs)
	if err != nil {
		return fmt.Errorf("failed to parse node addresses: %w", err)
	}

	if len(expandedAddrs) == 0 {
		return fmt.Errorf("no node addresses specified")
	}

	// Validate that at least one dataset is enabled
	if !enableBlocks && !enableContracts && !enableIPFS {
		return fmt.Errorf("at least one dataset must be enabled (--blocks, --contracts, or --ipfs-metadata)")
	}

	cfg := &config.Config{
		NodeAddresses:      expandedAddrs,
		NumFetchers:        numFetchers,
		OutputDir:          outputDir,
		EnableBlocks:       enableBlocks,
		EnableContracts:    enableContracts,
		EnableIPFS:         enableIPFS,
		IPFSBootstrapPeers: ipfsBootstrapPeers,
		StartFrom:          startFrom,
	}

	log.Printf("Configuration:")
	log.Printf("  Tezos nodes: %v", cfg.NodeAddresses)
	log.Printf("  Fetcher workers: %d", cfg.NumFetchers)
	log.Printf("  Output directory: %s", cfg.OutputDir)
	log.Printf("  Datasets enabled: blocks=%v, contracts=%v, ipfs=%v", cfg.EnableBlocks, cfg.EnableContracts, cfg.EnableIPFS)

	// Create syncer
	syncer, err := sync.NewSyncer(cfg)
	if err != nil {
		return fmt.Errorf("failed to create syncer: %w", err)
	}
	defer syncer.Close()

	// Setup context with signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT and SIGTERM
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// Run the syncer
	return syncer.Run(ctx)
}
