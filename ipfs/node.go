package ipfs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ipld "github.com/ipfs/go-ipld-format"
	car "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	ipfslite "github.com/hsanjuan/ipfs-lite"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
)

// DefaultBootstrapPeers are the default IPFS bootstrap nodes
var DefaultBootstrapPeers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
}

// NodeConfig holds configuration for the embedded IPFS node
type NodeConfig struct {
	// BootstrapPeers are the multiaddrs of peers to connect to on startup
	BootstrapPeers []string
	// ListenAddrs are the addresses the node will listen on
	ListenAddrs []string
	// ConnectionTimeout is the timeout for connecting to the network
	ConnectionTimeout time.Duration
	// FetchTimeout is the timeout for fetching individual CIDs
	FetchTimeout time.Duration
}

// DefaultNodeConfig returns a default configuration
func DefaultNodeConfig() *NodeConfig {
	return &NodeConfig{
		BootstrapPeers:    DefaultBootstrapPeers,
		ListenAddrs:       []string{"/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"},
		ConnectionTimeout: 30 * time.Second,
		FetchTimeout:      60 * time.Second,
	}
}

// Node is an embedded IPFS node for fetching content
type Node struct {
	config    *NodeConfig
	host      host.Host
	dht       *dht.IpfsDHT
	peer      *ipfslite.Peer
	ds        datastore.Batching
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

// NewNode creates and starts a new embedded IPFS node
func NewNode(ctx context.Context, config *NodeConfig) (*Node, error) {
	if config == nil {
		config = DefaultNodeConfig()
	}

	nodeCtx, cancel := context.WithCancel(ctx)

	// Create in-memory datastore
	ds := dssync.MutexWrap(datastore.NewMapDatastore())

	// Generate a new identity
	priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Parse listen addresses
	var listenAddrs []multiaddr.Multiaddr
	for _, addr := range config.ListenAddrs {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to parse listen address %s: %w", addr, err)
		}
		listenAddrs = append(listenAddrs, ma)
	}

	// Create libp2p host with DHT
	var kadDHT *dht.IpfsDHT
	h, err := libp2p.New(
		libp2p.Identity(priv),
		libp2p.ListenAddrs(listenAddrs...),
		libp2p.Routing(func(h host.Host) (routing.PeerRouting, error) {
			var err error
			kadDHT, err = dht.New(nodeCtx, h, dht.Mode(dht.ModeClient))
			return kadDHT, err
		}),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create libp2p host: %w", err)
	}

	// Create IPFS-Lite peer
	peer, err := ipfslite.New(nodeCtx, ds, nil, h, kadDHT, nil)
	if err != nil {
		h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create ipfs-lite peer: %w", err)
	}

	node := &Node{
		config: config,
		host:   h,
		dht:    kadDHT,
		peer:   peer,
		ds:     ds,
		ctx:    nodeCtx,
		cancel: cancel,
	}

	// Bootstrap the DHT
	if err := node.bootstrap(); err != nil {
		node.Close()
		return nil, fmt.Errorf("failed to bootstrap: %w", err)
	}

	log.Printf("IPFS: node started with peer ID %s", h.ID())
	return node, nil
}

// bootstrap connects to the bootstrap peers
func (n *Node) bootstrap() error {
	log.Printf("IPFS: connecting to %d bootstrap peers...", len(n.config.BootstrapPeers))

	// Parse bootstrap peer addresses into AddrInfo
	var bootstrapPeers []peer.AddrInfo
	for _, addr := range n.config.BootstrapPeers {
		ma, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			log.Printf("IPFS: failed to parse bootstrap peer %s: %v", addr, err)
			continue
		}
		addrInfo, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			log.Printf("IPFS: failed to get AddrInfo from %s: %v", addr, err)
			continue
		}
		bootstrapPeers = append(bootstrapPeers, *addrInfo)
	}

	// Connect to bootstrap peers
	n.peer.Bootstrap(bootstrapPeers)

	// Wait a bit for connections to establish
	ctx, cancel := context.WithTimeout(n.ctx, n.config.ConnectionTimeout)
	defer cancel()

	// Wait until we have some connections or timeout
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			connCount := len(n.host.Network().Peers())
			if connCount == 0 {
				return fmt.Errorf("failed to connect to any bootstrap peers")
			}
			log.Printf("IPFS: connected to %d peers", connCount)
			return nil
		case <-ticker.C:
			connCount := len(n.host.Network().Peers())
			if connCount >= 3 {
				log.Printf("IPFS: connected to %d peers", connCount)
				return nil
			}
		}
	}
}

// FetchCAR fetches content by CID and returns it as CAR data
func (n *Node) FetchCAR(cidStr string) ([]byte, error) {
	// Parse the CID
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid CID %s: %w", cidStr, err)
	}

	ctx, cancel := context.WithTimeout(n.ctx, n.config.FetchTimeout)
	defer cancel()

	// Get the DAG service from the peer
	dag := n.peer

	// Fetch the root node
	rootNode, err := dag.Get(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch CID %s: %w", cidStr, err)
	}

	// Collect all blocks in the DAG
	blocks := make(map[cid.Cid][]byte)
	if err := n.collectBlocks(ctx, dag, rootNode, blocks); err != nil {
		return nil, fmt.Errorf("failed to collect blocks for %s: %w", cidStr, err)
	}

	// Create CAR data
	carData, err := n.createCAR(c, blocks)
	if err != nil {
		return nil, fmt.Errorf("failed to create CAR for %s: %w", cidStr, err)
	}

	return carData, nil
}

// collectBlocks recursively collects all blocks in a DAG
func (n *Node) collectBlocks(ctx context.Context, dag ipld.DAGService, node ipld.Node, blocks map[cid.Cid][]byte) error {
	c := node.Cid()
	if _, exists := blocks[c]; exists {
		return nil
	}

	blocks[c] = node.RawData()

	// Recursively fetch linked nodes
	for _, link := range node.Links() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		childNode, err := dag.Get(ctx, link.Cid)
		if err != nil {
			return fmt.Errorf("failed to fetch linked node %s: %w", link.Cid, err)
		}

		if err := n.collectBlocks(ctx, dag, childNode, blocks); err != nil {
			return err
		}
	}

	return nil
}

// createCAR creates CAR format data from collected blocks
func (n *Node) createCAR(root cid.Cid, blockData map[cid.Cid][]byte) ([]byte, error) {
	var buf bytes.Buffer

	// Create CAR writer with root CID
	carWriter, err := storage.NewWritable(&buf, []cid.Cid{root}, car.WriteAsCarV1(true))
	if err != nil {
		return nil, fmt.Errorf("failed to create CAR writer: %w", err)
	}

	// Write all blocks to CAR
	for c, data := range blockData {
		if err := carWriter.Put(n.ctx, c.KeyString(), data); err != nil {
			return nil, fmt.Errorf("failed to write block to CAR: %w", err)
		}
	}

	if err := carWriter.Finalize(); err != nil {
		return nil, fmt.Errorf("failed to finalize CAR: %w", err)
	}

	return buf.Bytes(), nil
}

// FetchRaw fetches raw content by CID (useful for simple files)
func (n *Node) FetchRaw(cidStr string) ([]byte, error) {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid CID %s: %w", cidStr, err)
	}

	ctx, cancel := context.WithTimeout(n.ctx, n.config.FetchTimeout)
	defer cancel()

	reader, err := n.peer.GetFile(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch %s: %w", cidStr, err)
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// PeerCount returns the number of connected peers
func (n *Node) PeerCount() int {
	return len(n.host.Network().Peers())
}

// PeerID returns this node's peer ID
func (n *Node) PeerID() string {
	return n.host.ID().String()
}

// Close shuts down the IPFS node
func (n *Node) Close() error {
	var err error
	n.closeOnce.Do(func() {
		log.Printf("IPFS: shutting down node...")
		n.cancel()

		if n.dht != nil {
			if e := n.dht.Close(); e != nil {
				err = e
			}
		}

		if n.host != nil {
			if e := n.host.Close(); e != nil && err == nil {
				err = e
			}
		}

		if n.ds != nil {
			if e := n.ds.Close(); e != nil && err == nil {
				err = e
			}
		}

		log.Printf("IPFS: node shut down")
	})
	return err
}
