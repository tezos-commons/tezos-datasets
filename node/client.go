package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"time"
)

// Client is a Tezos node RPC client with round-robin load balancing
type Client struct {
	addresses  []string
	httpClient *http.Client
	counter    uint64
}

// NewClient creates a new Tezos node client with the given addresses
func NewClient(addresses []string) *Client {
	return &Client{
		addresses: addresses,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		counter: 0,
	}
}

// getNextAddress returns the next address using round-robin selection
func (c *Client) getNextAddress() string {
	idx := atomic.AddUint64(&c.counter, 1) - 1
	return c.addresses[idx%uint64(len(c.addresses))]
}

// doRequest performs an HTTP GET request with the given path
func (c *Client) doRequest(ctx context.Context, path string) ([]byte, error) {
	addr := c.getNextAddress()
	var url string
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		url = strings.TrimRight(addr, "/") + path
	} else {
		url = fmt.Sprintf("http://%s%s", addr, path)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

// blockHeader is used for parsing the level from block header response
type blockHeader struct {
	Level int64 `json:"level"`
}

// GetBlockHeight returns the current block height from the chain head
func (c *Client) GetBlockHeight(ctx context.Context) (int64, error) {
	data, err := c.doRequest(ctx, "/chains/main/blocks/head/header")
	if err != nil {
		return 0, err
	}

	var header blockHeader
	if err := json.Unmarshal(data, &header); err != nil {
		return 0, fmt.Errorf("failed to parse block header: %w", err)
	}

	return header.Level, nil
}

// GetBlock returns the raw JSON bytes for a block at the given level
func (c *Client) GetBlock(ctx context.Context, level int64) ([]byte, error) {
	path := fmt.Sprintf("/chains/main/blocks/%d", level)
	return c.doRequest(ctx, path)
}

// GetContractScript returns the raw JSON bytes for a contract's script
func (c *Client) GetContractScript(ctx context.Context, address string) ([]byte, error) {
	path := fmt.Sprintf("/chains/main/blocks/head/context/contracts/%s/script", address)
	return c.doRequest(ctx, path)
}

// Addresses returns the list of node addresses
func (c *Client) Addresses() []string {
	return c.addresses
}
