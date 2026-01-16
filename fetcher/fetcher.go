package fetcher

import (
	"context"
	"log"
	"sync"
	"time"

	"tezos-datasets/node"
)

// BlockTodo represents a block to be fetched
type BlockTodo struct {
	Level int64
	Data  []byte
	Err   error
	wg    sync.WaitGroup
}

// FetcherPool manages a pool of workers that fetch blocks from Tezos nodes
type FetcherPool struct {
	client     *node.Client
	numWorkers int
	todoChan   chan *BlockTodo
	ctx        context.Context
	cancel     context.CancelFunc
	workerWg   sync.WaitGroup
}

// NewFetcherPool creates a new fetcher pool with the specified number of workers
func NewFetcherPool(client *node.Client, numWorkers int) *FetcherPool {
	ctx, cancel := context.WithCancel(context.Background())
	return &FetcherPool{
		client:     client,
		numWorkers: numWorkers,
		todoChan:   make(chan *BlockTodo, numWorkers*2),
		ctx:        ctx,
		cancel:     cancel,
	}
}

// Start starts the worker goroutines
func (p *FetcherPool) Start() {
	for i := 0; i < p.numWorkers; i++ {
		p.workerWg.Add(1)
		go p.worker(i)
	}
}

// Stop stops all workers and waits for them to finish
func (p *FetcherPool) Stop() {
	p.cancel()
	close(p.todoChan)
	p.workerWg.Wait()
}

// worker is the main worker loop
func (p *FetcherPool) worker(id int) {
	defer p.workerWg.Done()

	for todo := range p.todoChan {
		p.fetchBlock(todo)
	}
}

// fetchBlock fetches a block with retry logic
func (p *FetcherPool) fetchBlock(todo *BlockTodo) {
	defer todo.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			todo.Err = p.ctx.Err()
			return
		default:
		}

		// Create a context with 10s timeout for this attempt
		ctx, cancel := context.WithTimeout(p.ctx, 10*time.Second)
		data, err := p.client.GetBlock(ctx, todo.Level)
		cancel()

		if err == nil {
			todo.Data = data
			return
		}

		// Log the error and retry
		log.Printf("Failed to fetch block %d: %v, retrying...", todo.Level, err)

		// Wait before retrying
		select {
		case <-p.ctx.Done():
			todo.Err = p.ctx.Err()
			return
		case <-time.After(time.Second):
		}
	}
}

// Submit submits a block to be fetched
func (p *FetcherPool) Submit(todo *BlockTodo) {
	todo.wg.Add(1)
	p.todoChan <- todo
}

// FetchBatch fetches a batch of blocks and returns them in order
func (p *FetcherPool) FetchBatch(startLevel, endLevel int64) ([]*BlockTodo, error) {
	todos := make([]*BlockTodo, 0, endLevel-startLevel)

	// Submit all blocks
	for level := startLevel; level < endLevel; level++ {
		todo := &BlockTodo{Level: level}
		p.Submit(todo)
		todos = append(todos, todo)
	}

	// Wait for all blocks to be fetched (in order)
	for _, todo := range todos {
		todo.wg.Wait()
		if todo.Err != nil {
			return nil, todo.Err
		}
	}

	return todos, nil
}
