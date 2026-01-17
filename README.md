# tezos-datasets

A CLI tool that scans Tezos blocks and generates datasets for archival and analysis.

## Datasets

### Blocks (`--blocks`)

Stores raw Tezos block JSON data as LZ4 compressed files, organized in batches of 100 blocks.

**Output:** `output/blocks/`

**Format:**
- Filename: `{start_level}-{end_level}.lz4` (e.g., `0-100.lz4`)
- Content: LZ4-compressed (level 9) newline-delimited JSON (NDJSON)
- Each line is a compacted JSON block from the Tezos RPC

```bash
# Decompress and view
lz4 -d 0-100.lz4 | head -1 | jq .
```

### Contracts (`--contracts`)

SQLite database containing contract scripts extracted from block operations.

**Output:** `output/contracts/contracts.sqlite`

**Schema:**
```sql
CREATE TABLE contracts (
    address TEXT PRIMARY KEY,
    script BLOB
);
```

| Column | Type | Description |
|--------|------|-------------|
| address | TEXT | Contract address (e.g., KT1...) |
| script | BLOB | Raw Micheline script JSON |

### IPFS Metadata (`--ipfs-metadata`)

Discovers and fetches IPFS content referenced in Tezos blocks. The tool scans for:
- `ipfs://` URIs
- IPFS gateway URLs (ipfs.io, pinata, cloudflare-ipfs, dweb.link)
- Raw CIDs (Qm... and bafy... formats)
- Hex-encoded IPFS references

Content is fetched via an embedded IPFS node and stored in CAR (Content Addressable aRchive) format.

**Output:** `output/ipfs/metadata.sqlite`

**Schema:**
```sql
CREATE TABLE metadata (
    cid TEXT PRIMARY KEY,
    content_length INTEGER DEFAULT 0,
    data BLOB
);
```

| Column | Type | Description |
|--------|------|-------------|
| cid | TEXT | Content identifier (Qm... or bafy...) |
| content_length | INTEGER | Size in bytes. 0 = pending fetch |
| data | BLOB | CAR format data. NULL if pending or >512KB |

**Status interpretation:**
- `content_length = 0, data IS NULL` → Pending fetch
- `content_length > 0, data IS NULL` → Fetched but too large (>512KB)
- `content_length > 0, data IS NOT NULL` → Fetched and stored

## Building

Requires Go 1.22+, GCC (for SQLite), and Git.

```bash
git clone https://github.com/tezos-commons/tezos-datasets.git
cd tezos-datasets
go build -o tezos-datasets .
```

## Usage

```bash
# Basic usage - scan blocks and extract IPFS metadata
./tezos-datasets --node localhost:8732 --blocks --ipfs-metadata

# Multiple Tezos nodes with port range
./tezos-datasets --node localhost:8730-8750 --blocks --contracts --ipfs-metadata

# All options
./tezos-datasets \
  --node localhost:8732 \
  --fetchers 16 \
  --output-dir ./output \
  --blocks \
  --contracts \
  --ipfs-metadata \
  --start-from 0
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--node` | localhost:8732 | Tezos node address(es). Supports port ranges (e.g., `localhost:8730-8750`) |
| `--fetchers`, `-n` | 16 | Number of block fetcher workers |
| `--output-dir` | ./output | Output directory for datasets |
| `--blocks` | false | Enable blocks dataset |
| `--contracts` | false | Enable contracts dataset |
| `--ipfs-metadata` | false | Enable IPFS metadata dataset |
| `--ipfs-bootstrap` | (default peers) | Custom IPFS bootstrap peers |
| `--start-from` | -1 | Start from specific block level (-1 = resume from saved state) |

## Network Requirements

### Tezos Node

The tool requires access to a Tezos node RPC endpoint (default port 8732).

### IPFS (when using `--ipfs-metadata`)

The embedded IPFS node uses libp2p for peer-to-peer communication:

- **Outbound TCP:** Required for connecting to IPFS peers (random high ports)
- **Outbound UDP:** Used for DHT discovery
- **Inbound (optional):** Not required but improves connectivity. The node listens on random TCP ports.

No specific ports need to be opened for basic operation - the node works in client mode and initiates all connections. For better performance behind NAT/firewall, ensure outbound connections to ports 4001 (default IPFS) and high ports (1024-65535) are allowed.

## State and Resume

The tool saves progress to `output/last_block_n` and automatically resumes from the last processed block on restart. Use `--start-from` to override.

## License

MIT
