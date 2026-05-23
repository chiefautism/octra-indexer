# octra-indexer

**A cross-platform raw data indexer for Octra mainnet.**

`octra-indexer` downloads Octra chain data from RPC and saves it to local disk as append-only JSONL files. It is meant to be the first reliable layer: get the data, keep it, resume safely, and build search/API/analytics on top later.

## Why This Exists

Explorers and dashboards are slow to build if every question has to hit public RPC again.

An indexer gives you your own local copy of the useful chain data:

- epochs
- transactions by epoch
- optional transaction details
- optional receipts
- staging snapshots

Once that raw data is local, you can build:

- address history
- transaction search
- explorer APIs
- analytics tables
- anomaly checks
- backups independent of a public explorer

The current goal is raw collection, not a finished database/query engine.

## Design

1. **Resume first**: completed epochs get `.done` markers, active epochs use lock directories, and stale locks can be cleaned.
2. **Raw first**: RPC responses are stored as JSONL without over-modeling a young chain too early.
3. **Cross-platform**: the CLI uses Bun processes, PID files, and files under the data directory. No `tmux`, `launchd`, or `systemd` is required.
4. **External storage friendly**: choose any data directory with `--data-dir` or `OCTRA_DATA_DIR`.
5. **Rate-limit aware**: configure workers, global RPS, retries, SOCKS proxies, proxy files, and provider refresh URLs.

## What It Collects

Default RPC:

```text
https://octra.network/rpc
```

Methods:

- `epoch_current`
- `epoch_get`
- `octra_transactionsByEpoch`
- `staging_view`
- `staging_stats`
- `octra_transaction` with `--include-tx-details`
- `contract_receipt` with `--include-receipts`

## Install

```bash
bun install
bun run check
```

## Run In The Foreground

Backfill from genesis and keep following new epochs:

```bash
bun run cli -- run --data-dir="/path/to/octra-data" --from=0 --follow --workers=8 --rps=3.5
```

Run a bounded range:

```bash
bun run cli -- run --data-dir="/path/to/octra-data" --from=921300 --to=921305 --workers=4
```

Use the package shortcut if you only need the raw indexer process:

```bash
bun run start -- --data-dir="/path/to/octra-data" --from=0 --follow --workers=8
```

## Run In The Background

This works the same way on macOS, Linux, and Windows because it is just a detached Bun process plus a PID file.

```bash
bun run cli -- start --data-dir="/path/to/octra-data" --from=0 --workers=8 --rps=3.5
```

Operate it:

```bash
bun run cli -- status --data-dir="/path/to/octra-data"
bun run cli -- progress --data-dir="/path/to/octra-data"
bun run cli -- logs --data-dir="/path/to/octra-data"
bun run cli -- logs --data-dir="/path/to/octra-data" --follow
bun run cli -- restart --data-dir="/path/to/octra-data"
bun run cli -- stop --data-dir="/path/to/octra-data"
```

The background runner writes:

```text
/path/to/octra-data/state/indexer.pid.json
/path/to/octra-data/logs/indexer.log
/path/to/octra-data/logs/indexer.err.log
```

## Proxies

The public RPC is rate limited. In practice, expect about **3-4 requests per second** from one endpoint/IP before `429 Too Many Requests` starts showing up.

If you keep pushing above that limit, the RPC anti-DDoS layer can ban the client temporarily. For faster backfills, use a proxy pool and keep per-proxy RPS conservative.

Recommended approach:

- keep direct RPC around `--rps=3.5`
- rotate proxies for higher aggregate throughput
- cool down failing proxies
- use provider refresh URLs when available
- keep retries/backoff enabled for `429` and network errors

Use one proxy:

```bash
OCTRA_SOCKS_PROXY="socks5://user:pass@host:port" \
  bun run cli -- run --data-dir="/path/to/octra-data" --from=0 --follow
```

Use a proxy file:

```bash
bun run cli -- start \
  --data-dir="/path/to/octra-data" \
  --workers=8 \
  --per-proxy-rps=3 \
  --proxy-file="/path/to/proxies.txt"
```

Proxy file format:

```text
socks5://user:pass@host:port
socks5://user:pass@host:port[https://provider.example/refresh-ip]
```

## Configuration

| Setting | Flag | Environment | Default |
| --- | --- | --- | --- |
| Data directory | `--data-dir` | `OCTRA_DATA_DIR` | `./octra-data` |
| RPC endpoint | n/a | `OCTRA_RPC_URL` | `https://octra.network/rpc` |
| Workers | `--workers` | `OCTRA_WORKERS` | `1` direct, `6` background |
| Global RPS | `--rps` | `OCTRA_RPS` | `3.5` |
| Timeout | `--timeout-ms` | `OCTRA_TIMEOUT_MS` | `20000` direct, `30000` background |
| Retries | `--retries` | `OCTRA_RETRIES` | `4` direct, `8` background |
| Proxy file | `--proxy-file` | `OCTRA_PROXY_FILE` | empty |
| Single proxy | n/a | `OCTRA_SOCKS_PROXY` | empty |

## Storage Layout

```text
/path/to/octra-data/
  raw/
    epochs/
    tx_by_epoch/
    tx_details/
    receipts/
    staging/
  state/
    cursor.json
    epochs/*.done
    locks/*.lock/
    indexer.pid.json
  logs/
    indexer.log
    indexer.err.log
```

Raw files are sharded by epoch range and written as JSONL. This makes the collector easy to stop, copy, replay, and process with separate tools.

## Maintenance

```bash
bun run cli -- doctor --data-dir="/path/to/octra-data"
bun run cli -- locks --data-dir="/path/to/octra-data"
bun run cli -- locks clean --data-dir="/path/to/octra-data" --done
```

## Tests

The test suite is intentionally real integration coverage. It talks to the live Octra RPC, runs the CLI against temporary data directories, writes JSONL files, checks cursor state, and verifies background logs/progress.

```bash
bun run test
```

Because these tests hit public RPC, keep them low-rate and do not run them in a tight loop.

This repository intentionally does not include generated docs, local proxy files, runtime logs, or indexed chain data.
