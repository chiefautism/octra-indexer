import { appendFile, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

type Json = null | boolean | number | string | Json[] | { [key: string]: Json };

type RpcResponse<T> = {
  jsonrpc?: string;
  id?: number;
  result?: T;
  error?: { code?: number; message?: string; data?: Json };
};

type EpochCurrent = {
  epoch_id?: number;
  roots?: number;
};

type Cursor = {
  last_completed_epoch: number;
  updated_at: string;
};

type ProxyEntry = {
  id: string;
  url: string;
  refreshUrl?: string;
  cooldownUntil: number;
  failures: number;
};

type Config = {
  rpcUrl: string;
  socksProxy?: ProxyEntry;
  socksProxies: ProxyEntry[];
  dataDir: string;
  fromEpoch?: number;
  toEpoch?: number;
  once: boolean;
  follow: boolean;
  includeTxDetails: boolean;
  includeReceipts: boolean;
  limit: number;
  pollMs: number;
  timeoutMs: number;
  retries: number;
  workers: number;
  lockTtlMs: number;
  rps: number;
};

const defaultDataDir = join(process.cwd(), "octra-data");

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
let nextRpcAt = 0;
let nextProxyIndex = 0;

function parseSocksProxy(raw: string | undefined): ProxyEntry | undefined {
  if (!raw?.trim()) return undefined;

  const refreshUrl = raw.match(/\[(https?:\/\/[^\]]+)\]/)?.[1];
  const withoutRefreshUrl = raw.trim().split("[", 1)[0]?.trim() ?? "";
  const match = withoutRefreshUrl.match(/^(socks5h?|socks4a?):\/\/(.+@)?([^:/\s]+):(\d+)/i);
  if (!match) {
    throw new Error("OCTRA_SOCKS_PROXY must look like socks5://user:pass@host:port");
  }

  const auth = match[2] ?? "";
  const host = match[3];
  const port = match[4];
  return {
    id: `${host}:${port}`,
    url: `socks5h://${auth}${host}:${port}`,
    refreshUrl,
    cooldownUntil: 0,
    failures: 0,
  };
}

async function loadSocksProxies(path: string | undefined) {
  if (!path?.trim()) return [];
  const raw = await readFile(path, "utf8");
  return raw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line && !line.startsWith("#"))
    .map((line) => parseSocksProxy(line))
    .filter((proxy): proxy is ProxyEntry => Boolean(proxy));
}

async function parseArgs(): Promise<Config> {
  const args = new Map<string, string | boolean>();
  for (const arg of Bun.argv.slice(2)) {
    if (!arg.startsWith("--")) continue;
    const [key = "", value] = arg.slice(2).split("=", 2);
    if (!key) continue;
    args.set(key, value ?? true);
  }

  const num = (name: string, fallback?: number) => {
    const envName = `OCTRA_${name.toUpperCase().replace(/-/g, "_")}`;
    const raw = args.get(name) ?? Bun.env[envName];
    if (raw === undefined || raw === true) return fallback;
    const value = Number(raw);
    if (!Number.isFinite(value)) throw new Error(`Invalid numeric value for ${name}: ${raw}`);
    return value;
  };
  const str = (name: string, fallback?: string) => {
    const envName = `OCTRA_${name.toUpperCase().replace(/-/g, "_")}`;
    const raw = args.get(name) ?? Bun.env[envName];
    if (raw === undefined || raw === true) return fallback;
    return String(raw);
  };

  const proxyFile = String(Bun.env.OCTRA_PROXY_FILE ?? "");
  const socksProxy = parseSocksProxy(Bun.env.OCTRA_SOCKS_PROXY);
  const socksProxies = await loadSocksProxies(proxyFile);
  if (socksProxy) socksProxies.unshift(socksProxy);

  return {
    rpcUrl: String(Bun.env.OCTRA_RPC_URL ?? "https://octra.network/rpc"),
    socksProxy,
    socksProxies,
    dataDir: str("data-dir", defaultDataDir) ?? defaultDataDir,
    fromEpoch: num("from"),
    toEpoch: num("to"),
    once: args.has("once"),
    follow: args.has("follow"),
    includeTxDetails: args.has("include-tx-details"),
    includeReceipts: args.has("include-receipts"),
    limit: num("limit", 500) ?? 500,
    pollMs: num("poll-ms", 10_000) ?? 10_000,
    timeoutMs: num("timeout-ms", 20_000) ?? 20_000,
    retries: num("retries", 4) ?? 4,
    workers: Math.max(1, num("workers", 1) ?? 1),
    lockTtlMs: num("lock-ttl-ms", 15 * 60_000) ?? 15 * 60_000,
    rps: Math.max(0.1, num("rps", 3.5) ?? 3.5),
  };
}

async function ensureExternalDataDir(dataDir: string) {
  await mkdir(dataDir, { recursive: true });
  await mkdir(join(dataDir, "raw", "epochs"), { recursive: true });
  await mkdir(join(dataDir, "raw", "tx_by_epoch"), { recursive: true });
  await mkdir(join(dataDir, "raw", "tx_details"), { recursive: true });
  await mkdir(join(dataDir, "raw", "receipts"), { recursive: true });
  await mkdir(join(dataDir, "raw", "staging"), { recursive: true });
  await mkdir(join(dataDir, "state"), { recursive: true });
  await mkdir(join(dataDir, "state", "epochs"), { recursive: true });
  await mkdir(join(dataDir, "state", "locks"), { recursive: true });
}

async function rpc<T>(config: Config, method: string, params: Json[] = []): Promise<T> {
  let lastError: unknown;

  for (let attempt = 0; attempt <= config.retries; attempt += 1) {
    let usedProxy: ProxyEntry | undefined;
    try {
      await throttleRpc(config);
      usedProxy = await nextProxy(config);
      const body = usedProxy
        ? await rpcViaCurl<T>(config, method, params, usedProxy.url)
        : await rpcViaFetch<T>(config, method, params);
      if (usedProxy) markProxySuccess(usedProxy);
      if (body.error) {
        throw new Error(`RPC ${method} failed: ${body.error.message ?? JSON.stringify(body.error)}`);
      }
      if (body.result === undefined) {
        throw new Error(`RPC ${method} returned no result`);
      }
      return body.result;
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (usedProxy && isProxyFailure(message)) await markProxyFailure(usedProxy, message);
      lastError =
        error instanceof Error && error.name === "AbortError"
          ? new Error(`RPC ${method} timed out after ${config.timeoutMs}ms`)
          : new Error(`RPC ${method} failed: ${message}`);
      const isRateLimit = message.includes("429") || message.includes("Too Many");
      const backoff = isRateLimit ? Math.min(60_000, 5_000 * 2 ** attempt) : Math.min(20_000, 500 * 2 ** attempt);
      if (attempt < config.retries) await sleep(backoff);
    }
  }

  throw lastError instanceof Error ? lastError : new Error(String(lastError));
}

async function nextProxy(config: Config) {
  if (config.socksProxies.length === 0) return undefined;
  const now = Date.now();
  for (let i = 0; i < config.socksProxies.length; i += 1) {
    const proxy = config.socksProxies[nextProxyIndex % config.socksProxies.length]!;
    nextProxyIndex += 1;
    if (proxy.cooldownUntil <= now) {
      return proxy;
    }
  }

  const soonest = config.socksProxies.reduce((a, b) => (a.cooldownUntil < b.cooldownUntil ? a : b));
  const waitMs = Math.max(0, soonest.cooldownUntil - Date.now());
  if (waitMs > 0) await sleep(waitMs);
  return soonest;
}

function isProxyFailure(message: string) {
  return (
    message.includes("502 Bad Gateway") ||
    message.includes("SOCKS5") ||
    message.includes("SSL_ERROR_SYSCALL") ||
    message.includes("Connection timed out") ||
    message.includes("curl exited 28") ||
    message.includes("curl exited 35") ||
    message.includes("curl exited 97")
  );
}

function markProxySuccess(proxy: ProxyEntry) {
  proxy.failures = 0;
  proxy.cooldownUntil = 0;
}

async function markProxyFailure(proxy: ProxyEntry, message: string) {
  proxy.failures += 1;
  const cooldownMs = Math.min(120_000, 10_000 * proxy.failures);
  proxy.cooldownUntil = Date.now() + cooldownMs;
  console.error(`proxy cooldown id=${proxy.id} failures=${proxy.failures} cooldown_ms=${cooldownMs} reason=${summarizeProxyError(message)}`);
  if (proxy.refreshUrl) {
    await refreshProxy(proxy).catch((error) => {
      console.error(`proxy refresh failed id=${proxy.id} error=${error instanceof Error ? error.message : String(error)}`);
    });
  }
}

function summarizeProxyError(message: string) {
  return message.replace(/\s+/g, " ").slice(0, 160);
}

async function refreshProxy(proxy: ProxyEntry) {
  if (!proxy.refreshUrl) return;
  const response = await fetch(proxy.refreshUrl, { method: "GET" });
  if (!response.ok) throw new Error(`HTTP ${response.status}`);
  console.error(`proxy refresh requested id=${proxy.id}`);
}

async function throttleRpc(config: Config) {
  const intervalMs = 1000 / config.rps;
  const now = Date.now();
  const waitMs = Math.max(0, nextRpcAt - now);
  nextRpcAt = Math.max(now, nextRpcAt) + intervalMs;
  if (waitMs > 0) await sleep(waitMs);
}

async function rpcViaFetch<T>(config: Config, method: string, params: Json[]): Promise<RpcResponse<T>> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.timeoutMs);

  try {
    const response = await fetch(config.rpcUrl, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
      signal: controller.signal,
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status} ${response.statusText}`);
    }

    return (await response.json()) as RpcResponse<T>;
  } finally {
    clearTimeout(timer);
  }
}

async function rpcViaCurl<T>(config: Config, method: string, params: Json[], proxy: string): Promise<RpcResponse<T>> {
  const payload = JSON.stringify({ jsonrpc: "2.0", id: 1, method, params });
  const proc = Bun.spawn(
    [
      "curl",
      "--silent",
      "--show-error",
      "--max-time",
      String(Math.ceil(config.timeoutMs / 1000)),
      "--proxy",
      proxy,
      "-X",
      "POST",
      config.rpcUrl,
      "-H",
      "Content-Type: application/json",
      "--data",
      payload,
    ],
    {
      stdout: "pipe",
      stderr: "pipe",
    },
  );

  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);

  if (exitCode !== 0) {
    throw new Error(`curl exited ${exitCode}: ${stderr.trim() || stdout.trim() || "no output"}`);
  }

  try {
    return JSON.parse(stdout) as RpcResponse<T>;
  } catch {
    throw new Error(`curl returned non-JSON response: ${stdout.slice(0, 300)}`);
  }
}

function shardFile(prefix: string, epoch: number) {
  const start = Math.floor(epoch / 10_000) * 10_000;
  const end = start + 9_999;
  return `${prefix}-${String(start).padStart(9, "0")}-${String(end).padStart(9, "0")}.jsonl`;
}

async function appendJsonl(dataDir: string, relativePath: string, value: unknown) {
  const path = join(dataDir, relativePath);
  await mkdir(dirname(path), { recursive: true });
  await appendFile(path, `${JSON.stringify(value)}\n`);
}

async function readCursor(dataDir: string): Promise<Cursor | undefined> {
  const path = join(dataDir, "state", "cursor.json");
  try {
    const raw = await readFile(path, "utf8");
    return JSON.parse(raw) as Cursor;
  } catch {
    return undefined;
  }
}

async function writeCursor(dataDir: string, lastCompletedEpoch: number) {
  const path = join(dataDir, "state", "cursor.json");
  const cursor: Cursor = {
    last_completed_epoch: lastCompletedEpoch,
    updated_at: new Date().toISOString(),
  };
  await writeFile(path, `${JSON.stringify(cursor, null, 2)}\n`);
}

async function alreadyDumped(dataDir: string, marker: string) {
  try {
    await stat(join(dataDir, marker));
    return true;
  } catch {
    return false;
  }
}

async function writeMarker(dataDir: string, marker: string) {
  const path = join(dataDir, marker);
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${new Date().toISOString()}\n`);
}

async function claimEpoch(config: Config, epoch: number) {
  const dataDir = config.dataDir;
  const done = `state/epochs/${epoch}.done`;
  if (await alreadyDumped(dataDir, done)) {
    await releaseEpoch(dataDir, epoch);
    return false;
  }

  const lockPath = join(dataDir, "state", "locks", `${epoch}.lock`);
  try {
    await mkdir(lockPath);
    await writeFile(join(lockPath, "claimed_at"), `${new Date().toISOString()}\n`);
    return true;
  } catch {
    try {
      const lockStat = await stat(lockPath);
      if (Date.now() - lockStat.mtimeMs > config.lockTtlMs) {
        await rm(lockPath, { recursive: true, force: true });
        await mkdir(lockPath);
        await writeFile(join(lockPath, "claimed_at"), `${new Date().toISOString()}\n`);
        return true;
      }
    } catch {
      return false;
    }
    return false;
  }
}

async function releaseEpoch(dataDir: string, epoch: number) {
  await rm(join(dataDir, "state", "locks", `${epoch}.lock`), { recursive: true, force: true });
}

function extractTxHashes(response: unknown): string[] {
  const hashes: string[] = [];
  const obj = response as { transactions?: unknown; rejected?: unknown };
  for (const rows of [obj.transactions, obj.rejected]) {
    if (!Array.isArray(rows)) continue;
    for (const row of rows) {
      if (row && typeof row === "object") {
        const hash = (row as { hash?: unknown; tx_hash?: unknown }).hash ?? (row as { tx_hash?: unknown }).tx_hash;
        if (typeof hash === "string" && hash.length >= 32) hashes.push(hash);
      }
    }
  }
  return [...new Set(hashes)];
}

async function dumpEpoch(config: Config, epoch: number) {
  const epochMarker = `state/epochs/${epoch}.done`;
  if (await alreadyDumped(config.dataDir, epochMarker)) {
    await writeCursor(config.dataDir, epoch);
    return;
  }

  const epochSummary = await rpc<unknown>(config, "epoch_get", [epoch]);
  await appendJsonl(config.dataDir, join("raw", "epochs", shardFile("epochs", epoch)), {
    type: "epoch_get",
    epoch,
    fetched_at: new Date().toISOString(),
    result: epochSummary,
  });

  let offset = 0;
  let hasMore = true;
  const txHashes: string[] = [];

  while (hasMore) {
    const result = await rpc<unknown>(config, "octra_transactionsByEpoch", [epoch, config.limit, offset]);
    await appendJsonl(config.dataDir, join("raw", "tx_by_epoch", shardFile("tx_by_epoch", epoch)), {
      type: "octra_transactionsByEpoch",
      epoch,
      limit: config.limit,
      offset,
      fetched_at: new Date().toISOString(),
      result,
    });

    txHashes.push(...extractTxHashes(result));
    const typed = result as { has_more?: unknown; count?: unknown };
    hasMore = typed.has_more === true;
    const count = typeof typed.count === "number" ? typed.count : config.limit;
    offset += count > 0 ? count : config.limit;
    if (count === 0) break;
  }

  if (config.includeTxDetails || config.includeReceipts) {
    for (const hash of [...new Set(txHashes)]) {
      if (config.includeTxDetails) {
        const detail = await rpc<unknown>(config, "octra_transaction", [hash]);
        await appendJsonl(config.dataDir, join("raw", "tx_details", shardFile("tx_details", epoch)), {
          type: "octra_transaction",
          epoch,
          hash,
          fetched_at: new Date().toISOString(),
          result: detail,
        });
      }

      if (config.includeReceipts) {
        try {
          const receipt = await rpc<unknown>(config, "contract_receipt", [hash]);
          await appendJsonl(config.dataDir, join("raw", "receipts", shardFile("receipts", epoch)), {
            type: "contract_receipt",
            epoch,
            hash,
            fetched_at: new Date().toISOString(),
            result: receipt,
          });
        } catch (error) {
          await appendJsonl(config.dataDir, join("raw", "receipts", shardFile("receipt_errors", epoch)), {
            type: "contract_receipt_error",
            epoch,
            hash,
            fetched_at: new Date().toISOString(),
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
    }
  }

  await writeMarker(config.dataDir, epochMarker);
  await writeCursor(config.dataDir, epoch);
  console.log(`dumped epoch=${epoch} tx_hashes=${new Set(txHashes).size}`);
}

async function dumpClaimedEpoch(config: Config, epoch: number, workerId: number) {
  const claimed = await claimEpoch(config, epoch);
  if (!claimed) return;

  try {
    await dumpEpoch(config, epoch);
    await releaseEpoch(config.dataDir, epoch);
    console.log(`worker=${workerId} done epoch=${epoch}`);
  } catch (error) {
    await releaseEpoch(config.dataDir, epoch);
    throw error;
  }
}

async function dumpStaging(config: Config) {
  const ts = new Date();
  const file = `${ts.toISOString().slice(0, 10)}.jsonl`;
  const [view, stats] = await Promise.allSettled([
    rpc<unknown>(config, "staging_view", []),
    rpc<unknown>(config, "staging_stats", []),
  ]);

  await appendJsonl(config.dataDir, join("raw", "staging", file), {
    type: "staging_snapshot",
    fetched_at: ts.toISOString(),
    staging_view: view.status === "fulfilled" ? view.value : { error: String(view.reason) },
    staging_stats: stats.status === "fulfilled" ? stats.value : { error: String(stats.reason) },
  });
}

async function getCurrentEpoch(config: Config) {
  const current = await rpc<EpochCurrent>(config, "epoch_current", []);
  if (typeof current.epoch_id !== "number") {
    throw new Error(`epoch_current did not return epoch_id: ${JSON.stringify(current)}`);
  }
  return current.epoch_id;
}

async function waitForCurrentEpoch(config: Config, label: string) {
  while (true) {
    try {
      return await getCurrentEpoch(config);
    } catch (error) {
      console.error(`${label}_current_epoch_error=${error instanceof Error ? error.message : String(error)}`);
      await sleep(Math.min(60_000, config.pollMs * 2));
    }
  }
}

async function run() {
  const config = await parseArgs();
  await ensureExternalDataDir(config.dataDir);

  const cursor = await readCursor(config.dataDir);
  let nextEpoch = config.fromEpoch ?? ((cursor?.last_completed_epoch ?? -1) + 1);

  console.log(`rpc=${config.rpcUrl}`);
  console.log(`proxy=${config.socksProxies.length > 0 ? `enabled count=${config.socksProxies.length}` : "disabled"}`);
  console.log(`data=${config.dataDir}`);
  console.log(`next_epoch=${nextEpoch}`);
  console.log(`workers=${config.workers}`);
  console.log(`rps=${config.rps}`);

  if (config.workers > 1) {
    await runParallel(config, nextEpoch);
    return;
  }

  while (true) {
    const currentEpoch = config.toEpoch ?? (await waitForCurrentEpoch(config, "single"));
    while (nextEpoch <= currentEpoch) {
      await dumpEpoch(config, nextEpoch);
      nextEpoch += 1;
    }

    await dumpStaging(config);

    if (config.once || config.toEpoch !== undefined || !config.follow) break;
    await sleep(config.pollMs);
  }
}

async function runParallel(config: Config, fromEpoch: number) {
  let highWater = config.toEpoch ?? (await waitForCurrentEpoch(config, "parallel"));
  let next = fromEpoch;
  const retryQueue: number[] = [];

  const worker = async (workerId: number) => {
    while (true) {
      const epoch = retryQueue.shift() ?? next;
      if (epoch === next) next += 1;

      if (epoch > highWater) {
        if (config.toEpoch !== undefined || config.once || !config.follow) return;
        await dumpStaging(config).catch((error) => {
          console.error(`worker=${workerId} staging_error=${error instanceof Error ? error.message : String(error)}`);
        });
        await sleep(config.pollMs);
        try {
          highWater = await waitForCurrentEpoch(config, `worker=${workerId}`);
        } catch (error) {
          console.error(`worker=${workerId} high_water_error=${error instanceof Error ? error.message : String(error)}`);
          await sleep(Math.min(60_000, config.pollMs * 2));
        }
        continue;
      }

      try {
        await dumpClaimedEpoch(config, epoch, workerId);
      } catch (error) {
        console.error(`worker=${workerId} retry epoch=${epoch} error=${error instanceof Error ? error.message : String(error)}`);
        retryQueue.push(epoch);
        await sleep(Math.min(30_000, 2_000 + retryQueue.length * 250));
      }
    }
  };

  await Promise.all(Array.from({ length: config.workers }, (_, i) => worker(i + 1)));
}

run().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : error);
  process.exit(1);
});
