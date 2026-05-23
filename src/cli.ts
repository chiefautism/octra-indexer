import { mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";

const repoDir = process.cwd();
const defaultDataDir = join(repoDir, "octra-data");
const bunPath = Bun.env.BUN_BIN ?? Bun.argv[0] ?? "bun";

type CommandResult = {
  stdout: string;
  stderr: string;
  exitCode: number;
};

type PidState = {
  pid: number;
  started_at: string;
  data_dir: string;
  args: string[];
};

function usage() {
  const dataDir = resolveDataDir();
  console.log(`octra-indexer

Usage:
  bun run cli -- run [--data-dir=/path/data] [--from=0] [--follow] [--workers=6] [--rps=3.5]
  bun run cli -- start [--data-dir=/path/data] [--from=0] [--workers=6] [--rps=3.5] [--proxy-file=/path/proxies.txt]
  bun run cli -- stop [--data-dir=/path/data]
  bun run cli -- restart [--data-dir=/path/data]
  bun run cli -- status [--data-dir=/path/data]
  bun run cli -- progress [--data-dir=/path/data]
  bun run cli -- materialize [--data-dir=/path/data] [--db-path=/path/octra.sqlite] [--from=0] [--to=1000] [--force]
  bun run cli -- logs [--data-dir=/path/data] [--follow]
  bun run cli -- doctor [--data-dir=/path/data]
  bun run cli -- locks [--data-dir=/path/data]
  bun run cli -- locks clean [--data-dir=/path/data] [--done|--stale-ms=900000|--all]

Defaults:
  data: ${dataDir}
`);
}

function getOpt(name: string, fallback?: string) {
  const prefix = `--${name}=`;
  const arg = Bun.argv.slice(3).find((item) => item.startsWith(prefix));
  return arg ? arg.slice(prefix.length) : fallback;
}

function hasFlag(name: string) {
  return Bun.argv.slice(3).includes(`--${name}`);
}

function resolveDataDir() {
  return getOpt("data-dir", Bun.env.OCTRA_DATA_DIR ?? defaultDataDir) ?? defaultDataDir;
}

function pidPath(dataDir = resolveDataDir()) {
  return join(dataDir, "state", "indexer.pid.json");
}

function outLogPath(dataDir = resolveDataDir()) {
  return join(dataDir, "logs", "indexer.log");
}

function errLogPath(dataDir = resolveDataDir()) {
  return join(dataDir, "logs", "indexer.err.log");
}

function defaultProxyFileFor(dataDir: string) {
  return join(dataDir, "config", "proxies.txt");
}

async function runCommand(cmd: string[], options: { inherit?: boolean; env?: Record<string, string | undefined> } = {}): Promise<CommandResult> {
  const proc = Bun.spawn(cmd, {
    stdout: options.inherit ? "inherit" : "pipe",
    stderr: options.inherit ? "inherit" : "pipe",
    env: options.env,
  });

  if (options.inherit) {
    return { stdout: "", stderr: "", exitCode: await proc.exited };
  }

  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  return { stdout, stderr, exitCode };
}

async function ensureDataDirs(dataDir = resolveDataDir()) {
  await mkdir(join(dataDir, "logs"), { recursive: true });
  await mkdir(join(dataDir, "config"), { recursive: true });
  await mkdir(join(dataDir, "state", "epochs"), { recursive: true });
  await mkdir(join(dataDir, "state", "locks"), { recursive: true });
}

function envForIndexer(dataDir: string) {
  const defaultProxyFile = defaultProxyFileFor(dataDir);
  const proxyFile = getOpt("proxy-file", existsSync(defaultProxyFile) ? defaultProxyFile : "") ?? "";
  const perProxyRps = Number(getOpt("per-proxy-rps", ""));
  const proxyCount = proxyFile && existsSync(proxyFile)
    ? Bun.file(proxyFile)
        .text()
        .then((raw) => raw.split(/\r?\n/).filter((line) => line.trim() && !line.trim().startsWith("#")).length)
    : Promise.resolve(0);

  return proxyCount.then((count) => {
    const computedRps = Number.isFinite(perProxyRps) && perProxyRps > 0 && count > 0
      ? String(Math.max(0.1, perProxyRps * count))
      : undefined;
    return {
      ...Bun.env,
      OCTRA_DATA_DIR: dataDir,
      OCTRA_RPC_URL: Bun.env.OCTRA_RPC_URL ?? "https://octra.network/rpc",
      OCTRA_PROXY_FILE: proxyFile,
      OCTRA_WORKERS: getOpt("workers", Bun.env.OCTRA_WORKERS ?? "6") ?? "6",
      OCTRA_RPS: getOpt("rps", Bun.env.OCTRA_RPS ?? computedRps ?? "3.5") ?? "3.5",
      OCTRA_TIMEOUT_MS: getOpt("timeout-ms", Bun.env.OCTRA_TIMEOUT_MS ?? "30000") ?? "30000",
      OCTRA_RETRIES: getOpt("retries", Bun.env.OCTRA_RETRIES ?? "8") ?? "8",
      BUN_BIN: bunPath,
    };
  });
}

function indexerArgs(mode: "run" | "start") {
  const dataDir = resolveDataDir();
  const args = [`--data-dir=${dataDir}`];
  const passThrough = ["from", "to", "limit", "poll-ms", "lock-ttl-ms", "workers", "rps", "timeout-ms", "retries"];
  for (const name of passThrough) {
    const value = getOpt(name);
    if (value !== undefined) args.push(`--${name}=${value}`);
  }

  for (const name of ["include-tx-details", "include-receipts"]) {
    if (hasFlag(name)) args.push(`--${name}`);
  }

  if (hasFlag("once")) args.push("--once");
  if (hasFlag("follow") || (mode === "start" && !hasFlag("once") && getOpt("to") === undefined)) args.push("--follow");
  return args;
}

function processState(pid: number) {
  try {
    process.kill(pid, 0);
    return "running";
  } catch (error) {
    const code = error && typeof error === "object" && "code" in error ? String((error as { code?: unknown }).code) : "";
    if (code === "EPERM") return "unknown";
    return "stopped";
  }
}

function isProcessRunning(pid: number) {
  return processState(pid) === "running";
}

async function waitForStop(pid: number) {
  for (let i = 0; i < 50; i += 1) {
    await Bun.sleep(200);
    const state = processState(pid);
    if (state === "stopped") {
      return true;
    }
    if (state === "unknown") {
      return false;
    }
  }
  return false;
}

function killProcess(pid: number, signal: NodeJS.Signals) {
  try {
    process.kill(pid, signal);
    return true;
  } catch {
    return false;
  }
}

async function readPidState(dataDir = resolveDataDir()): Promise<PidState | undefined> {
  try {
    return JSON.parse(await readFile(pidPath(dataDir), "utf8")) as PidState;
  } catch {
    return undefined;
  }
}

async function runForeground() {
  const dataDir = resolveDataDir();
  await ensureDataDirs(dataDir);
  const env = await envForIndexer(dataDir);
  const args = indexerArgs("run");
  const result = await runCommand([bunPath, "src/index.ts", ...args], { inherit: true, env });
  process.exit(result.exitCode);
}

async function materialize() {
  const passThrough = ["data-dir", "db-path", "from", "to"];
  const args = passThrough.flatMap((name) => {
    const value = getOpt(name);
    return value === undefined ? [] : [`--${name}=${value}`];
  });
  if (hasFlag("force")) args.push("--force");

  const result = await runCommand([bunPath, "src/materialize.ts", ...args], { inherit: true });
  process.exit(result.exitCode);
}

async function start() {
  const dataDir = resolveDataDir();
  await ensureDataDirs(dataDir);

  const existing = await readPidState(dataDir);
  const existingState = existing ? processState(existing.pid) : "stopped";
  if (existing && existingState !== "stopped") {
    console.log(`status: running`);
    console.log(`pid: ${existing.pid}`);
    return;
  }

  const args = indexerArgs("start");
  const env = await envForIndexer(dataDir);
  const proc = Bun.spawn([bunPath, "src/daemon.ts"], {
    cwd: repoDir,
    detached: true,
    stdio: ["ignore", "ignore", "ignore"],
    env: {
      ...env,
      OCTRA_INDEXER_ARGS: JSON.stringify(args),
      OCTRA_LOG_PATH: outLogPath(dataDir),
      OCTRA_ERR_PATH: errLogPath(dataDir),
      OCTRA_PID_PATH: pidPath(dataDir),
    },
  });
  proc.unref();

  const state: PidState = {
    pid: proc.pid,
    started_at: new Date().toISOString(),
    data_dir: dataDir,
    args,
  };
  await writeFile(pidPath(dataDir), `${JSON.stringify(state, null, 2)}\n`);
  console.log(`started`);
  console.log(`pid: ${proc.pid}`);
  console.log(`data: ${dataDir}`);
}

async function stop() {
  const dataDir = resolveDataDir();
  const state = await readPidState(dataDir);
  if (!state) {
    console.log("status: stopped");
    return;
  }

  const currentState = processState(state.pid);
  if (currentState === "stopped") {
    await rm(pidPath(dataDir), { force: true });
    console.log("status: stopped");
    return;
  }

  if (!killProcess(state.pid, "SIGTERM")) {
    console.log("status: unknown");
    console.log(`pid: ${state.pid}`);
    console.log("could_not_signal_process");
    return;
  }

  if (await waitForStop(state.pid)) {
    await rm(pidPath(dataDir), { force: true });
    console.log("stopped");
    return;
  }

  killProcess(state.pid, "SIGKILL");
  await rm(pidPath(dataDir), { force: true });
  console.log("stopped");
}

async function status() {
  const dataDir = resolveDataDir();
  const state = await readPidState(dataDir);
  const currentState = state ? processState(state.pid) : "stopped";
  if (!state || currentState === "stopped") {
    if (state) await rm(pidPath(dataDir), { force: true });
    console.log("status: stopped");
    console.log(`data: ${dataDir}`);
    return;
  }

  console.log(`status: ${currentState}`);
  console.log(`pid: ${state.pid}`);
  console.log(`started_at: ${state.started_at}`);
  console.log(`data: ${state.data_dir}`);
  console.log(`args: ${state.args.join(" ")}`);
}

async function doneCount(dataDir = resolveDataDir()) {
  try {
    const entries = await readdir(join(dataDir, "state", "epochs"));
    return entries.filter((entry) => entry.endsWith(".done")).length;
  } catch {
    return 0;
  }
}

async function lockEntries(dataDir = resolveDataDir()) {
  try {
    const entries = await readdir(join(dataDir, "state", "locks"));
    return entries.filter((entry) => entry.endsWith(".lock") && !entry.startsWith("._"));
  } catch {
    return [];
  }
}

async function dirSize(path: string): Promise<number> {
  let total = 0;
  let entries;
  try {
    entries = await readdir(path, { withFileTypes: true });
  } catch {
    return 0;
  }

  for (const entry of entries) {
    const entryPath = join(path, entry.name);
    if (entry.isDirectory()) total += await dirSize(entryPath);
    else if (entry.isFile()) total += (await stat(entryPath)).size;
  }
  return total;
}

function formatBytes(value: number) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) {
    size /= 1024;
    unit += 1;
  }
  return `${size.toFixed(unit === 0 ? 0 : 1)}${units[unit]}`;
}

async function progress() {
  const dataDir = resolveDataDir();
  let cursor = "missing";
  try {
    cursor = (await readFile(join(dataDir, "state", "cursor.json"), "utf8")).trim();
  } catch {}

  const done = await doneCount(dataDir);
  const locks = await lockEntries(dataDir);
  const size = await dirSize(dataDir);
  console.log(`done_epochs: ${done}`);
  console.log(`active_locks: ${locks.length}`);
  console.log(`data_size: ${formatBytes(size)}`);
  console.log(`cursor: ${cursor}`);
}

async function printLogs() {
  const dataDir = resolveDataDir();
  const follow = Bun.argv.includes("--follow") || Bun.argv.includes("-f");
  const path = hasFlag("err") ? errLogPath(dataDir) : outLogPath(dataDir);
  let lastSize = 0;

  while (true) {
    let raw = "";
    try {
      raw = await readFile(path, "utf8");
    } catch {
      raw = "";
    }

    if (follow) {
      const next = raw.slice(lastSize);
      if (next) process.stdout.write(next);
      lastSize = raw.length;
      await Bun.sleep(1000);
      continue;
    }

    console.log(raw.split(/\r?\n/).slice(-80).join("\n"));
    return;
  }
}

async function doctor() {
  const dataDir = resolveDataDir();
  const dataParent = dirname(dataDir);
  const checks: Array<[string, boolean, string]> = [];
  const bunCheck = await runCommand([bunPath, "--version"]).catch(() => ({ exitCode: 1, stdout: "", stderr: "" }));
  checks.push(["bun_runnable", bunCheck.exitCode === 0, bunPath]);
  checks.push(["repo_exists", existsSync(repoDir), repoDir]);
  checks.push(["data_parent_exists", existsSync(dataParent), dataParent]);
  checks.push(["data_dir_exists", existsSync(dataDir), dataDir]);

  for (const [name, ok, detail] of checks) {
    console.log(`${ok ? "ok" : "fail"} ${name} ${detail}`);
  }

  const response = await fetch("https://octra.network/rpc", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "epoch_current", params: [] }),
  }).catch(() => undefined);
  const text = response ? await response.text() : "";
  console.log(`${response?.ok && text.includes("epoch_id") ? "ok" : "fail"} rpc_epoch_current`);
}

async function locks() {
  const dataDir = resolveDataDir();
  const entries = await lockEntries(dataDir);
  if (Bun.argv[3] === "clean") {
    const all = Bun.argv.includes("--all");
    const doneOnly = Bun.argv.includes("--done");
    const staleMs = Number(getOpt("stale-ms", "900000"));
    let removed = 0;
    for (const entry of entries) {
      const path = join(dataDir, "state", "locks", entry);
      let lockStat;
      try {
        lockStat = await stat(path);
      } catch {
        continue;
      }
      const epoch = entry.replace(/\.lock$/, "");
      const hasDoneMarker = existsSync(join(dataDir, "state", "epochs", `${epoch}.done`));
      if (all || (doneOnly && hasDoneMarker) || (!doneOnly && Date.now() - lockStat.mtimeMs > staleMs)) {
        await rm(path, { recursive: true, force: true });
        removed += 1;
      }
    }
    console.log(`removed_locks: ${removed}`);
    return;
  }

  console.log(`locks: ${entries.length}`);
  for (const entry of entries.slice(0, 20)) console.log(entry);
  if (entries.length > 20) console.log(`... ${entries.length - 20} more`);
}

async function main() {
  const command = Bun.argv[2] ?? "help";
  if (command === "run") return runForeground();
  if (command === "start") return start();
  if (command === "stop") return stop();
  if (command === "restart") {
    await stop().catch(() => {});
    return start();
  }
  if (command === "status") return status();
  if (command === "progress") return progress();
  if (command === "materialize") return materialize();
  if (command === "logs") return printLogs();
  if (command === "doctor") return doctor();
  if (command === "locks") return locks();
  usage();
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
