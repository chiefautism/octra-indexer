import { afterAll, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtemp, readFile, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

setDefaultTimeout(45_000);

const repoDir = process.cwd();
const bun = process.execPath;
const rpcUrl = Bun.env.OCTRA_RPC_URL ?? "https://octra.network/rpc";
const tempDirs: string[] = [];

type CommandResult = {
  stdout: string;
  stderr: string;
  exitCode: number;
};

type EpochCurrentResponse = {
  result?: {
    epoch_id?: number;
  };
};

async function tempDataDir() {
  const dir = await mkdtemp(join(tmpdir(), "octra-indexer-test-"));
  tempDirs.push(dir);
  return dir;
}

async function runCli(args: string[], timeoutMs = 35_000): Promise<CommandResult> {
  const proc = Bun.spawn([bun, "src/cli.ts", ...args], {
    cwd: repoDir,
    stdout: "pipe",
    stderr: "pipe",
    env: {
      ...Bun.env,
      OCTRA_RPC_URL: rpcUrl,
    },
  });

  const timeout = setTimeout(() => proc.kill("SIGKILL"), timeoutMs);
  const [stdout, stderr, exitCode] = await Promise.all([
    new Response(proc.stdout).text(),
    new Response(proc.stderr).text(),
    proc.exited,
  ]);
  clearTimeout(timeout);

  return { stdout, stderr, exitCode };
}

async function readJsonl(path: string) {
  const raw = await readFile(path, "utf8");
  return raw
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

async function expectFile(path: string) {
  const fileStat = await stat(path);
  expect(fileStat.isFile()).toBe(true);
  expect(fileStat.size).toBeGreaterThan(0);
}

async function currentEpoch() {
  const response = await fetch(rpcUrl, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "epoch_current", params: [] }),
  });
  expect(response.ok).toBe(true);

  const body = (await response.json()) as EpochCurrentResponse;
  const epochId = body.result?.epoch_id;
  expect(typeof epochId).toBe("number");
  if (typeof epochId !== "number") throw new Error("epoch_current did not return numeric epoch_id");
  return epochId;
}

function randomEpochs(count: number, maxEpoch: number) {
  const upperBound = Math.max(1, maxEpoch - 10);
  const epochs = new Set<number>();
  while (epochs.size < count) {
    epochs.add(1 + Math.floor(Math.random() * upperBound));
  }
  return [...epochs].sort((a, b) => a - b);
}

function epochShard(prefix: string, epoch: number) {
  const start = Math.floor(epoch / 10_000) * 10_000;
  const end = start + 9_999;
  return `${prefix}-${String(start).padStart(9, "0")}-${String(end).padStart(9, "0")}.jsonl`;
}

afterAll(async () => {
  await Promise.all(tempDirs.map((dir) => rm(dir, { recursive: true, force: true })));
});

test("real Octra RPC returns current epoch", async () => {
  const epochId = await currentEpoch();
  expect(epochId).toBeGreaterThan(0);
});

test("real foreground index run writes epoch data and cursor", async () => {
  const dataDir = await tempDataDir();
  const result = await runCli([
    "run",
    `--data-dir=${dataDir}`,
    "--from=1",
    "--to=1",
    "--workers=1",
    "--rps=1",
    "--timeout-ms=15000",
    "--retries=2",
  ]);

  expect(result.exitCode).toBe(0);
  expect(result.stdout).toContain("dumped epoch=1");

  await expectFile(join(dataDir, "state", "epochs", "1.done"));
  const cursor = JSON.parse(await readFile(join(dataDir, "state", "cursor.json"), "utf8"));
  expect(cursor.last_completed_epoch).toBe(1);

  const epochRows = await readJsonl(join(dataDir, "raw", "epochs", "epochs-000000000-000009999.jsonl"));
  expect(epochRows.some((row) => row.type === "epoch_get" && row.epoch === 1)).toBe(true);

  const txRows = await readJsonl(join(dataDir, "raw", "tx_by_epoch", "tx_by_epoch-000000000-000009999.jsonl"));
  expect(txRows.some((row) => row.type === "octra_transactionsByEpoch" && row.epoch === 1)).toBe(true);
});

test("real background run writes logs and progress", async () => {
  const dataDir = await tempDataDir();
  const start = await runCli([
    "start",
    `--data-dir=${dataDir}`,
    "--from=1",
    "--to=1",
    "--workers=1",
    "--rps=1",
    "--timeout-ms=15000",
    "--retries=2",
  ]);

  expect(start.exitCode).toBe(0);
  expect(start.stdout).toContain("started");

  let progress = "";
  for (let i = 0; i < 30; i += 1) {
    await Bun.sleep(1_000);
    const result = await runCli(["progress", `--data-dir=${dataDir}`]);
    progress = result.stdout;
    if (progress.includes("done_epochs: 1")) break;
  }

  expect(progress).toContain("done_epochs: 1");
  expect(progress).toContain("active_locks: 0");

  let logs: CommandResult | undefined;
  for (let i = 0; i < 10; i += 1) {
    await Bun.sleep(500);
    logs = await runCli(["logs", `--data-dir=${dataDir}`]);
    if (logs.stdout.includes("daemon_exit")) break;
  }

  expect(logs?.stdout ?? "").toContain("daemon_start");
  expect(logs?.stdout ?? "").toContain("dumped epoch=1");
  expect(logs?.stdout ?? "").toContain("daemon_exit");

  const status = await runCli(["status", `--data-dir=${dataDir}`]);
  expect(status.stdout).toContain("status: stopped");
});

test("real index run handles 10 random historical epochs", async () => {
  const dataDir = await tempDataDir();
  const epochs = randomEpochs(10, await currentEpoch());

  for (const epoch of epochs) {
    const result = await runCli(
      [
        "run",
        `--data-dir=${dataDir}`,
        `--from=${epoch}`,
        `--to=${epoch}`,
        "--workers=1",
        "--rps=1",
        "--timeout-ms=15000",
        "--retries=2",
      ],
      45_000,
    );

    expect(result.exitCode).toBe(0);
    expect(result.stdout).toContain(`dumped epoch=${epoch}`);
    await expectFile(join(dataDir, "state", "epochs", `${epoch}.done`));
  }

  const cursor = JSON.parse(await readFile(join(dataDir, "state", "cursor.json"), "utf8"));
  expect(cursor.last_completed_epoch).toBe(epochs.at(-1));

  for (const epoch of epochs) {
    const epochRows = await readJsonl(join(dataDir, "raw", "epochs", epochShard("epochs", epoch)));
    expect(epochRows.some((row) => row.type === "epoch_get" && row.epoch === epoch)).toBe(true);

    const txRows = await readJsonl(join(dataDir, "raw", "tx_by_epoch", epochShard("tx_by_epoch", epoch)));
    expect(txRows.some((row) => row.type === "octra_transactionsByEpoch" && row.epoch === epoch)).toBe(true);
  }
}, { timeout: 120_000 });
