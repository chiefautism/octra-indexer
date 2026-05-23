import { appendFile, rm } from "node:fs/promises";

const bunPath = Bun.env.BUN_BIN ?? Bun.argv[0] ?? "bun";
const args = JSON.parse(Bun.env.OCTRA_INDEXER_ARGS ?? "[]") as string[];
const logPath = Bun.env.OCTRA_LOG_PATH ?? "indexer.log";
const errPath = Bun.env.OCTRA_ERR_PATH ?? "indexer.err.log";
const pidPath = Bun.env.OCTRA_PID_PATH;

async function pipeToFile(stream: ReadableStream<Uint8Array> | null, path: string) {
  if (!stream) return;
  for await (const chunk of stream) {
    await appendFile(path, chunk);
  }
}

await appendFile(logPath, `daemon_start ${new Date().toISOString()}\n`);

const child = Bun.spawn([bunPath, "src/index.ts", ...args], {
  stdin: "ignore",
  stdout: "pipe",
  stderr: "pipe",
  env: Bun.env,
});

const stop = () => {
  child.kill("SIGTERM");
};

process.on("SIGINT", stop);
process.on("SIGTERM", stop);

await Promise.all([
  pipeToFile(child.stdout, logPath),
  pipeToFile(child.stderr, errPath),
  child.exited.then(async (code) => {
    await appendFile(logPath, `daemon_exit ${new Date().toISOString()} code=${code}\n`);
    if (pidPath) await rm(pidPath, { force: true });
  }),
]);
