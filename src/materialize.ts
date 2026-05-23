import { Database } from "bun:sqlite";
import { mkdir, readdir, readFile, stat, writeFile } from "node:fs/promises";
import { dirname, join, relative } from "node:path";

type JsonObject = Record<string, unknown>;

type Config = {
  dataDir: string;
  dbPath: string;
  fromEpoch?: number;
  toEpoch?: number;
  force: boolean;
};

const defaultDataDir = join(process.cwd(), "octra-data");

function parseArgs(): Config {
  const args = new Map<string, string | boolean>();
  for (const arg of Bun.argv.slice(2)) {
    if (!arg.startsWith("--")) continue;
    const [key = "", value] = arg.slice(2).split("=", 2);
    if (!key) continue;
    args.set(key, value ?? true);
  }

  const str = (name: string, fallback?: string) => {
    const envName = `OCTRA_${name.toUpperCase().replace(/-/g, "_")}`;
    const raw = args.get(name) ?? Bun.env[envName];
    if (raw === undefined || raw === true) return fallback;
    return String(raw);
  };

  const num = (name: string) => {
    const raw = args.get(name);
    if (raw === undefined || raw === true) return undefined;
    const value = Number(raw);
    if (!Number.isFinite(value)) throw new Error(`Invalid numeric value for ${name}: ${raw}`);
    return value;
  };

  const dataDir = str("data-dir", defaultDataDir) ?? defaultDataDir;
  return {
    dataDir,
    dbPath: str("db-path", join(dataDir, "materialized", "octra.sqlite")) ?? join(dataDir, "materialized", "octra.sqlite"),
    fromEpoch: num("from"),
    toEpoch: num("to"),
    force: args.has("force"),
  };
}

async function listFiles(root: string): Promise<string[]> {
  let entries;
  try {
    entries = await readdir(root, { withFileTypes: true });
  } catch {
    return [];
  }

  const files: string[] = [];
  for (const entry of entries) {
    const path = join(root, entry.name);
    if (entry.isDirectory()) files.push(...(await listFiles(path)));
    else if (entry.isFile() && entry.name.endsWith(".jsonl")) files.push(path);
  }
  return files.sort();
}

function inEpochRange(config: Config, epoch: unknown) {
  if (typeof epoch !== "number") return true;
  if (config.fromEpoch !== undefined && epoch < config.fromEpoch) return false;
  if (config.toEpoch !== undefined && epoch > config.toEpoch) return false;
  return true;
}

function asObject(value: unknown): JsonObject {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as JsonObject) : {};
}

function asString(value: unknown) {
  if (value === undefined || value === null) return undefined;
  return String(value);
}

function asNumber(value: unknown) {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    if (Number.isFinite(parsed)) return parsed;
  }
  return undefined;
}

function sqlString(value: unknown) {
  return asString(value) ?? null;
}

function sqlNumber(value: unknown) {
  return asNumber(value) ?? null;
}

function txHash(tx: JsonObject, fallback: string) {
  return asString(tx.hash ?? tx.tx_hash ?? tx.id) ?? fallback;
}

function txAddress(tx: JsonObject, key: string) {
  return asString(tx[key] ?? tx[`${key}_address`] ?? tx[`${key}Address`]);
}

function collectAddresses(tx: JsonObject) {
  const pairs: Array<[string, string]> = [];
  const from = txAddress(tx, "from") ?? txAddress(tx, "sender");
  const to = txAddress(tx, "to") ?? txAddress(tx, "recipient");
  if (from) pairs.push([from, "from"]);
  if (to) pairs.push([to, "to"]);
  return pairs;
}

function setupDb(db: Database) {
  db.exec(`
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;

    CREATE TABLE IF NOT EXISTS epochs (
      epoch_id INTEGER PRIMARY KEY,
      fetched_at TEXT,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS transactions (
      hash TEXT PRIMARY KEY,
      epoch_id INTEGER NOT NULL,
      status TEXT NOT NULL,
      tx_index INTEGER NOT NULL,
      from_address TEXT,
      to_address TEXT,
      amount TEXT,
      ou TEXT,
      op_type TEXT,
      timestamp REAL,
      message TEXT,
      raw_json TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS transactions_epoch_idx ON transactions(epoch_id, tx_index);
    CREATE INDEX IF NOT EXISTS transactions_from_idx ON transactions(from_address);
    CREATE INDEX IF NOT EXISTS transactions_to_idx ON transactions(to_address);

    CREATE TABLE IF NOT EXISTS addresses (
      address TEXT PRIMARY KEY,
      first_seen_epoch INTEGER NOT NULL,
      last_seen_epoch INTEGER NOT NULL,
      tx_count INTEGER NOT NULL DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS address_transactions (
      address TEXT NOT NULL,
      tx_hash TEXT NOT NULL,
      epoch_id INTEGER NOT NULL,
      role TEXT NOT NULL,
      PRIMARY KEY (address, tx_hash, role)
    );

    CREATE INDEX IF NOT EXISTS address_transactions_epoch_idx ON address_transactions(epoch_id);

    CREATE TABLE IF NOT EXISTS staging_snapshots (
      fetched_at TEXT PRIMARY KEY,
      raw_json TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS materializer_checkpoints (
      source_file TEXT PRIMARY KEY,
      size_bytes INTEGER NOT NULL,
      mtime_ms REAL NOT NULL,
      processed_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS materializer_state (
      key TEXT PRIMARY KEY,
      value TEXT NOT NULL,
      updated_at TEXT NOT NULL
    );
  `);
}

function upsertAddress(db: Database, address: string, epoch: number, txHashValue: string, role: string) {
  db.query(`
    INSERT INTO address_transactions (address, tx_hash, epoch_id, role)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(address, tx_hash, role) DO NOTHING
  `).run(address, txHashValue, epoch, role);

  db.query(`
    INSERT INTO addresses (address, first_seen_epoch, last_seen_epoch, tx_count)
    VALUES (?, ?, ?, 1)
    ON CONFLICT(address) DO UPDATE SET
      first_seen_epoch = MIN(first_seen_epoch, excluded.first_seen_epoch),
      last_seen_epoch = MAX(last_seen_epoch, excluded.last_seen_epoch),
      tx_count = (
        SELECT COUNT(DISTINCT tx_hash)
        FROM address_transactions
        WHERE address = excluded.address
      )
  `).run(address, epoch, epoch);
}

function insertEpoch(db: Database, row: JsonObject) {
  const epoch = asNumber(row.epoch);
  if (epoch === undefined) return 0;
  db.query(`
    INSERT INTO epochs (epoch_id, fetched_at, raw_json)
    VALUES (?, ?, ?)
    ON CONFLICT(epoch_id) DO UPDATE SET
      fetched_at = excluded.fetched_at,
      raw_json = excluded.raw_json
  `).run(epoch, sqlString(row.fetched_at), JSON.stringify(row.result ?? row));
  return 1;
}

function insertTransactions(db: Database, row: JsonObject, config: Config) {
  const result = asObject(row.result);
  const epoch = asNumber(row.epoch ?? result.epoch_id);
  if (epoch === undefined || !inEpochRange(config, epoch)) return 0;

  let inserted = 0;
  const offset = asNumber(row.offset) ?? asNumber(result.offset) ?? 0;
  const groups: Array<[string, unknown]> = [
    ["confirmed", result.transactions],
    ["rejected", result.rejected],
  ];

  for (const [status, value] of groups) {
    if (!Array.isArray(value)) continue;
    value.forEach((item, index) => {
      const tx = asObject(item);
      const hash = txHash(tx, `${epoch}:${status}:${offset + index}`);
      const from = txAddress(tx, "from") ?? txAddress(tx, "sender");
      const to = txAddress(tx, "to") ?? txAddress(tx, "recipient");
      db.query(`
        INSERT INTO transactions (
          hash, epoch_id, status, tx_index, from_address, to_address,
          amount, ou, op_type, timestamp, message, raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(hash) DO UPDATE SET
          epoch_id = excluded.epoch_id,
          status = excluded.status,
          tx_index = excluded.tx_index,
          from_address = excluded.from_address,
          to_address = excluded.to_address,
          amount = excluded.amount,
          ou = excluded.ou,
          op_type = excluded.op_type,
          timestamp = excluded.timestamp,
          message = excluded.message,
          raw_json = excluded.raw_json
      `).run(
        hash,
        epoch,
        status,
        offset + index,
        from ?? null,
        to ?? null,
        sqlString(tx.amount),
        sqlString(tx.ou),
        sqlString(tx.op_type),
        sqlNumber(tx.timestamp),
        sqlString(tx.message),
        JSON.stringify(tx),
      );

      for (const [address, role] of collectAddresses(tx)) {
        upsertAddress(db, address, epoch, hash, role);
      }
      inserted += 1;
    });
  }

  return inserted;
}

function insertStaging(db: Database, row: JsonObject) {
  const fetchedAt = asString(row.fetched_at);
  if (!fetchedAt) return 0;
  db.query(`
    INSERT INTO staging_snapshots (fetched_at, raw_json)
    VALUES (?, ?)
    ON CONFLICT(fetched_at) DO UPDATE SET raw_json = excluded.raw_json
  `).run(fetchedAt, JSON.stringify(row));
  return 1;
}

async function processJsonlFile(db: Database, config: Config, file: string) {
  const fileStat = await stat(file);
  const sourceFile = relative(config.dataDir, file);
  if (!config.force) {
    const checkpoint = db.query("SELECT size_bytes, mtime_ms FROM materializer_checkpoints WHERE source_file = ?").get(sourceFile) as
      | { size_bytes: number; mtime_ms: number }
      | undefined;
    if (checkpoint && checkpoint.size_bytes === fileStat.size && checkpoint.mtime_ms === fileStat.mtimeMs) {
      return { files: 0, epochs: 0, transactions: 0, staging: 0 };
    }
  }

  let epochs = 0;
  let transactions = 0;
  let staging = 0;
  const raw = await readFile(file, "utf8");
  const tx = db.transaction(() => {
    for (const line of raw.split(/\r?\n/)) {
      if (!line.trim()) continue;
      const row = JSON.parse(line) as JsonObject;
      const type = asString(row.type);
      const epoch = asNumber(row.epoch);
      if (epoch !== undefined && !inEpochRange(config, epoch)) continue;

      if (type === "epoch_get") epochs += insertEpoch(db, row);
      else if (type === "octra_transactionsByEpoch") transactions += insertTransactions(db, row, config);
      else if (type === "staging_snapshot") staging += insertStaging(db, row);
    }

    db.query(`
      INSERT INTO materializer_checkpoints (source_file, size_bytes, mtime_ms, processed_at)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(source_file) DO UPDATE SET
        size_bytes = excluded.size_bytes,
        mtime_ms = excluded.mtime_ms,
        processed_at = excluded.processed_at
    `).run(sourceFile, fileStat.size, fileStat.mtimeMs, new Date().toISOString());
  });
  tx();

  return { files: 1, epochs, transactions, staging };
}

async function writeMaterializerCursor(config: Config, summary: { files: number; epochs: number; transactions: number; staging: number }) {
  const path = join(config.dataDir, "state", "materializer.cursor.json");
  await mkdir(dirname(path), { recursive: true });
  await writeFile(path, `${JSON.stringify({ ...summary, db_path: config.dbPath, updated_at: new Date().toISOString() }, null, 2)}\n`);
}

async function main() {
  const config = parseArgs();
  await mkdir(dirname(config.dbPath), { recursive: true });
  await mkdir(join(config.dataDir, "state"), { recursive: true });

  const db = new Database(config.dbPath);
  setupDb(db);

  const files = [
    ...(await listFiles(join(config.dataDir, "raw", "epochs"))),
    ...(await listFiles(join(config.dataDir, "raw", "tx_by_epoch"))),
    ...(await listFiles(join(config.dataDir, "raw", "staging"))),
  ];

  const summary = { files: 0, epochs: 0, transactions: 0, staging: 0 };
  for (const file of files) {
    const result = await processJsonlFile(db, config, file);
    summary.files += result.files;
    summary.epochs += result.epochs;
    summary.transactions += result.transactions;
    summary.staging += result.staging;
  }

  db.query(`
    INSERT INTO materializer_state (key, value, updated_at)
    VALUES ('last_summary', ?, ?)
    ON CONFLICT(key) DO UPDATE SET
      value = excluded.value,
      updated_at = excluded.updated_at
  `).run(JSON.stringify(summary), new Date().toISOString());

  db.close();
  await writeMaterializerCursor(config, summary);

  console.log(`materialized_db=${config.dbPath}`);
  console.log(`processed_files=${summary.files}`);
  console.log(`epochs=${summary.epochs}`);
  console.log(`transactions=${summary.transactions}`);
  console.log(`staging_snapshots=${summary.staging}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.stack ?? error.message : error);
  process.exit(1);
});
