import assert from "node:assert/strict";
import type { DatabaseSync } from "node:sqlite";
import { test } from "node:test";
import {
  isSqliteWriteContentionError,
  runSqliteWrite,
  runSqliteWriteTransaction,
} from "../../src/core/sqliteWrite.js";

interface TestSqliteError extends Error {
  code: string;
  errcode: number;
  errstr: string;
}

function makeLockedError(errcode = 517): TestSqliteError {
  const err = new Error("database is locked") as TestSqliteError;
  err.code = "ERR_SQLITE_ERROR";
  err.errcode = errcode;
  err.errstr = "database is locked";
  return err;
}

test("runSqliteWrite retries contention and returns once unlocked", () => {
  let attempts = 0;
  const sleeps: number[] = [];
  const value = runSqliteWrite(
    () => {
      attempts += 1;
      if (attempts < 3) {
        throw makeLockedError();
      }
      return "ok";
    },
    {
      maxRetries: 5,
      baseDelayMs: 1,
      maxDelayMs: 4,
      sleep: (delayMs: number) => {
        sleeps.push(delayMs);
      },
    },
  );

  assert.strictEqual(value, "ok");
  assert.strictEqual(attempts, 3);
  assert.deepStrictEqual(sleeps, [1, 2]);
});

test("runSqliteWrite does not retry non-contention errors", () => {
  let attempts = 0;
  assert.throws(
    () =>
      runSqliteWrite(
        () => {
          attempts += 1;
          throw new Error("boom");
        },
        {
          maxRetries: 5,
          sleep: () => {
            throw new Error("sleep should not be called");
          },
        },
      ),
    /boom/,
  );
  assert.strictEqual(attempts, 1);
});

test("runSqliteWriteTransaction retries a rolled-back transaction", () => {
  const statements: string[] = [];
  const fakeDb = {
    exec: (sql: string) => {
      statements.push(sql);
    },
  } as unknown as DatabaseSync;

  let attempts = 0;
  const sleeps: number[] = [];
  const result = runSqliteWriteTransaction(
    fakeDb,
    () => {
      attempts += 1;
      if (attempts === 1) {
        throw makeLockedError(5);
      }
      return 7;
    },
    {
      maxRetries: 2,
      baseDelayMs: 3,
      maxDelayMs: 3,
      sleep: (delayMs: number) => {
        sleeps.push(delayMs);
      },
    },
  );

  assert.strictEqual(result, 7);
  assert.strictEqual(attempts, 2);
  assert.deepStrictEqual(statements, ["BEGIN", "ROLLBACK", "BEGIN", "COMMIT"]);
  assert.deepStrictEqual(sleeps, [3]);
});

test("isSqliteWriteContentionError matches sqlite busy/locked families", () => {
  assert.strictEqual(isSqliteWriteContentionError(makeLockedError(5)), true);
  assert.strictEqual(isSqliteWriteContentionError(makeLockedError(261)), true);
  assert.strictEqual(isSqliteWriteContentionError(makeLockedError(6)), true);
  assert.strictEqual(isSqliteWriteContentionError(new Error("other")), false);
});
