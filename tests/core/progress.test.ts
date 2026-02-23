import assert from "node:assert/strict";
import { test } from "node:test";
import { createProgressReporter } from "../../src/core/progress.js";

test("progress reporter line mode formats prefix + context once", () => {
  const envVar = "AGGR_TEST_PROGRESS";
  const previous = process.env[envVar];
  process.env[envVar] = "line";

  const logs: string[] = [];
  const originalLog = console.log;
  console.log = (...args: unknown[]) => {
    logs.push(args.map((arg) => String(arg)).join(" "));
  };

  try {
    const reporter = createProgressReporter({
      envVar,
      prefix: "[task]",
      minUpdateIntervalMs: 0,
      maxLen: 220,
    });
    reporter.setContext("[EX/SYM/2024-01-01] 120000ms gap @ 00:00");
    reporter.update("[task] scanning 2024-01-01-00.gz ...");
    reporter.update("[task] recovered 5 / 8");
    reporter.log("[task] complete");
  } finally {
    console.log = originalLog;
    if (previous === undefined) delete process.env[envVar];
    else process.env[envVar] = previous;
  }

  assert.deepEqual(logs, [
    "[task] [EX/SYM/2024-01-01] 120000ms gap @ 00:00 : scanning 2024-01-01-00.gz ...",
    "[task] [EX/SYM/2024-01-01] 120000ms gap @ 00:00 : recovered 5 / 8",
    "[task] complete",
  ]);
});

test("progress reporter off mode suppresses updates only", () => {
  const envVar = "AGGR_TEST_PROGRESS";
  const previous = process.env[envVar];
  process.env[envVar] = "0";

  const logs: string[] = [];
  const originalLog = console.log;
  console.log = (...args: unknown[]) => {
    logs.push(args.map((arg) => String(arg)).join(" "));
  };

  try {
    const reporter = createProgressReporter({
      envVar,
      prefix: "[task]",
      minUpdateIntervalMs: 0,
    });
    reporter.update("[task] hidden update");
    reporter.log("[task] explicit line");
  } finally {
    console.log = originalLog;
    if (previous === undefined) delete process.env[envVar];
    else process.env[envVar] = previous;
  }

  assert.deepEqual(logs, ["[task] explicit line"]);
});
