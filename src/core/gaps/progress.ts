import { createProgressReporter, formatProgressUrl } from "../progress.js";

const reporter = createProgressReporter({
  envVar: "AGGR_FIXGAPS_PROGRESS",
  prefix: "[fixgaps]",
  maxLen: 180,
  minUpdateIntervalMs: 150,
});

export function setFixgapsProgressContext(context?: string): void {
  reporter.setContext(context);
}

export function setFixgapsProgress(message: string): void {
  reporter.update(message);
}

export function clearFixgapsProgress(): void {
  reporter.clear();
}

export function logFixgapsLine(line: string): void {
  reporter.log(line);
}

export { formatProgressUrl };
