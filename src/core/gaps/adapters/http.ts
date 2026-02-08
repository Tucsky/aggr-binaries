import { setTimeout as delay } from "node:timers/promises";
import type { FetchLike } from "./types.js";

const RETRYABLE_STATUS = new Set<number>([429, 500, 502, 503, 504]);
const MAX_SERVER_RETRY_AFTER_MS = 300_000;

interface FetchPolicy {
  minIntervalMs: number;
  maxAttempts: number;
  baseBackoffMs: number;
  maxBackoffMs: number;
}

interface FetchPolicyOverride {
  minIntervalMs?: number;
  maxAttempts?: number;
  baseBackoffMs?: number;
  maxBackoffMs?: number;
}

interface CreateRateLimitedFetchOptions {
  defaultPolicy?: FetchPolicyOverride;
  hostOverrides?: Record<string, FetchPolicyOverride>;
  now?: () => number;
  sleep?: (ms: number) => Promise<void>;
}

const DEFAULT_POLICY: FetchPolicy = {
  minIntervalMs: 100,
  maxAttempts: 5,
  baseBackoffMs: 250,
  maxBackoffMs: 5_000,
};

const HOST_OVERRIDES: Record<string, FetchPolicyOverride> = {
  "api-pub.bitfinex.com": { minIntervalMs: 1000, maxAttempts: 8, baseBackoffMs: 500, maxBackoffMs: 10_000 },
  "api.kraken.com": { minIntervalMs: 400, maxAttempts: 6, baseBackoffMs: 500, maxBackoffMs: 10_000 },
  "api.exchange.coinbase.com": { minIntervalMs: 250, maxAttempts: 6, baseBackoffMs: 500, maxBackoffMs: 10_000 },
  "api.coinbase.com": { minIntervalMs: 200, maxAttempts: 6, baseBackoffMs: 400, maxBackoffMs: 8_000 },
  "public.bybit.com": { minIntervalMs: 120, maxAttempts: 4, baseBackoffMs: 300, maxBackoffMs: 4_000 },
  "data.binance.vision": { minIntervalMs: 120, maxAttempts: 4, baseBackoffMs: 300, maxBackoffMs: 4_000 },
};

export function createRateLimitedFetch(
  fetchImpl: FetchLike,
  options: CreateRateLimitedFetchOptions = {},
): FetchLike {
  const now = options.now ?? Date.now;
  const sleep = options.sleep ?? defaultSleep;
  const nextAllowedAtByHost = new Map<string, number>();
  const defaultPolicy = mergePolicy(DEFAULT_POLICY, options.defaultPolicy);
  const debug = process.env.AGGR_FIXGAPS_DEBUG_HTTP === "1" || process.env.AGGR_FIXGAPS_DEBUG === "1";

  return async (input: string | URL, init?: RequestInit): Promise<Response> => {
    const url = toUrl(input);
    const host = url.host.toLowerCase();
    const policy = resolvePolicy(host, defaultPolicy, options.hostOverrides);

    for (let attempt = 1; attempt <= policy.maxAttempts; attempt += 1) {
      await waitForHostTurn(host, policy.minIntervalMs, nextAllowedAtByHost, now, sleep);

      let response: Response;
      try {
        response = await fetchImpl(input, init);
      } catch (err) {
        if (attempt >= policy.maxAttempts) {
          throw err;
        }
        const backoffMs = computeBackoff(policy, attempt);
        if (debug) {
          const message = err instanceof Error ? err.message : "unknown";
          console.log(
            `[fixgaps/http] retry_transport host=${host} attempt=${attempt}/${policy.maxAttempts} delay_ms=${backoffMs} url=${formatUrlForLog(url)} error=${message}`,
          );
        }
        pushHostCooldown(host, backoffMs, nextAllowedAtByHost, now);
        await sleepWithDebug(backoffMs, sleep, debug, host);
        continue;
      }

      if (!RETRYABLE_STATUS.has(response.status) || attempt >= policy.maxAttempts) {
        if (debug && response.status >= 400) {
          console.log(
            `[fixgaps/http] response host=${host} attempt=${attempt}/${policy.maxAttempts} status=${response.status} url=${formatUrlForLog(url)}`,
          );
        }
        return response;
      }

      const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"), now);
      const backoffMs = computeBackoff(policy, attempt);
      const serverDelayMs = retryAfterMs === undefined ? undefined : Math.min(MAX_SERVER_RETRY_AFTER_MS, retryAfterMs);
      const delayMs = Math.max(policy.minIntervalMs, serverDelayMs ?? backoffMs);
      if (debug) {
        const retryAfterLog = retryAfterMs === undefined ? "none" : String(retryAfterMs);
        console.log(
          `[fixgaps/http] retry_status host=${host} attempt=${attempt}/${policy.maxAttempts} status=${response.status} retry_after_ms=${retryAfterLog} delay_ms=${delayMs} url=${formatUrlForLog(url)}`,
        );
      }
      disposeBody(response);
      pushHostCooldown(host, delayMs, nextAllowedAtByHost, now);
      await sleepWithDebug(delayMs, sleep, debug, host);
    }

    throw new Error(`Exceeded retry budget for ${url.toString()}`);
  };
}

function mergePolicy(base: FetchPolicy, override?: FetchPolicyOverride): FetchPolicy {
  return {
    minIntervalMs: positiveOr(base.minIntervalMs, override?.minIntervalMs),
    maxAttempts: positiveOr(base.maxAttempts, override?.maxAttempts),
    baseBackoffMs: positiveOr(base.baseBackoffMs, override?.baseBackoffMs),
    maxBackoffMs: positiveOr(base.maxBackoffMs, override?.maxBackoffMs),
  };
}

function resolvePolicy(
  host: string,
  defaultPolicy: FetchPolicy,
  hostOverrides?: Record<string, FetchPolicyOverride>,
): FetchPolicy {
  const override = hostOverrides?.[host] ?? HOST_OVERRIDES[host];
  return mergePolicy(defaultPolicy, override);
}

async function waitForHostTurn(
  host: string,
  minIntervalMs: number,
  nextAllowedAtByHost: Map<string, number>,
  now: () => number,
  sleep: (ms: number) => Promise<void>,
): Promise<void> {
  const nowMs = now();
  const nextAllowedAt = nextAllowedAtByHost.get(host) ?? 0;
  if (nextAllowedAt > nowMs) {
    await sleep(nextAllowedAt - nowMs);
  }
  nextAllowedAtByHost.set(host, now() + minIntervalMs);
}

function computeBackoff(policy: FetchPolicy, attempt: number): number {
  const multiplier = 2 ** (attempt - 1);
  const raw = policy.baseBackoffMs * multiplier;
  return raw > policy.maxBackoffMs ? policy.maxBackoffMs : raw;
}

function pushHostCooldown(
  host: string,
  cooldownMs: number,
  nextAllowedAtByHost: Map<string, number>,
  now: () => number,
): void {
  const until = now() + cooldownMs;
  const current = nextAllowedAtByHost.get(host) ?? 0;
  if (until > current) {
    nextAllowedAtByHost.set(host, until);
  }
}

function parseRetryAfterMs(value: string | null, now: () => number): number | undefined {
  if (!value) return undefined;
  const asNumber = Number(value);
  if (Number.isFinite(asNumber)) {
    const ms = Math.round(asNumber * 1000);
    return ms >= 0 ? ms : 0;
  }
  const asDate = Date.parse(value);
  if (!Number.isFinite(asDate)) return undefined;
  const ms = asDate - now();
  return ms > 0 ? ms : 0;
}

function disposeBody(response: Response): void {
  const body = response.body;
  if (!body) return;
  void body.cancel().catch(() => {});
}

function positiveOr(fallback: number, candidate?: number): number {
  if (candidate === undefined) return fallback;
  if (!Number.isFinite(candidate) || candidate <= 0) return fallback;
  return Math.round(candidate);
}

function toUrl(input: string | URL): URL {
  if (input instanceof URL) return input;
  return new URL(input);
}

async function defaultSleep(ms: number): Promise<void> {
  if (ms <= 0) return;
  await delay(ms);
}

async function sleepWithDebug(
  ms: number,
  sleep: (ms: number) => Promise<void>,
  debug: boolean,
  host: string,
): Promise<void> {
  if (!debug || ms <= 10_000) {
    await sleep(ms);
    return;
  }
  let remaining = ms;
  while (remaining > 0) {
    const chunk = remaining > 10_000 ? 10_000 : remaining;
    await sleep(chunk);
    remaining -= chunk;
    if (remaining > 0) {
      console.log(`[fixgaps/http] wait host=${host} remaining_ms=${remaining}`);
    }
  }
}

function formatUrlForLog(url: URL): string {
  return `${url.origin}${url.pathname}${url.search}`;
}
