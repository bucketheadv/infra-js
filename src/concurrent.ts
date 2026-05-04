import _ from "lodash";

type AsyncTask<Args extends unknown[], Result> = (
    ...args: Args
) => Promise<Result>;

type MaybePromise<T> = T | Promise<T>;

export enum RateLimitMode {
    Delay = "delay",
    FailFast = "failFast",
}

export class RateLimitError extends Error {
    readonly qps: number;
    readonly waitMs: number;
    readonly mode: RateLimitMode;

    constructor(qps: number, waitMs: number, mode: RateLimitMode) {
        super("触发限流");
        this.name = "RateLimitError";
        this.qps = qps;
        this.waitMs = waitMs;
        this.mode = mode;
    }
}

type RateLimitOptions = {
    qps?: number;
    mode?: RateLimitMode;
};

export enum HystrixState {
    Closed = "closed",
    Open = "open",
    HalfOpen = "halfOpen",
}

export class HystrixOpenError extends Error {
    /** 距离允许下一次探测请求还需等待的毫秒数。 */
    readonly retryAfterMs: number;
    /** 熔断打开后的休眠窗口，单位毫秒。 */
    readonly sleepWindowMs: number;

    constructor(retryAfterMs: number, sleepWindowMs: number) {
        super("熔断器处于打开状态");
        this.name = "HystrixOpenError";
        this.retryAfterMs = retryAfterMs;
        this.sleepWindowMs = sleepWindowMs;
    }
}

export class HystrixTimeoutError extends Error {
    /** 当前请求允许的最大执行时长，单位毫秒。 */
    readonly timeoutMs: number;

    constructor(timeoutMs: number) {
        super("请求执行超时");
        this.name = "HystrixTimeoutError";
        this.timeoutMs = timeoutMs;
    }
}

type HystrixFallback<Args extends unknown[], Result> = (
    error: unknown,
    ...args: Args
) => MaybePromise<Result>;

export type HystrixOptions<Args extends unknown[], Result> = {
    /** 单次请求超时时间，默认 `3000` 毫秒。 */
    timeoutMs?: number;
    /** 统计窗口内最少请求数，达到后才开始按失败率判断是否熔断，默认 `10`。 */
    volumeThreshold?: number;
    /** 触发熔断所需的失败率阈值，取值范围 `1-100`，默认 `50`。 */
    errorThresholdPercentage?: number;
    /** 熔断打开后进入半开探测前的等待时间，默认 `5000` 毫秒。 */
    sleepWindowMs?: number;
    /** 熔断或执行失败时的降级回调，默认不提供。 */
    fallback?: HystrixFallback<Args, Result>;
};

const DEFAULT_HYSTRIX_OPTIONS = {
    timeoutMs: 3000,
    volumeThreshold: 10,
    errorThresholdPercentage: 50,
    sleepWindowMs: 5000,
} as const;

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

// 递归排序对象的 key，确保语义相同的参数能生成一致的缓存 key。
function sortKeysDeep(input: unknown): unknown {
    if (Array.isArray(input)) {
        return input.map(sortKeysDeep);
    }

    if (_.isPlainObject(input)) {
        return Object.fromEntries(
            Object.entries(input as Record<string, unknown>)
                .sort(([left], [right]) => left.localeCompare(right))
                .map(([entryKey, entryValue]) => [entryKey, sortKeysDeep(entryValue)]),
        );
    }

    return input;
}

// 将整组入参序列化为稳定字符串，用于并发请求去重。
function serializeArgs(args: unknown[]): string {
    return JSON.stringify(sortKeysDeep(args));
}

export function warpOnce<Args extends unknown[], Result>(
    task: AsyncTask<Args, Result>,
): AsyncTask<Args, Result> {
    const pendingPromises = new Map<string, Promise<Result>>();

    return async (...args: Args): Promise<Result> => {
        const argsKey = serializeArgs(args);
        const pendingPromise = pendingPromises.get(argsKey);

        if (pendingPromise) {
            return pendingPromise;
        }

        // 同一个参数 key 下，只让第一个进行中的请求真正执行原始异步方法。
        const currentPromise = Promise.resolve()
            .then(() => task(...args))
            .finally(() => {
                pendingPromises.delete(argsKey);
            });

        pendingPromises.set(argsKey, currentPromise);

        return currentPromise;
    };
}

export function warpQps<Args extends unknown[], Result>(
    task: AsyncTask<Args, Result>,
    options: RateLimitOptions = {},
): AsyncTask<Args, Result> {
    const qps = options.qps ?? 500;
    const mode = options.mode ?? RateLimitMode.FailFast;

    if (qps <= 0) {
        throw new Error("qps 必须大于 0");
    }

    const capacity = qps;
    let availableTokens = capacity;
    let lastRefillTime = Date.now();
    let acquireQueue = Promise.resolve();

    function refillTokens(now: number): void {
        const elapsedMs = now - lastRefillTime;

        if (elapsedMs <= 0) {
            return;
        }

        const restoredTokens = (elapsedMs / 1000) * qps;
        availableTokens = Math.min(capacity, availableTokens + restoredTokens);
        lastRefillTime = now;
    }

    async function acquireToken(): Promise<void> {
        while (true) {
            const now = Date.now();
            refillTokens(now);

            if (availableTokens >= 1) {
                availableTokens -= 1;
                return;
            }

            const waitMs = ((1 - availableTokens) / qps) * 1000;

            if (mode === RateLimitMode.FailFast) {
                throw new RateLimitError(qps, waitMs, mode);
            }

            await sleep(waitMs);
        }
    }

    return async (...args: Args): Promise<Result> => {
        const currentAcquire = acquireQueue.then(() => acquireToken());
        acquireQueue = currentAcquire.catch(() => {});
        await currentAcquire;

        return task(...args);
    };
}

export function warpHystrix<Args extends unknown[], Result>(
    task: AsyncTask<Args, Result>,
    options: HystrixOptions<Args, Result> = {},
): AsyncTask<Args, Result> {
    const timeoutMs = options.timeoutMs ?? DEFAULT_HYSTRIX_OPTIONS.timeoutMs;
    const volumeThreshold =
        options.volumeThreshold ?? DEFAULT_HYSTRIX_OPTIONS.volumeThreshold;
    const errorThresholdPercentage =
        options.errorThresholdPercentage ??
        DEFAULT_HYSTRIX_OPTIONS.errorThresholdPercentage;
    const sleepWindowMs = options.sleepWindowMs ?? DEFAULT_HYSTRIX_OPTIONS.sleepWindowMs;
    const fallback = options.fallback;

    if (timeoutMs <= 0) {
        throw new Error("timeoutMs 必须大于 0");
    }

    if (volumeThreshold <= 0) {
        throw new Error("volumeThreshold 必须大于 0");
    }

    if (errorThresholdPercentage <= 0 || errorThresholdPercentage > 100) {
        throw new Error("errorThresholdPercentage 必须在 1 到 100 之间");
    }

    if (sleepWindowMs <= 0) {
        throw new Error("sleepWindowMs 必须大于 0");
    }

    let state = HystrixState.Closed;
    let openedAt = 0;
    let halfOpenProbeInFlight = false;
    const recentOutcomes: boolean[] = [];

    function recordOutcome(succeeded: boolean): void {
        recentOutcomes.push(succeeded);

        if (recentOutcomes.length > volumeThreshold) {
            recentOutcomes.shift();
        }
    }

    function resetCircuit(): void {
        state = HystrixState.Closed;
        openedAt = 0;
        halfOpenProbeInFlight = false;
        recentOutcomes.length = 0;
    }

    function openCircuit(): void {
        state = HystrixState.Open;
        openedAt = Date.now();
        halfOpenProbeInFlight = false;
    }

    function shouldOpenCircuit(): boolean {
        if (recentOutcomes.length < volumeThreshold) {
            return false;
        }

        const failureCount = recentOutcomes.filter((item) => !item).length;
        const failureRate = (failureCount / recentOutcomes.length) * 100;
        return failureRate >= errorThresholdPercentage;
    }

    async function withTimeout(...args: Args): Promise<Result> {
        return new Promise<Result>((resolve, reject) => {
            const timer = setTimeout(() => {
                reject(new HystrixTimeoutError(timeoutMs));
            }, timeoutMs);

            Promise.resolve(task(...args))
                .then((result) => {
                    clearTimeout(timer);
                    resolve(result);
                })
                .catch((error: unknown) => {
                    clearTimeout(timer);
                    reject(error);
                });
        });
    }

    async function handleFallback(error: unknown, args: Args): Promise<Result> {
        if (!fallback) {
            throw error;
        }

        return fallback(error, ...args);
    }

    return async (...args: Args): Promise<Result> => {
        const now = Date.now();

        if (state === HystrixState.Open) {
            const elapsed = now - openedAt;

            if (elapsed < sleepWindowMs) {
                return handleFallback(
                    new HystrixOpenError(sleepWindowMs - elapsed, sleepWindowMs),
                    args,
                );
            }

            state = HystrixState.HalfOpen;
        }

        if (state === HystrixState.HalfOpen) {
            if (halfOpenProbeInFlight) {
                return handleFallback(new HystrixOpenError(sleepWindowMs, sleepWindowMs), args);
            }

            halfOpenProbeInFlight = true;
        }

        try {
            const result = await withTimeout(...args);

            if (state === HystrixState.HalfOpen) {
                resetCircuit();
                return result;
            }

            recordOutcome(true);
            return result;
        } catch (error) {
            if (state === HystrixState.HalfOpen) {
                openCircuit();
                return handleFallback(error, args);
            }

            recordOutcome(false);

            if (shouldOpenCircuit()) {
                openCircuit();
            }

            return handleFallback(error, args);
        } finally {
            if (state !== HystrixState.HalfOpen) {
                halfOpenProbeInFlight = false;
            }
        }
    };
}
