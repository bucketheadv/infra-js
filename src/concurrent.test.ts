import test from "node:test";
import assert from "node:assert/strict";

import {
    HystrixOpenError,
    HystrixTimeoutError,
    RateLimitError,
    RateLimitMode,
    warpHystrix,
    warpOnce,
    warpQps,
} from "./concurrent";

type Deferred<T> = {
    promise: Promise<T>;
    resolve: (value: T) => void;
    reject: (reason?: unknown) => void;
};

function createDeferred<T>(): Deferred<T> {
    let resolve!: (value: T) => void;
    let reject!: (reason?: unknown) => void;

    const promise = new Promise<T>((innerResolve, innerReject) => {
        resolve = innerResolve;
        reject = innerReject;
    });

    return { promise, resolve, reject };
}

function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

test("相同参数的并发调用会复用同一个结果", async () => {
    const deferred = createDeferred<{ value: string; executionCount: number }>();
    let executionCount = 0;

    const wrapped = warpOnce(async (value: string) => {
        executionCount += 1;
        return deferred.promise;
    });

    const firstPromise = wrapped("api");
    const secondPromise = wrapped("api");

    await Promise.resolve();

    assert.equal(executionCount, 1);

    deferred.resolve({ value: "api", executionCount });

    const [firstResult, secondResult] = await Promise.all([
        firstPromise,
        secondPromise,
    ]);

    assert.deepEqual(firstResult, secondResult);
    assert.equal(firstResult.executionCount, 1);
});

test("不同参数的并发调用会分别执行", async () => {
    let executionCount = 0;

    const wrapped = warpOnce(async (value: string) => {
        executionCount += 1;
        await Promise.resolve();

        return { value, executionCount };
    });

    const [firstResult, secondResult] = await Promise.all([
        wrapped("api"),
        wrapped("worker"),
    ]);

    assert.equal(executionCount, 2);
    assert.equal(firstResult.value, "api");
    assert.equal(secondResult.value, "worker");
});

test("同一参数在上一次完成后会重新执行", async () => {
    let executionCount = 0;

    const wrapped = warpOnce(async (value: string) => {
        executionCount += 1;
        return { value, executionCount };
    });

    const firstResult = await wrapped("api");
    const secondResult = await wrapped("api");

    assert.equal(firstResult.executionCount, 1);
    assert.equal(secondResult.executionCount, 2);
});

test("对象参数即使 key 顺序不同也会命中同一个进行中请求", async () => {
    const deferred = createDeferred<number>();
    let executionCount = 0;

    const wrapped = warpOnce(async (payload: Record<string, unknown>) => {
        executionCount += 1;
        await deferred.promise;

        return payload;
    });

    const firstPromise = wrapped({ id: 1, meta: { region: "ap", stage: "dev" } });
    const secondPromise = wrapped({ meta: { stage: "dev", region: "ap" }, id: 1 });

    await Promise.resolve();

    assert.equal(executionCount, 1);

    deferred.resolve(1);

    const [firstResult, secondResult] = await Promise.all([
        firstPromise,
        secondPromise,
    ]);

    assert.deepEqual(firstResult, secondResult);
});

test("warpQps 默认使用 500 qps", async () => {
    let executionCount = 0;
    const wrapped = warpQps(async () => {
        executionCount += 1;
        return executionCount;
    });

    await wrapped();

    assert.equal(executionCount, 1);
});

test("warpQps 默认使用 failFast 模式", async () => {
    const wrapped = warpQps(async (value: string) => value, { qps: 1 });

    const firstResult = await wrapped("a");
    let caughtError: unknown;

    try {
        await wrapped("b");
    } catch (error) {
        caughtError = error;
    }

    assert.equal(firstResult, "a");
    assert.ok(caughtError instanceof RateLimitError);
    assert.equal(caughtError.mode, RateLimitMode.FailFast);
});

test("warpQps 使用令牌桶后，允许桶容量内的突发请求", async () => {
    const startTimes: number[] = [];
    const wrapped = warpQps(
        async (value: string) => {
            startTimes.push(Date.now());
            return value;
        },
        { qps: 2, mode: RateLimitMode.Delay },
    );

    const [firstResult, secondResult, thirdResult] = await Promise.all([
        wrapped("a"),
        wrapped("b"),
        wrapped("c"),
    ]);

    assert.equal(firstResult, "a");
    assert.equal(secondResult, "b");
    assert.equal(thirdResult, "c");
    assert.equal(startTimes.length, 3);
    assert.ok(startTimes[1] - startTimes[0] < 100);
    assert.ok(startTimes[2] - startTimes[1] >= 400);
});

test("warpQps 在 qps 非法时会抛错", async () => {
    assert.throws(() => {
        warpQps(async () => "ok", { qps: 0 });
    }, /qps 必须大于 0/);
});

test("warpQps 使用 failFast 模式后，超出速率会直接抛错", async () => {
    let executionCount = 0;
    const wrapped = warpQps(
        async (value: string) => {
            executionCount += 1;
            return value;
        },
        { qps: 1, mode: RateLimitMode.FailFast },
    );

    const firstResult = await wrapped("a");

    let caughtError: unknown;

    try {
        await wrapped("b");
    } catch (error) {
        caughtError = error;
    }

    assert.equal(firstResult, "a");
    assert.equal(executionCount, 1);
    assert.ok(caughtError instanceof RateLimitError);
    assert.equal(caughtError.message, "触发限流");
    assert.equal(caughtError.mode, RateLimitMode.FailFast);
    assert.equal(caughtError.qps, 1);
    assert.ok(caughtError.waitMs > 0);
});

test("warpQps 使用 delay 模式后，超出速率会等待后执行", async () => {
    const startTimes: number[] = [];
    const wrapped = warpQps(
        async (value: string) => {
            startTimes.push(Date.now());
            return value;
        },
        { qps: 2, mode: RateLimitMode.Delay },
    );

    const [firstResult, secondResult, thirdResult] = await Promise.all([
        wrapped("a"),
        wrapped("b"),
        wrapped("c"),
    ]);

    assert.equal(firstResult, "a");
    assert.equal(secondResult, "b");
    assert.equal(thirdResult, "c");
    assert.equal(startTimes.length, 3);
    assert.ok(startTimes[1] - startTimes[0] < 100);
    assert.ok(startTimes[2] - startTimes[1] >= 400);
});

test("warpHystrix 达到失败阈值后会熔断", async () => {
    let executionCount = 0;
    const wrapped = warpHystrix(
        async () => {
            executionCount += 1;
            throw new Error("upstream failed");
        },
        {
            volumeThreshold: 2,
            errorThresholdPercentage: 50,
            sleepWindowMs: 100,
            timeoutMs: 50,
        },
    );

    await assert.rejects(() => wrapped(), /upstream failed/);
    await assert.rejects(() => wrapped(), /upstream failed/);

    let caughtError: unknown;
    try {
        await wrapped();
    } catch (error) {
        caughtError = error;
    }

    assert.equal(executionCount, 2);
    assert.ok(caughtError instanceof HystrixOpenError);
    assert.ok(caughtError.retryAfterMs > 0);
});

test("warpHystrix 半开后探测成功会关闭熔断器", async () => {
    let shouldFail = true;
    let executionCount = 0;
    const wrapped = warpHystrix(
        async () => {
            executionCount += 1;

            if (shouldFail) {
                throw new Error("temporary failure");
            }

            return "ok";
        },
        {
            volumeThreshold: 2,
            errorThresholdPercentage: 50,
            sleepWindowMs: 30,
            timeoutMs: 50,
        },
    );

    await assert.rejects(() => wrapped(), /temporary failure/);
    await assert.rejects(() => wrapped(), /temporary failure/);
    await assert.rejects(() => wrapped(), HystrixOpenError);

    await sleep(40);
    shouldFail = false;

    assert.equal(await wrapped(), "ok");
    assert.equal(await wrapped(), "ok");
    assert.equal(executionCount, 4);
});

test("warpHystrix 超时会抛出 HystrixTimeoutError", async () => {
    const wrapped = warpHystrix(
        async () => {
            await sleep(30);
            return "ok";
        },
        {
            timeoutMs: 10,
            volumeThreshold: 1,
            errorThresholdPercentage: 100,
            sleepWindowMs: 50,
        },
    );

    let caughtError: unknown;
    try {
        await wrapped();
    } catch (error) {
        caughtError = error;
    }

    assert.ok(caughtError instanceof HystrixTimeoutError);
    assert.equal(caughtError.timeoutMs, 10);
});

test("warpHystrix 支持 fallback 降级结果", async () => {
    const wrapped = warpHystrix(
        async (value: string) => {
            throw new Error(`failed:${value}`);
        },
        {
            volumeThreshold: 1,
            errorThresholdPercentage: 100,
            sleepWindowMs: 50,
            timeoutMs: 20,
            fallback: async (error, value) => {
                assert.match((error as Error).message, /failed:api|熔断器处于打开状态/);
                return `fallback:${value}`;
            },
        },
    );

    assert.equal(await wrapped("api"), "fallback:api");
    assert.equal(await wrapped("api"), "fallback:api");
});
