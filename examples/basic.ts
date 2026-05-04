import { warpOnce } from "../src";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main(): Promise<void> {
  let executedCount = 0;

  const loadConfig = warpOnce(async (service: string, version: number) => {
    executedCount += 1;
    console.log(
      `[原始方法执行] 服务=${service}，版本=${version}，执行次数=${executedCount}`,
    );
    await sleep(100);

    return {
      service,
      version,
      executedCount,
    };
  });

  const [first, second, third] = await Promise.all([
    loadConfig("api", 1),
    loadConfig("api", 1),
    loadConfig("worker", 1),
  ]);

  console.log("相同参数共享结果：", first, second);
  console.log("不同参数独立执行：", third);
}

void main();
