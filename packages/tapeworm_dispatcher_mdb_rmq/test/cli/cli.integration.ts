import { spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { expect, test } from "vitest";
import { mongo, mongoUri, rabbitUri } from "../support/services";

test("CLI closes live Mongo after invalid database initialization and exits voluntarily", async () => {
  const env = await mongo();
  try {
    const result = spawnSync(process.execPath, [resolve("dist/bin/cli.js"),
      "--mongodb-uri", mongoUri, "--database", "bad.name", "--collection", "commits",
      "--rabbitmq-uri", rabbitUri, "--exchange", "commits"], {
      encoding: "utf8", timeout: 4000, killSignal: "SIGKILL",
    });
    expect(result.stderr).toContain("Database names cannot contain the character '.'");
    expect(result.error).toBeUndefined();
    expect(result.signal).toBeNull();
    expect(result.status).toBe(1);
  } finally { await env.close(); }
});
