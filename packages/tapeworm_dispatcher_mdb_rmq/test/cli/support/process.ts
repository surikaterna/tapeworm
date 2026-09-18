import { spawn } from "node:child_process";
import { resolve } from "node:path";
import { mongoUri, rabbitUri } from "../../support/services";

export function launchCli(database: string, exchange: string) {
  const child = spawn(process.execPath, [resolve("dist/bin/cli.js"),
    "--mongodb-uri", mongoUri, "--database", database, "--collection", "commits",
    "--rabbitmq-uri", rabbitUri, "--exchange", exchange, "--resume-collection", "checkpoint",
    "--checkpoint-key", "shutdown-test", "--feed-id", "shutdown-test"], { stdio: ["ignore", "pipe", "pipe"] });
  let stderr = "";
  child.stdout.resume();
  child.stderr.on("data", (value: unknown) => { if (Buffer.isBuffer(value)) stderr += value.toString(); });
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((yes, no) => {
    child.once("error", no);
    child.once("exit", (code, signal) => { yes({ code, signal }); });
  });
  void exited.catch(() => {});
  return { child, exited, stderr: () => stderr };
}

export type CliProcess = ReturnType<typeof launchCli>;

export async function exitWithin(process: CliProcess, ms: number) {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      process.child.kill("SIGKILL");
      reject(new Error(`Parent watchdog killed CLI after ${ms}ms: ${process.stderr()}`));
    }, ms);
  });
  try { return await Promise.race([process.exited, timeout]); }
  finally { clearTimeout(timer); }
}

export async function cleanupCli(process: CliProcess): Promise<void> {
  if (process.child.exitCode !== null || process.child.signalCode !== null) return;
  process.child.kill("SIGKILL");
  await exitWithin(process, 5000);
}
