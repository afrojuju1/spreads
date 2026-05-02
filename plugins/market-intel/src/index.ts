import { spawn } from "node:child_process";
import { appendFile } from "node:fs/promises";
import { resolve } from "node:path";

import { definePluginEntry } from "openclaw/plugin-sdk/plugin-entry";

type RunParams = {
  ticker: string;
  asOf?: string;
  sources?: string;
  noLlm?: boolean;
  refresh?: boolean;
  cwd?: string;
};

export default definePluginEntry({
  id: "market-intel",
  name: "Market Intel",
  description: "Evidence-backed market intelligence workflow tools.",
  register(api) {
    api.registerTool({
      name: "market_intel_run",
      description: "Create a file-backed market-intel run for a single ticker.",
      parameters: {
        type: "object",
        additionalProperties: false,
        required: ["ticker"],
        properties: {
          ticker: { type: "string", minLength: 1 },
          asOf: { type: "string", description: "YYYY-MM-DD as-of date." },
          sources: {
            type: "string",
            default: "sec,market",
            description: "Comma-separated source adapters.",
          },
          noLlm: { type: "boolean", default: true },
          refresh: { type: "boolean", default: false },
          cwd: {
            type: "string",
            description: "Spreads repo checkout. Defaults to SPREADS_WORKSPACE or the current process cwd.",
          },
        },
      },
      async execute(_id, rawParams) {
        const params = rawParams as RunParams;
        const cwd = params.cwd || process.env.SPREADS_WORKSPACE || process.cwd();
        const args = [
          "run",
          "spreads",
          "market-intel",
          "thesis",
          "--ticker",
          params.ticker.toUpperCase(),
          "--sources",
          params.sources || "sec,market",
          "--json",
        ];

        if (params.asOf) {
          args.push("--as-of", params.asOf);
        }
        if (params.noLlm ?? true) {
          args.push("--no-llm");
        }
        if (params.refresh) {
          args.push("--refresh");
        }

        const result = await runProcess("uv", args, cwd);
        await appendRunHookTrace(result, cwd, {
          event: "plugin_tool_completed",
          tool: "market_intel_run",
          ticker: params.ticker.toUpperCase(),
          sources: params.sources || "sec,market",
          noLlm: params.noLlm ?? true,
        });
        return {
          content: [
            {
              type: "text",
              text: result,
            },
          ],
        };
      },
    });

    api.on(
      "before_prompt_build",
      async (event) => {
        const prompt = String((event as { prompt?: unknown }).prompt ?? "");
        if (!/\bmarket[-_\s]?intel\b/i.test(prompt)) {
          return;
        }
        return {
          appendSystemContext:
            "Market Intel mode: keep claims evidence-backed, prefer SEC and market snapshot sources first, write durable run artifacts under outputs/market_intel, and use the configured Alpaca MCP server only in paper mode unless the operator explicitly changes that policy.",
        };
      },
      { priority: 25 },
    );
  },
});

function runProcess(command: string, args: string[], cwd: string): Promise<string> {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      env: process.env,
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.on("error", reject);
    child.on("close", (code) => {
      if (code === 0) {
        resolve(stdout.trim() || "{}");
        return;
      }
      reject(new Error(stderr.trim() || `${command} exited with ${code}`));
    });
  });
}

async function appendRunHookTrace(
  result: string,
  cwd: string,
  payload: Record<string, unknown>,
): Promise<void> {
  try {
    const parsed = JSON.parse(result) as { run_dir?: string };
    if (!parsed.run_dir) {
      return;
    }
    const hooksPath = resolve(cwd, parsed.run_dir, "hooks.jsonl");
    const record = {
      logged_at: new Date().toISOString(),
      ...payload,
    };
    await appendFile(hooksPath, `${JSON.stringify(record)}\n`);
  } catch {
    return;
  }
}
