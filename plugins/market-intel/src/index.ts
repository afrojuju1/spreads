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

type EvalParams = {
  tickers?: string;
  asOf?: string;
  sources?: string;
  noLlm?: boolean;
  cwd?: string;
};

export default definePluginEntry({
  id: "market-intel",
  name: "Market Intel",
  description: "Evidence-backed market intelligence workflow tools.",
  register(api) {
    api.registerGatewayMethod("marketIntel.run", async ({ params, respond }) => {
      try {
        const runParams = normalizeGatewayParams(params);
        const result = await executeMarketIntelRun(runParams);
        respond(true, JSON.parse(result.stdout));
      } catch (error) {
        respond(false, {
          error: formatError(error),
        });
      }
    });

    api.registerGatewayMethod("marketIntel.eval", async ({ params, respond }) => {
      try {
        const evalParams = normalizeEvalGatewayParams(params);
        const result = await executeMarketIntelEval(evalParams);
        respond(true, JSON.parse(result.stdout));
      } catch (error) {
        respond(false, {
          error: formatError(error),
        });
      }
    });

    api.registerCommand({
      name: "market-intel",
      nativeNames: { default: "market-intel" },
      nativeProgressMessages: { default: "Running market intel..." },
      description: "Run an evidence-backed market-intel thesis for one ticker.",
      acceptsArgs: true,
      agentPromptGuidance: [
        "Use /market-intel TICKER for deterministic market-intel runs before drafting unsupported claims.",
      ],
      async handler(ctx) {
        try {
          const params = parseCommandArgs(ctx.args || "");
          const result = await executeMarketIntelRun(params);
          const payload = JSON.parse(result.stdout) as {
            run_id?: string;
            ticker?: string;
            as_of?: string;
            status?: string;
            run_dir?: string;
            warnings?: string[];
          };
          return { text: formatCommandResult(payload) };
        } catch (error) {
          return { text: `market-intel failed: ${formatError(error)}` };
        }
      },
    });

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
        const result = await executeMarketIntelRun(params);
        await appendRunHookTrace(result.stdout, result.cwd, {
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
              text: result.stdout,
            },
          ],
        };
      },
    });

    api.registerTool({
      name: "market_intel_eval",
      description: "Run the market-intel eval harness.",
      parameters: {
        type: "object",
        additionalProperties: false,
        properties: {
          tickers: {
            type: "string",
            default: "SOFI",
            description: "Comma-separated tickers to evaluate.",
          },
          asOf: { type: "string", default: "2026-05-01" },
          sources: { type: "string", default: "sec,market" },
          noLlm: { type: "boolean", default: false },
          cwd: {
            type: "string",
            description: "Spreads repo checkout. Defaults to SPREADS_WORKSPACE or the current process cwd.",
          },
        },
      },
      async execute(_id, rawParams) {
        const params = rawParams as EvalParams;
        const result = await executeMarketIntelEval(params);
        return {
          content: [
            {
              type: "text",
              text: result.stdout,
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

type RunResult = {
  stdout: string;
  cwd: string;
};

async function executeMarketIntelRun(params: RunParams): Promise<RunResult> {
  const cwd = params.cwd || process.env.SPREADS_WORKSPACE || process.cwd();
  const normalized = {
    ...params,
    ticker: params.ticker.toUpperCase(),
    sources: params.sources || "sec,market",
    noLlm: params.noLlm ?? true,
  };
  const args = [
    "run",
    "spreads",
    "market-intel",
    "thesis",
    "--ticker",
    normalized.ticker,
    "--sources",
    normalized.sources,
    "--json",
  ];

  if (normalized.asOf) {
    args.push("--as-of", normalized.asOf);
  }
  if (normalized.noLlm) {
    args.push("--no-llm");
  }
  if (normalized.refresh) {
    args.push("--refresh");
  }

  const stdout = await runProcess("uv", args, cwd);
  await appendRunHookTrace(stdout, cwd, {
    event: "plugin_run_completed",
    ticker: normalized.ticker,
    sources: normalized.sources,
    noLlm: normalized.noLlm,
  });
  return { stdout, cwd };
}

async function executeMarketIntelEval(params: EvalParams): Promise<RunResult> {
  const cwd = params.cwd || process.env.SPREADS_WORKSPACE || process.cwd();
  const normalized = {
    ...params,
    tickers: params.tickers || "SOFI",
    sources: params.sources || "sec,market",
    noLlm: params.noLlm ?? false,
  };
  const args = [
    "run",
    "spreads",
    "market-intel",
    "eval",
    "--tickers",
    normalized.tickers,
    "--sources",
    normalized.sources,
    "--json",
  ];
  if (normalized.asOf) {
    args.push("--as-of", normalized.asOf);
  }
  if (normalized.noLlm) {
    args.push("--no-llm");
  }
  const stdout = await runProcess("uv", args, cwd);
  return { stdout, cwd };
}

function normalizeGatewayParams(params: Record<string, unknown>): RunParams {
  const ticker = normalizeString(params.ticker);
  if (!ticker) {
    throw new Error("ticker is required");
  }
  return {
    ticker,
    asOf: normalizeString(params.asOf) || normalizeString(params.as_of),
    sources: normalizeString(params.sources),
    noLlm: normalizeBoolean(params.noLlm ?? params.no_llm, true),
    refresh: normalizeBoolean(params.refresh, false),
    cwd: normalizeString(params.cwd),
  };
}

function normalizeEvalGatewayParams(params: Record<string, unknown>): EvalParams {
  return {
    tickers: normalizeString(params.tickers) || normalizeString(params.ticker),
    asOf: normalizeString(params.asOf) || normalizeString(params.as_of),
    sources: normalizeString(params.sources),
    noLlm: normalizeBoolean(params.noLlm ?? params.no_llm, false),
    cwd: normalizeString(params.cwd),
  };
}

function parseCommandArgs(rawArgs: string): RunParams {
  const tokens = rawArgs.trim().split(/\s+/).filter(Boolean);
  const params: RunParams = {
    ticker: "",
    sources: "sec,market",
    noLlm: true,
  };
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (token === "--as-of") {
      params.asOf = tokens[index + 1];
      index += 1;
    } else if (token === "--sources") {
      params.sources = tokens[index + 1];
      index += 1;
    } else if (token === "--refresh") {
      params.refresh = true;
    } else if (token === "--llm") {
      params.noLlm = false;
    } else if (token === "--no-llm") {
      params.noLlm = true;
    } else if (!params.ticker) {
      params.ticker = token;
    } else if (!params.asOf && /^\d{4}-\d{2}-\d{2}$/.test(token)) {
      params.asOf = token;
    } else {
      throw new Error(`unexpected argument: ${token}`);
    }
  }
  if (!params.ticker) {
    throw new Error("usage: /market-intel TICKER [--as-of YYYY-MM-DD] [--sources sec,market] [--refresh] [--llm]");
  }
  return params;
}

function formatCommandResult(payload: {
  run_id?: string;
  ticker?: string;
  as_of?: string;
  status?: string;
  run_dir?: string;
  warnings?: string[];
}): string {
  const lines = [
    `market-intel ${payload.ticker || "run"} complete`,
    `- run_id: ${payload.run_id || "unknown"}`,
    `- as_of: ${payload.as_of || "unknown"}`,
    `- status: ${payload.status || "unknown"}`,
    `- run_dir: ${payload.run_dir || "unknown"}`,
  ];
  if (payload.warnings?.length) {
    lines.push(`- warnings: ${payload.warnings.join("; ")}`);
  }
  return lines.join("\n");
}

function normalizeString(value: unknown): string | undefined {
  if (typeof value !== "string") {
    return undefined;
  }
  const trimmed = value.trim();
  return trimmed || undefined;
}

function normalizeBoolean(value: unknown, fallback: boolean): boolean {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    if (/^(true|1|yes)$/i.test(value)) {
      return true;
    }
    if (/^(false|0|no)$/i.test(value)) {
      return false;
    }
  }
  return fallback;
}

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

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return String(error);
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
