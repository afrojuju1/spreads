import { NextRequest, NextResponse } from "next/server";

const BACKEND_API_BASE_URL =
  process.env.SPREADS_API_BASE_URL?.replace(/\/$/, "") ?? "http://localhost:58080";

export const dynamic = "force-dynamic";

async function forward(request: NextRequest, paramsPromise: Promise<{ path: string[] }>) {
  const { path } = await paramsPromise;
  const upstreamUrl = new URL(`${BACKEND_API_BASE_URL}/${path.join("/")}`);

  for (const [key, value] of request.nextUrl.searchParams.entries()) {
    upstreamUrl.searchParams.append(key, value);
  }

  const response = await fetch(upstreamUrl, {
    method: request.method,
    headers: {
      accept: request.headers.get("accept") ?? "application/json",
    },
    cache: "no-store",
  });

  return new NextResponse(response.body, {
    status: response.status,
    headers: {
      "content-type": response.headers.get("content-type") ?? "application/json",
    },
  });
}

export async function GET(
  request: NextRequest,
  context: { params: Promise<{ path: string[] }> },
) {
  return forward(request, context.params);
}
