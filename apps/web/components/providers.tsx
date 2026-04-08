"use client";

import { QueryClient, QueryClientProvider, useQueryClient } from "@tanstack/react-query";
import { useEffect, useRef, useState } from "react";

import { buildGlobalEventsWebSocketUrl, parseGlobalRealtimeEvent } from "@/lib/api";

function GlobalRealtimeBridge() {
  const queryClient = useQueryClient();
  const reconnectTimerRef = useRef<number | null>(null);

  useEffect(() => {
    let closedByCleanup = false;
    let socket: WebSocket | null = null;

    const connect = () => {
      socket = new WebSocket(buildGlobalEventsWebSocketUrl());
      socket.onmessage = (event) => {
        if (typeof event.data !== "string") {
          return;
        }
        const realtimeEvent = parseGlobalRealtimeEvent(event.data);
        switch (realtimeEvent.topic) {
          case "generator.job.updated":
            queryClient.invalidateQueries({ queryKey: ["generator-jobs"] });
            queryClient.invalidateQueries({ queryKey: ["generator-job", realtimeEvent.entity_id] });
            break;
          case "alert.event.created":
            queryClient.invalidateQueries({ queryKey: ["alerts-latest"] });
            break;
          case "job.run.updated":
            queryClient.invalidateQueries({ queryKey: ["jobs"] });
            queryClient.invalidateQueries({ queryKey: ["job-runs"] });
            queryClient.invalidateQueries({ queryKey: ["jobs-health"] });
            break;
          case "post_market.analysis.updated":
            queryClient.invalidateQueries({ queryKey: ["session-summary"] });
            queryClient.invalidateQueries({ queryKey: ["session-tuning"] });
            break;
          default:
            break;
        }
      };
      socket.onclose = () => {
        if (closedByCleanup) {
          return;
        }
        reconnectTimerRef.current = window.setTimeout(connect, 1500);
      };
    };

    connect();

    return () => {
      closedByCleanup = true;
      if (reconnectTimerRef.current !== null) {
        window.clearTimeout(reconnectTimerRef.current);
      }
      socket?.close();
    };
  }, [queryClient]);

  return null;
}

export function Providers({ children }: { children: React.ReactNode }) {
  const [queryClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: {
          queries: {
            retry: 1,
            staleTime: 30_000,
            refetchOnWindowFocus: false,
          },
        },
      }),
  );

  return (
    <QueryClientProvider client={queryClient}>
      <GlobalRealtimeBridge />
      {children}
    </QueryClientProvider>
  );
}
