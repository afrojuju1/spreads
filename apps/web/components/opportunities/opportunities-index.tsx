"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { Radar, RefreshCw } from "lucide-react";

import { DataTable } from "@/components/data-table";
import {
  buildPipelineHref,
  executeOpportunity,
  getOpportunities,
  type Opportunity,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  LoadingState,
  MetricTile,
  SectionSurface,
} from "@/components/sessions/workspace-primitives";

const OPPORTUNITY_COLUMNS: ColumnDef<Opportunity>[] = [
  {
    accessorKey: "underlying_symbol",
    header: "Symbol",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold">{row.original.underlying_symbol}</div>
        <div className="text-xs text-muted-foreground">
          {row.original.strategy_family}
        </div>
      </div>
    ),
  },
  {
    accessorKey: "lifecycle_state",
    header: "Lifecycle",
    cell: ({ getValue }) => <Badge variant="outline">{String(getValue())}</Badge>,
  },
  {
    accessorKey: "selection_state",
    header: "Selection",
    cell: ({ getValue }) => <Badge variant="outline">{String(getValue())}</Badge>,
  },
  {
    accessorKey: "promotion_score",
    header: "Score",
    cell: ({ getValue }) => (
      <span className="font-mono">
        {typeof getValue() === "number" ? Number(getValue()).toFixed(1) : "—"}
      </span>
    ),
  },
  {
    id: "pipeline",
    header: "",
    cell: ({ row }) => (
      <Link
        href={buildPipelineHref(
          row.original.pipeline_id ?? undefined,
          row.original.market_date,
        )}
        className="text-xs text-muted-foreground underline-offset-4 hover:underline"
      >
        View pipeline
      </Link>
    ),
  },
];

export function OpportunitiesIndexPageContent() {
  const queryClient = useQueryClient();
  const opportunitiesQuery = useQuery({
    queryKey: ["opportunities"],
    queryFn: () => getOpportunities({ limit: 200 }),
  });
  const executeMutation = useMutation({
    mutationFn: (opportunityId: string) => executeOpportunity(opportunityId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["opportunities"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
      ]);
    },
  });

  if (opportunitiesQuery.isLoading) {
    return <LoadingState />;
  }

  const opportunities = opportunitiesQuery.data?.opportunities ?? [];
  const readyCount = opportunities.filter(
    (row) => row.lifecycle_state === "ready",
  ).length;

  return (
    <div className="flex flex-col gap-4">
      <div className="app-hero">
        <div className="flex flex-col gap-4 xl:flex-row xl:items-start xl:justify-between">
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <Badge
                variant="outline"
                className="rounded-full border-border/70 bg-background/80 px-2.5 py-1 text-[11px] uppercase tracking-[0.18em] text-muted-foreground"
              >
                <Radar data-icon="inline-start" />
                Opportunities
              </Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Executable ideas
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Browse the live opportunity pool across pipelines and launch an
              execution from the canonical opportunity id.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => void opportunitiesQuery.refetch()}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Opportunities"
          value={String(opportunities.length)}
          note="Current live rows"
        />
        <MetricTile
          label="Ready"
          value={String(readyCount)}
          note="Lifecycle ready"
        />
        <MetricTile
          label="Consumed"
          value={String(
            opportunities.filter((row) => row.lifecycle_state === "consumed")
              .length,
          )}
          note="Used for execution"
        />
        <MetricTile
          label="Executing"
          value={executeMutation.isPending ? "Yes" : "No"}
          note="Manual submit action"
        />
      </div>

      <SectionSurface
        title="Opportunity List"
        description="Use the pipeline workspace for the full context, or execute directly from here."
      >
        <DataTable
          columns={[
            ...OPPORTUNITY_COLUMNS,
            {
              id: "actions",
              header: "",
              cell: ({ row }) => (
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={executeMutation.isPending}
                  onClick={() => executeMutation.mutate(row.original.opportunity_id)}
                >
                  Execute
                </Button>
              ),
            },
          ]}
          data={opportunities}
          getRowId={(row) => row.opportunity_id}
          emptyMessage="No opportunities were available."
        />
      </SectionSurface>
    </div>
  );
}
