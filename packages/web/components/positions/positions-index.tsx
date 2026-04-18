"use client";

import Link from "next/link";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { ColumnDef } from "@tanstack/react-table";
import { BriefcaseBusiness, RefreshCw } from "lucide-react";

import { DataTable } from "@/components/data-table";
import {
  buildAutomationHref,
  buildPipelineHref,
  closePosition,
  getPositions,
  type Position,
} from "@/lib/api";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  formatNullableCurrency,
  formatQuantity,
  formatSignedCurrency,
  LoadingState,
  MetricTile,
  readString,
  SectionSurface,
} from "@/components/sessions/workspace-primitives";

function positionRecord(position: Position): Record<string, unknown> {
  return position as Record<string, unknown>;
}

function readRecord(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
}

function getPositionOwner(position: Position): Record<string, unknown> {
  return readRecord(positionRecord(position).owner);
}

function getPositionDiscovery(position: Position): Record<string, unknown> {
  return readRecord(positionRecord(position).discovery);
}

function hasPositionAutomationOwner(position: Position): boolean {
  const owner = getPositionOwner(position);
  return Boolean(
    typeof owner.bot_id === "string" &&
      owner.bot_id &&
      typeof owner.automation_id === "string" &&
      owner.automation_id,
  );
}

function getPositionAutomationLabel(position: Position): string {
  const owner = getPositionOwner(position);
  const botId = readString(owner.bot_id, "");
  const automationId = readString(owner.automation_id, "");
  if (botId && automationId) {
    return `${botId} / ${automationId}`;
  }
  return "—";
}

function getPositionDiscoveryLabel(position: Position): string {
  const discovery = getPositionDiscovery(position);
  return readString(discovery.label, readString(position.pipeline_id));
}

function getPositionAutomationHref(position: Position): string {
  const owner = getPositionOwner(position);
  const botId = typeof owner.bot_id === "string" ? owner.bot_id : null;
  const automationId =
    typeof owner.automation_id === "string" ? owner.automation_id : null;
  return buildAutomationHref(botId, automationId, position.market_date);
}

function getPositionDiscoveryHref(position: Position): string {
  const discovery = getPositionDiscovery(position);
  const pipelineId =
    typeof discovery.pipeline_id === "string"
      ? discovery.pipeline_id
      : position.pipeline_id;
  return buildPipelineHref(pipelineId, position.market_date);
}

const POSITION_COLUMNS: ColumnDef<Position>[] = [
  {
    accessorKey: "root_symbol",
    header: "Position",
    cell: ({ row }) => (
      <div>
        <div className="font-semibold">{row.original.root_symbol}</div>
        <div className="text-xs text-muted-foreground">
          {row.original.strategy_family}
        </div>
        {hasPositionAutomationOwner(row.original) ? (
          <Link
            href={getPositionAutomationHref(row.original)}
            className="mt-1 inline-block text-xs text-foreground underline-offset-4 hover:underline"
          >
            {getPositionAutomationLabel(row.original)}
          </Link>
        ) : null}
        <Link
          href={getPositionDiscoveryHref(row.original)}
          className="mt-1 inline-block text-xs text-muted-foreground underline-offset-4 hover:underline"
        >
          Discovery · {getPositionDiscoveryLabel(row.original)}
        </Link>
      </div>
    ),
  },
  {
    accessorKey: "position_status",
    header: "Status",
    cell: ({ getValue }) => <Badge variant="outline">{String(getValue())}</Badge>,
  },
  {
    accessorKey: "remaining_quantity",
    header: "Remaining",
    cell: ({ getValue }) => (
      <span className="font-mono">{formatQuantity(getValue() as number)}</span>
    ),
  },
  {
    accessorKey: "realized_pnl",
    header: "Realized",
    cell: ({ getValue }) => formatSignedCurrency(getValue() as number | null | undefined),
  },
  {
    accessorKey: "unrealized_pnl",
    header: "Unrealized",
    cell: ({ getValue }) => formatSignedCurrency(getValue() as number | null | undefined),
  },
  {
    id: "pipeline",
    header: "",
    cell: ({ row }) => (
      <span className="text-xs text-muted-foreground">
        {hasPositionAutomationOwner(row.original)
          ? "Automation-owned"
          : "Discovery-owned"}
      </span>
    ),
  },
];

export function PositionsIndexPageContent() {
  const queryClient = useQueryClient();
  const positionsQuery = useQuery({
    queryKey: ["positions"],
    queryFn: () => getPositions({ limit: 200 }),
  });
  const closeMutation = useMutation({
    mutationFn: (positionId: string) => closePosition(positionId, {}),
    onSuccess: async () => {
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["automations"] }),
        queryClient.invalidateQueries({ queryKey: ["positions"] }),
        queryClient.invalidateQueries({ queryKey: ["pipelines"] }),
      ]);
    },
  });

  if (positionsQuery.isLoading) {
    return <LoadingState />;
  }

  const positions = positionsQuery.data?.positions ?? [];
  const summary = positionsQuery.data?.summary ?? {};

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
                <BriefcaseBusiness data-icon="inline-start" />
                Positions
              </Badge>
            </div>
            <div className="mt-4 text-3xl font-semibold tracking-[0.02em]">
              Open risk inventory
            </div>
            <div className="mt-2 text-sm text-foreground/70">
              Inspect all runtime-owned positions, jump to the owning
              automation, and keep discovery lineage visible but secondary.
            </div>
          </div>
          <Button
            type="button"
            variant="outline"
            onClick={() => void positionsQuery.refetch()}
          >
            <RefreshCw data-icon="inline-start" />
            Refresh
          </Button>
        </div>
      </div>

      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <MetricTile
          label="Positions"
          value={String(summary.position_count ?? 0)}
          note="Current runtime inventory"
        />
        <MetricTile
          label="Open"
          value={String(summary.open_position_count ?? 0)}
          note="Open or partial"
        />
        <MetricTile
          label="Closed"
          value={String(summary.closed_position_count ?? 0)}
          note="Completed exits"
        />
        <MetricTile
          label="Running PnL"
          value={formatNullableCurrency(
            positions.reduce(
              (total, row) => total + (row.net_pnl ?? 0),
              0,
            ),
          )}
          note={closeMutation.isPending ? "Close queued" : "Live estimate"}
        />
      </div>

      <SectionSurface
        title="Position List"
        description="Use the automation runtime for owner context, or close directly from here."
      >
        <DataTable
          columns={[
            ...POSITION_COLUMNS,
            {
              id: "actions",
              header: "",
              cell: ({ row }) => (
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={closeMutation.isPending || row.original.position_status === "closed"}
                  onClick={() => closeMutation.mutate(row.original.position_id)}
                >
                  Close
                </Button>
              ),
            },
          ]}
          data={positions}
          getRowId={(row) => row.position_id}
          emptyMessage="No positions were available."
        />
      </SectionSurface>
    </div>
  );
}
