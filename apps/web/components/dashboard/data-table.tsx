"use client";

import {
  ColumnDef,
  SortingState,
  flexRender,
  getCoreRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { useState } from "react";

import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";

type DataTableProps<TData> = {
  columns: ColumnDef<TData>[];
  data: TData[];
  emptyMessage: string;
  getRowId?: (row: TData) => string;
  onSelect?: (row: TData) => void;
  selectedId?: string | null;
};

export function DataTable<TData>({
  columns,
  data,
  emptyMessage,
  getRowId,
  onSelect,
  selectedId,
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>([]);
  // TanStack Table owns its own instance lifecycle here; this component intentionally opts out
  // of compiler memoization assumptions around table function identity.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
    },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getRowId,
  });

  return (
    <Table className="text-[13px]">
      <TableHeader>
        {table.getHeaderGroups().map((headerGroup) => (
          <TableRow key={headerGroup.id} className="border-border/70 hover:bg-transparent">
            {headerGroup.headers.map((header) => (
              <TableHead key={header.id} className="h-9 px-3 text-[11px] uppercase tracking-[0.18em] text-muted-foreground">
                {header.isPlaceholder
                  ? null
                  : flexRender(header.column.columnDef.header, header.getContext())}
              </TableHead>
            ))}
          </TableRow>
        ))}
      </TableHeader>
      <TableBody>
        {table.getRowModel().rows.length ? (
          table.getRowModel().rows.map((row) => {
            const rowId = selectedId ?? "";
            const isSelected = rowId && row.id === rowId;

            return (
              <TableRow
                key={row.id}
                data-state={isSelected ? "selected" : undefined}
                className={cn(
                  "border-border/60",
                  onSelect ? "cursor-pointer" : "",
                  isSelected ? "bg-accent/60" : "hover:bg-accent/30",
                )}
                onClick={onSelect ? () => onSelect(row.original) : undefined}
              >
                {row.getVisibleCells().map((cell) => (
                  <TableCell key={cell.id} className="px-3 py-2">
                    {flexRender(cell.column.columnDef.cell, cell.getContext())}
                  </TableCell>
                ))}
              </TableRow>
            );
          })
        ) : (
          <TableRow className="border-border/60">
            <TableCell colSpan={columns.length} className="px-3 py-8 text-center text-sm text-muted-foreground">
              {emptyMessage}
            </TableCell>
          </TableRow>
        )}
      </TableBody>
    </Table>
  );
}
