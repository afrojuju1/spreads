"use client";

import {
  ColumnDef,
  PaginationState,
  SortingState,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
} from "@tanstack/react-table";
import { useEffect, useState } from "react";

import { Button } from "@/components/ui/button";
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
  pageSize?: number;
};

export function DataTable<TData>({
  columns,
  data,
  emptyMessage,
  getRowId,
  onSelect,
  selectedId,
  pageSize = 30,
}: DataTableProps<TData>) {
  const [sorting, setSorting] = useState<SortingState>([]);
  const [pagination, setPagination] = useState<PaginationState>({
    pageIndex: 0,
    pageSize,
  });

  useEffect(() => {
    setPagination((current) =>
      current.pageSize === pageSize
        ? current
        : {
            pageIndex: 0,
            pageSize,
          },
    );
  }, [pageSize]);

  useEffect(() => {
    setPagination((current) => {
      const maxPageIndex = Math.max(Math.ceil(data.length / current.pageSize) - 1, 0);
      if (current.pageIndex <= maxPageIndex) {
        return current;
      }
      return {
        ...current,
        pageIndex: maxPageIndex,
      };
    });
  }, [data.length]);

  // TanStack Table owns its own instance lifecycle here; this component intentionally opts out
  // of compiler memoization assumptions around table function identity.
  // eslint-disable-next-line react-hooks/incompatible-library
  const table = useReactTable({
    data,
    columns,
    state: {
      sorting,
      pagination,
    },
    onSortingChange: setSorting,
    onPaginationChange: setPagination,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    getRowId,
  });

  const totalRows = table.getPrePaginationRowModel().rows.length;
  const pageIndex = table.getState().pagination.pageIndex;
  const currentPageSize = table.getState().pagination.pageSize;
  const visibleRowCount = table.getRowModel().rows.length;
  const pageStart = totalRows === 0 ? 0 : pageIndex * currentPageSize + 1;
  const pageEnd = totalRows === 0 ? 0 : pageStart + visibleRowCount - 1;
  const totalPages = Math.max(table.getPageCount(), 1);

  return (
    <div className="flex flex-col gap-3">
      <Table className="text-[13px]">
        <TableHeader>
          {table.getHeaderGroups().map((headerGroup) => (
            <TableRow key={headerGroup.id} className="border-border/70 hover:bg-transparent">
              {headerGroup.headers.map((header) => (
                <TableHead
                  key={header.id}
                  className="h-9 px-3 text-[11px] uppercase tracking-[0.18em] text-muted-foreground"
                >
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
      {totalRows > 0 ? (
        <div className="flex flex-col gap-2 border-t border-border/70 pt-3 text-xs text-muted-foreground md:flex-row md:items-center md:justify-between">
          <div className="font-mono">
            {pageStart}-{pageEnd} of {totalRows}
          </div>
          <div className="flex items-center gap-2">
            <div className="font-mono">
              Page {pageIndex + 1} of {totalPages}
            </div>
            <Button
              type="button"
              variant="outline"
              size="xs"
              disabled={!table.getCanPreviousPage()}
              onClick={() => table.previousPage()}
            >
              Previous
            </Button>
            <Button
              type="button"
              variant="outline"
              size="xs"
              disabled={!table.getCanNextPage()}
              onClick={() => table.nextPage()}
            >
              Next
            </Button>
          </div>
        </div>
      ) : null}
    </div>
  );
}
