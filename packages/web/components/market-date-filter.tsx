"use client";

import { format } from "date-fns";
import { Calendar as CalendarIcon } from "lucide-react";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Calendar } from "@/components/ui/calendar";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { formatCalendarDate, parseDateValue } from "@/lib/date";

type MarketDateFilterProps = {
  selectedMarketDate?: string;
  defaultMarketDate: string;
  onSelectMarketDate: (nextMarketDate?: string) => void;
};

export function MarketDateFilter({
  selectedMarketDate,
  defaultMarketDate,
  onSelectMarketDate,
}: MarketDateFilterProps) {
  const [open, setOpen] = useState(false);
  const allDatesSelected = selectedMarketDate == null;
  const selectedMarketDateValue = parseDateValue(selectedMarketDate);
  const defaultMarketDateValue = parseDateValue(defaultMarketDate);
  const triggerLabel = allDatesSelected
    ? "Pick market date"
    : formatCalendarDate(selectedMarketDate);

  function selectToday() {
    setOpen(false);
    onSelectMarketDate(defaultMarketDate);
  }

  function selectAllDates() {
    setOpen(false);
    onSelectMarketDate(undefined);
  }

  function selectDate(nextDate: Date | undefined) {
    if (!nextDate) {
      return;
    }
    setOpen(false);
    onSelectMarketDate(format(nextDate, "yyyy-MM-dd"));
  }

  return (
    <>
      <Button
        type="button"
        variant={
          !allDatesSelected && selectedMarketDate === defaultMarketDate
            ? "default"
            : "outline"
        }
        onClick={selectToday}
      >
        Today
      </Button>
      <Popover open={open} onOpenChange={setOpen}>
        <PopoverTrigger
          render={
            <Button
              type="button"
              variant="outline"
              data-empty={allDatesSelected}
              className="min-w-[12.5rem] justify-start text-left font-normal data-[empty=true]:text-muted-foreground"
            />
          }
        >
          <CalendarIcon data-icon="inline-start" />
          {triggerLabel}
        </PopoverTrigger>
        <PopoverContent align="end" className="w-auto p-0">
          <Calendar
            mode="single"
            captionLayout="dropdown"
            selected={selectedMarketDateValue ?? undefined}
            defaultMonth={selectedMarketDateValue ?? defaultMarketDateValue ?? new Date()}
            onSelect={selectDate}
          />
        </PopoverContent>
      </Popover>
      <Button
        type="button"
        variant={allDatesSelected ? "default" : "outline"}
        onClick={selectAllDates}
      >
        All dates
      </Button>
    </>
  );
}
