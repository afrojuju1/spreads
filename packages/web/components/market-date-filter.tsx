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
  selectedMarketDate: string;
  defaultMarketDate: string;
  onSelectMarketDate: (nextMarketDate: string) => void;
};

export function MarketDateFilter({
  selectedMarketDate,
  defaultMarketDate,
  onSelectMarketDate,
}: MarketDateFilterProps) {
  const [open, setOpen] = useState(false);
  const selectedMarketDateValue = parseDateValue(selectedMarketDate);
  const defaultMarketDateValue = parseDateValue(defaultMarketDate);
  const triggerLabel = formatCalendarDate(selectedMarketDate);

  function selectToday() {
    setOpen(false);
    onSelectMarketDate(defaultMarketDate);
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
        variant={selectedMarketDate === defaultMarketDate ? "default" : "outline"}
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
              className="min-w-[12.5rem] justify-start text-left font-normal"
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
    </>
  );
}
