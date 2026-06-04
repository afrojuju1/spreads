import { PositionsIndexPageContent } from "@/components/positions/positions-index";

type PositionsPageProps = {
  searchParams: Promise<{
    marketDate?: string;
    tradingStrategyId?: string;
    label?: string;
  }>;
};

export default async function PositionsPage({
  searchParams,
}: PositionsPageProps) {
  const { marketDate, tradingStrategyId, label } = await searchParams;
  return (
    <PositionsIndexPageContent
      marketDate={marketDate}
      tradingStrategyId={tradingStrategyId}
      label={label}
    />
  );
}
