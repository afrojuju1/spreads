import { OpportunitiesIndexPageContent } from "@/components/opportunities/opportunities-index";

type OpportunitiesPageProps = {
  searchParams: Promise<{
    marketDate?: string;
    tradingStrategyId?: string;
    label?: string;
  }>;
};

function chicagoTodayDate(): string {
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Chicago",
  }).format(new Date());
}

export default async function OpportunitiesPage({
  searchParams,
}: OpportunitiesPageProps) {
  const { marketDate, tradingStrategyId, label } = await searchParams;

  return (
    <OpportunitiesIndexPageContent
      marketDate={marketDate}
      tradingStrategyId={tradingStrategyId}
      label={label}
      defaultMarketDate={chicagoTodayDate()}
    />
  );
}
