import { OpportunitiesIndexPageContent } from "@/components/opportunities/opportunities-index";

type OpportunitiesPageProps = {
  searchParams: Promise<{
    marketDate?: string;
    botId?: string;
    automationId?: string;
    strategyConfigId?: string;
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
  const {
    marketDate,
    botId,
    automationId,
    strategyConfigId,
    label,
  } = await searchParams;

  return (
    <OpportunitiesIndexPageContent
      marketDate={marketDate}
      botId={botId}
      automationId={automationId}
      strategyConfigId={strategyConfigId}
      label={label}
      defaultMarketDate={chicagoTodayDate()}
    />
  );
}
