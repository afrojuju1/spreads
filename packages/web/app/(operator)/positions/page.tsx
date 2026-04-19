import { PositionsIndexPageContent } from "@/components/positions/positions-index";

type PositionsPageProps = {
  searchParams: Promise<{
    marketDate?: string;
    botId?: string;
    automationId?: string;
    strategyConfigId?: string;
    label?: string;
  }>;
};

export default async function PositionsPage({
  searchParams,
}: PositionsPageProps) {
  const { marketDate, botId, automationId, strategyConfigId, label } =
    await searchParams;
  return (
    <PositionsIndexPageContent
      marketDate={marketDate}
      botId={botId}
      automationId={automationId}
      strategyConfigId={strategyConfigId}
      label={label}
    />
  );
}
