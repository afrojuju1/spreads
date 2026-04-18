import { AutomationDetailPageContent } from "@/components/automations/automation-detail";

type AutomationDetailPageProps = {
  params: Promise<{
    botId: string;
    automationId: string;
  }>;
  searchParams: Promise<{
    marketDate?: string;
  }>;
};

export default async function AutomationDetailPage({
  params,
  searchParams,
}: AutomationDetailPageProps) {
  const { botId, automationId } = await params;
  const { marketDate } = await searchParams;

  return (
    <AutomationDetailPageContent
      botId={decodeURIComponent(botId)}
      automationId={decodeURIComponent(automationId)}
      marketDate={marketDate}
    />
  );
}
