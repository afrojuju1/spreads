import { ManualScanPageContent } from "@/components/pipelines/manual-scan-page";

type ScanPageProps = {
  searchParams: Promise<{
    pipelineId?: string;
    cycleId?: string;
  }>;
};

export default async function ScanPage({ searchParams }: ScanPageProps) {
  const { pipelineId, cycleId } = await searchParams;

  return (
    <ManualScanPageContent
      initialPipelineId={pipelineId}
      initialCycleId={cycleId}
    />
  );
}
