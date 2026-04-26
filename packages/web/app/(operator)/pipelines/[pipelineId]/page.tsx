import { PipelineDetailPageContent } from "@/components/pipelines/pipeline-detail";

type PipelineDetailPageProps = {
  params: Promise<{
    pipelineId: string;
  }>;
  searchParams: Promise<{
    marketDate?: string;
    cycleId?: string;
  }>;
};

export default async function PipelineDetailPage({
  params,
  searchParams,
}: PipelineDetailPageProps) {
  const { pipelineId } = await params;
  const { marketDate, cycleId } = await searchParams;

  return (
    <PipelineDetailPageContent
      pipelineId={decodeURIComponent(pipelineId)}
      marketDate={marketDate}
      cycleId={cycleId}
    />
  );
}
