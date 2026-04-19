import { PipelineDetailPageContent } from "@/components/pipelines/pipeline-detail";

type PipelineDetailPageProps = {
  params: Promise<{
    pipelineId: string;
  }>;
  searchParams: Promise<{
    marketDate?: string;
  }>;
};

export default async function PipelineDetailPage({
  params,
  searchParams,
}: PipelineDetailPageProps) {
  const { pipelineId } = await params;
  const { marketDate } = await searchParams;

  return (
    <PipelineDetailPageContent
      pipelineId={decodeURIComponent(pipelineId)}
      marketDate={marketDate}
    />
  );
}
