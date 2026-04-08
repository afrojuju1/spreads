import { GeneratorJobDetail } from "@/components/generator/generator-job-detail";

export default async function GeneratorJobDetailPage({
  params,
}: {
  params: Promise<{ generatorJobId: string }>;
}) {
  const { generatorJobId } = await params;
  return <GeneratorJobDetail generatorJobId={generatorJobId} />;
}
