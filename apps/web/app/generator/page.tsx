import { GeneratorWorkbench } from "@/components/generator/generator-workbench";
import { parseGeneratorPageRequest } from "@/lib/generator-request";

export default async function GeneratorPage({
  searchParams,
}: {
  searchParams: Promise<{ [key: string]: string | string[] | undefined }>;
}) {
  const initialRequest = parseGeneratorPageRequest(await searchParams);
  return <GeneratorWorkbench key={JSON.stringify(initialRequest)} initialRequest={initialRequest} />;
}
