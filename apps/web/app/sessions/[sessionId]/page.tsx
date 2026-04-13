import { SessionDetailPageContent } from "@/components/sessions/session-detail";

export default async function SessionDetailPage({
  params,
}: {
  params: Promise<{ sessionId: string }>;
}) {
  const { sessionId } = await params;
  return <SessionDetailPageContent sessionId={sessionId} />;
}
