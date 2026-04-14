import { OpportunitiesIndexPageContent } from "@/components/opportunities/opportunities-index";

type OpportunitiesPageProps = {
  searchParams: Promise<{
    marketDate?: string;
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
  const { marketDate } = await searchParams;

  return (
    <OpportunitiesIndexPageContent
      marketDate={marketDate}
      defaultMarketDate={chicagoTodayDate()}
    />
  );
}
