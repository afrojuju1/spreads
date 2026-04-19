import type { ReactNode } from "react";

import { LayoutHeader } from "@/components/layout-header";
import { LayoutSidebar } from "@/components/layout-sidebar";

export default function OperatorLayout({
  children,
}: {
  children: ReactNode;
}) {
  return (
    <div className="min-h-dvh lg:flex">
      <LayoutSidebar />
      <div className="min-w-0 flex-1">
        <LayoutHeader />
        <main className="mx-auto w-full max-w-[1680px] px-4 py-6 lg:px-6">
          {children}
        </main>
      </div>
    </div>
  );
}
