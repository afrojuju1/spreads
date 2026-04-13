import type { ReactNode } from "react";

export default function SessionsLayout({
  children,
}: {
  children: ReactNode;
}) {
  return (
    <main className="mx-auto max-w-[1680px] px-4 py-6 lg:px-6">
      {children}
    </main>
  );
}
