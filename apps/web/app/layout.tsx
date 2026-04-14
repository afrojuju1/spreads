import type { Metadata } from "next";
import { IBM_Plex_Mono, Space_Grotesk } from "next/font/google";

import { LayoutHeader } from "@/components/layout-header";
import { LayoutSidebar } from "@/components/layout-sidebar";
import { Providers } from "@/components/providers";
import "./globals.css";

const sans = Space_Grotesk({
  variable: "--font-sans",
  subsets: ["latin"],
});

const mono = IBM_Plex_Mono({
  variable: "--font-mono",
  subsets: ["latin"],
  weight: ["400", "500"],
});

export const metadata: Metadata = {
  title: "Spreads Operator",
  description: "Session and account runtime console for spreads.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html
      lang="en"
      className={`${sans.variable} ${mono.variable} h-full antialiased`}
    >
      <body className="min-h-full font-sans">
        <Providers>
          <div className="min-h-dvh lg:flex">
            <LayoutSidebar />
            <div className="min-w-0 flex-1">
              <LayoutHeader />
              {children}
            </div>
          </div>
        </Providers>
      </body>
    </html>
  );
}
