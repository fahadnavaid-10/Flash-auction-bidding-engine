import React from "react";

export const metadata = {
  title: "Flash Auction - High Concurrency Engine",
  description: "50,000 Concurrent Bidders Real-Time Engine",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body style={{ margin: 0, fontFamily: "system-ui, -apple-system, sans-serif", backgroundColor: "#0b0f19", color: "#f3f4f6" }}>
        {children}
      </body>
    </html>
  );
}
