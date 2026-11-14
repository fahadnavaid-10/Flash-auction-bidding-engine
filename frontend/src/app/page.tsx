"use client";

import React, { useState, useEffect, useRef } from "react";

interface AuctionState {
  auction_id: string;
  item_name: string;
  highest_bid: number;
  highest_bidder: string;
  min_increment: number;
  time_remaining: number;
  end_time: number;
  status: string;
  extended?: boolean;
}

interface BidLog {
  id: string;
  user: string;
  amount: number;
  time: string;
  status: "CONFIRMED" | "OUTBID" | "REJECTED";
}

export default function AuctionRoom() {
  const [auction, setAuction] = useState<AuctionState>({
    auction_id: "auction_alpha",
    item_name: "Vintage Rolex Daytona 1968",
    highest_bid: 100.0,
    highest_bidder: "reserve_bot",
    min_increment: 5.0,
    time_remaining: 300.0,
    end_time: Date.now() / 1000 + 300,
    status: "ACTIVE",
  });

  const [connected, setConnected] = useState<boolean>(false);
  const [reconnecting, setReconnecting] = useState<boolean>(false);
  const [customBid, setCustomBid] = useState<string>("");
  const [myUserId] = useState<string>(() => "bidder_" + Math.random().toString(36).substring(2, 7));
  const [bidLogs, setBidLogs] = useState<BidLog[]>([]);
  const [antiSnipeAlert, setAntiSnipeAlert] = useState<string | null>(null);
  const [flashing, setFlashing] = useState<boolean>(false);

  const socketRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<any>(null);

  const connectWebSocket = () => {
    const wsUrl = `ws://${window.location.hostname || "localhost"}:8000/ws/auction_alpha`;
    const ws = new WebSocket(wsUrl);
    socketRef.current = ws;

    ws.onopen = () => {
      setConnected(true);
      setReconnecting(false);
      console.log("[WS] Connected to Auction Gateway");
    };

    ws.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);

        if (data.type === "INITIAL_STATE" || data.type === "BID_ACCEPTED") {
          setAuction((prev) => {
            if (data.highest_bid > prev.highest_bid) {
              setFlashing(true);
              setTimeout(() => setFlashing(false), 400);
            }
            return {
              ...prev,
              highest_bid: data.highest_bid,
              highest_bidder: data.highest_bidder,
              end_time: data.end_time || prev.end_time,
              status: data.status || prev.status,
            };
          });

          if (data.extended) {
            setAntiSnipeAlert("⚡ Soft-Close Triggered: Auction extended by 10s due to late bid!");
            setTimeout(() => setAntiSnipeAlert(null), 4000);
          }

          if (data.type === "BID_ACCEPTED") {
            setBidLogs((logs) => [
              {
                id: Math.random().toString(),
                user: data.highest_bidder,
                amount: data.highest_bid,
                time: new Date().toLocaleTimeString(),
                status: "CONFIRMED",
              },
              ...logs.slice(0, 19),
            ]);
          }
        } else if (data.type === "BID_CONFIRMED") {
          console.log("[WS] Bid acknowledged by cluster:", data);
        } else if (data.type === "BID_REJECTED") {
          alert(`Bid Rejected: ${data.reason}`);
        }
      } catch (err) {
        console.error("Failed to parse WS frame", err);
      }
    };

    ws.onclose = () => {
      setConnected(false);
      setReconnecting(true);
      reconnectTimeoutRef.current = setTimeout(connectWebSocket, 2000);
    };

    ws.onerror = (err) => {
      console.error("[WS] Socket error", err);
      ws.close();
    };
  };

  useEffect(() => {
    connectWebSocket();
    return () => {
      if (socketRef.current) socketRef.current.close();
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
    };
  }, []);

  // Countdown timer clock ticking every 100ms
  useEffect(() => {
    const timer = setInterval(() => {
      setAuction((prev) => {
        const nowSec = Date.now() / 1000;
        const remaining = Math.max(0, prev.end_time - nowSec);
        return { ...prev, time_remaining: remaining };
      });
    }, 100);
    return () => clearInterval(timer);
  }, []);

  const submitBid = (amount: number) => {
    if (!socketRef.current || socketRef.current.readyState !== WebSocket.OPEN) {
      alert("Not connected to gateway");
      return;
    }

    const bidId = "bid_" + Date.now() + "_" + Math.random().toString(36).substring(2, 6);
    socketRef.current.send(
      JSON.stringify({
        action: "bid",
        auction_id: auction.auction_id,
        user_id: myUserId,
        bid_id: bidId,
        amount: amount,
      })
    );
  };

  const nextMinBid = auction.highest_bid + auction.min_increment;
  const isWinning = auction.highest_bidder === myUserId;

  return (
    <div style={{ maxWidth: 860, margin: "40px auto", padding: "24px" }}>
      {/* Header bar */}
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", borderBottom: "1px solid #1f2937", paddingBottom: 16 }}>
        <div>
          <h1 style={{ margin: 0, fontSize: 24, fontWeight: 700 }}>⚡ Flash Auction Engine</h1>
          <p style={{ margin: "4px 0 0", color: "#9ca3af", fontSize: 14 }}>Connected as: <strong style={{ color: "#38bdf8" }}>{myUserId}</strong></p>
        </div>
        <div>
          <span style={{
            display: "inline-block",
            padding: "6px 12px",
            borderRadius: 999,
            fontSize: 12,
            fontWeight: 600,
            backgroundColor: connected ? "#065f46" : reconnecting ? "#854d0e" : "#991b1b",
            color: connected ? "#6ee7b7" : reconnecting ? "#fde047" : "#fca5a5",
          }}>
            {connected ? "● Live Gateway Connected" : reconnecting ? "◌ Reconnecting..." : "✕ Offline"}
          </span>
        </div>
      </div>

      {/* Anti-snipe Banner */}
      {antiSnipeAlert && (
        <div style={{ backgroundColor: "#78350f", border: "1px solid #d97706", color: "#fef3c7", padding: "12px 16px", borderRadius: 8, marginTop: 16, fontWeight: 600 }}>
          {antiSnipeAlert}
        </div>
      )}

      {/* Main Auction Showcase */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20, marginTop: 24 }}>
        {/* Left Card: Item & Countdown */}
        <div style={{ backgroundColor: "#111827", border: "1px solid #1f2937", borderRadius: 12, padding: 24 }}>
          <h2 style={{ margin: 0, fontSize: 18, color: "#e5e7eb" }}>{auction.item_name}</h2>
          <p style={{ color: "#6b7280", fontSize: 13, marginTop: 4 }}>ID: {auction.auction_id}</p>

          <div style={{ marginTop: 24 }}>
            <span style={{ fontSize: 13, textTransform: "uppercase", color: "#9ca3af", letterSpacing: 1 }}>Countdown</span>
            <div style={{
              fontSize: 44,
              fontWeight: 800,
              fontFamily: "monospace",
              color: auction.time_remaining <= 10 ? "#ef4444" : "#10b981",
              marginTop: 4
            }}>
              {auction.time_remaining.toFixed(1)}s
            </div>
            {auction.time_remaining <= 10 && auction.time_remaining > 0 && (
              <p style={{ color: "#ef4444", fontSize: 12, margin: "4px 0 0", fontWeight: 600 }}>FINAL 10 SECONDS RUSH!</p>
            )}
          </div>

          <div style={{ marginTop: 24 }}>
            <span style={{ fontSize: 13, textTransform: "uppercase", color: "#9ca3af" }}>Highest Bidder</span>
            <div style={{ fontSize: 16, fontWeight: 600, color: isWinning ? "#34d399" : "#e5e7eb", marginTop: 4 }}>
              {auction.highest_bidder} {isWinning && "(YOU HOLD THE WINNING BID!)"}
            </div>
          </div>
        </div>

        {/* Right Card: Price & Hot Bidding Controls */}
        <div style={{ backgroundColor: "#111827", border: "1px solid #1f2937", borderRadius: 12, padding: 24 }}>
          <span style={{ fontSize: 13, textTransform: "uppercase", color: "#9ca3af", letterSpacing: 1 }}>Current Highest Bid</span>
          <div style={{
            fontSize: 48,
            fontWeight: 800,
            color: flashing ? "#38bdf8" : "#f3f4f6",
            transition: "color 0.2s ease",
            marginTop: 4
          }}>
            ${auction.highest_bid.toFixed(2)}
          </div>

          <div style={{ marginTop: 20 }}>
            <span style={{ fontSize: 13, color: "#9ca3af" }}>Quick Bid (Min increment: +${auction.min_increment}):</span>
            <div style={{ display: "flex", gap: 10, marginTop: 8 }}>
              <button
                onClick={() => submitBid(nextMinBid)}
                disabled={!connected || auction.time_remaining <= 0}
                style={{ flex: 1, padding: "12px", borderRadius: 8, border: "none", backgroundColor: "#2563eb", color: "#fff", fontWeight: 700, cursor: "pointer" }}
              >
                +${auction.min_increment} (${nextMinBid.toFixed(0)})
              </button>
              <button
                onClick={() => submitBid(auction.highest_bid + 20)}
                disabled={!connected || auction.time_remaining <= 0}
                style={{ flex: 1, padding: "12px", borderRadius: 8, border: "none", backgroundColor: "#1d4ed8", color: "#fff", fontWeight: 700, cursor: "pointer" }}
              >
                +$20 (${(auction.highest_bid + 20).toFixed(0)})
              </button>
            </div>
          </div>

          <div style={{ marginTop: 16 }}>
            <div style={{ display: "flex", gap: 10 }}>
              <input
                type="number"
                placeholder={`Custom ($${nextMinBid}+)`}
                value={customBid}
                onChange={(e) => setCustomBid(e.target.value)}
                style={{ flex: 2, padding: "10px 14px", borderRadius: 8, border: "1px solid #374151", backgroundColor: "#1f2937", color: "#fff" }}
              />
              <button
                onClick={() => {
                  const val = parseFloat(customBid);
                  if (val) submitBid(val);
                }}
                disabled={!connected || auction.time_remaining <= 0}
                style={{ flex: 1, padding: "10px", borderRadius: 8, border: "none", backgroundColor: "#059669", color: "#fff", fontWeight: 700, cursor: "pointer" }}
              >
                Bid
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Real-Time Live Feed */}
      <div style={{ marginTop: 32, backgroundColor: "#111827", border: "1px solid #1f2937", borderRadius: 12, padding: 20 }}>
        <h3 style={{ margin: "0 0 16px", fontSize: 16 }}>Live Auction Feed (Redis Streams & Pub/Sub Fan-out)</h3>
        {bidLogs.length === 0 ? (
          <p style={{ color: "#6b7280", margin: 0 }}>No bids yet. Fire a bid above!</p>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
            {bidLogs.map((log) => (
              <div key={log.id} style={{ display: "flex", justifyContent: "space-between", padding: "8px 12px", backgroundColor: "#1f2937", borderRadius: 6, fontSize: 14 }}>
                <span><strong>{log.user}</strong> placed winning bid</span>
                <span style={{ color: "#34d399", fontWeight: 700 }}>${log.amount.toFixed(2)}</span>
                <span style={{ color: "#9ca3af", fontSize: 12 }}>{log.time}</span>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
