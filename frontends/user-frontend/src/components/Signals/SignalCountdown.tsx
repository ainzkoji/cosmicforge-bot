import { useEffect, useMemo, useState } from "react";
import { Timer } from "lucide-react";

interface SignalCountdownProps {
  expiresAt?: string | null;
  status?: string;
  compact?: boolean;
}

function secondsUntil(expiresAt?: string | null): number {
  if (!expiresAt) return 0;
  const expires = new Date(expiresAt).getTime();
  if (Number.isNaN(expires)) return 0;
  return Math.max(0, Math.floor((expires - Date.now()) / 1000));
}

function formatDuration(totalSeconds: number): string {
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function terminalCopy(status?: string): { title: string; body: string } {
  if (status === "INVALIDATED") {
    return { title: "Signal invalidated", body: "Signal invalidated — do not enter." };
  }
  if (status === "SL_HIT") {
    return { title: "Stop loss hit", body: "Stop loss hit — signal closed." };
  }
  return { title: "Entry window expired", body: "Entry window expired — do not enter." };
}

export function SignalCountdown({ expiresAt, status, compact = false }: SignalCountdownProps) {
  const [timeLeft, setTimeLeft] = useState(() => secondsUntil(expiresAt));
  const terminal = useMemo(
    () => ["EXPIRED", "SL_HIT", "CANCELLED", "INVALIDATED", "TP3_HIT"].includes(status || ""),
    [status]
  );

  useEffect(() => {
    setTimeLeft(secondsUntil(expiresAt));
    if (!expiresAt || terminal) return;

    const timer = window.setInterval(() => {
      setTimeLeft(secondsUntil(expiresAt));
    }, 1000);

    return () => window.clearInterval(timer);
  }, [expiresAt, terminal]);

  if (terminal || timeLeft <= 0) {
    const copy = terminalCopy(status);
    return (
      <div className="text-sm text-slate-400">
        <div className="font-semibold text-slate-300">{copy.title}</div>
        {!compact && <div className="mt-1 text-xs text-amber-200">{copy.body}</div>}
      </div>
    );
  }

  return (
    <div className="inline-flex items-center gap-2 rounded-full border border-cyan-400/20 bg-cyan-400/10 px-3 py-1 text-sm text-cyan-100">
      <Timer className="h-4 w-4" />
      <span>Entry valid for {formatDuration(timeLeft)}</span>
    </div>
  );
}
