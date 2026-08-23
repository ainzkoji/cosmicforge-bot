interface SignalSortSelectProps {
  value: string;
  onChange: (value: string) => void;
}

export function SignalSortSelect({ value, onChange }: SignalSortSelectProps) {
  return (
    <select
      className="rounded-xl border border-white/10 bg-white/[0.04] px-3 py-2 text-sm font-semibold text-white outline-none transition-colors hover:border-cyan-300/30"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    >
      <option value="newest">Newest</option>
      <option value="confidence">Highest confidence</option>
      <option value="time_left">Entry window ending soon</option>
      <option value="risk_reward">Best risk/reward</option>
    </select>
  );
}
