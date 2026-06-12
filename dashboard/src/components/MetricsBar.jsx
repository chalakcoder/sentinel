import React, { useState, useEffect } from 'react';

const TILES = [
  { key: 'total_alerts',   label: 'Fraud Alerts',           bg: 'bg-red-600' },
  { key: 'blocked',        label: 'Transactions Blocked',   bg: 'bg-orange-500' },
  { key: 'flagged',        label: 'Flagged for Review',     bg: 'bg-yellow-500' },
  { key: 'avg_latency_ms', label: 'Avg Agent Latency (ms)', bg: 'bg-blue-600' },
];

export function MetricsBar({ events }) {
  const [metrics, setMetrics] = useState(null);
  const apiUrl = process.env.REACT_APP_API_URL || '';

  useEffect(() => {
    const fetch_ = () =>
      fetch(`${apiUrl}/api/metrics`)
        .then(r => r.json())
        .then(setMetrics)
        .catch(() => {});

    fetch_();
    const id = setInterval(fetch_, 5000);
    return () => clearInterval(id);
  }, [apiUrl]);

  // Supplement server metrics with live WebSocket counts while server data loads
  const liveAlerts = events.filter(e => e.topic === 'fraud-alerts').length;
  const data = metrics ?? { total_alerts: liveAlerts, blocked: 0, flagged: 0, avg_latency_ms: 0 };

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 mb-6">
      {TILES.map(({ key, label, bg }) => (
        <div key={key} className={`${bg} text-white rounded-xl p-4 shadow`}>
          <div className="text-3xl font-bold tabular-nums">{data[key] ?? 0}</div>
          <div className="text-sm opacity-80 mt-1">{label}</div>
        </div>
      ))}
    </div>
  );
}
