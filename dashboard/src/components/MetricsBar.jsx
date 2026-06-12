import React, { useState, useEffect } from 'react';

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

  // Live counts from the WebSocket stream
  const txnsSeen   = events.filter(e => e.topic === 'risk-scores').length;
  const liveAlerts = events.filter(e => e.topic === 'fraud-alerts').length;

  const tiles = [
    { value: txnsSeen,                                    label: 'Transactions Scored (live)', bg: 'bg-blue-600' },
    { value: metrics?.total_alerts ?? liveAlerts,         label: 'Fraud Alerts',               bg: 'bg-red-600' },
    { value: metrics?.critical_alerts ?? 0,               label: 'Critical (≥90%)',            bg: 'bg-orange-500' },
    { value: metrics ? `${(metrics.avg_risk_score * 100).toFixed(0)}%` : '—',
                                                          label: 'Avg Alert Risk',             bg: 'bg-yellow-500' },
  ];

  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-4 mb-6">
      {tiles.map(({ value, label, bg }, i) => (
        <div key={i} className={`${bg} text-white rounded-xl p-4 shadow`}>
          <div className="text-3xl font-bold tabular-nums">{value}</div>
          <div className="text-sm opacity-80 mt-1">{label}</div>
        </div>
      ))}
    </div>
  );
}
