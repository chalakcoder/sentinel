import React from 'react';

const RISK_STYLE = {
  LOW:      { row: 'bg-green-50',  badge: 'bg-green-100 text-green-800' },
  MEDIUM:   { row: 'bg-yellow-50', badge: 'bg-yellow-100 text-yellow-800' },
  HIGH:     { row: 'bg-orange-50', badge: 'bg-orange-100 text-orange-800' },
  CRITICAL: { row: 'bg-red-50',    badge: 'bg-red-100 text-red-800' },
};

export function TransactionFeed({ events }) {
  const items = events
    .filter(e => e.topic === 'risk-scores')
    .slice(0, 60);

  return (
    <div className="bg-white rounded-xl shadow p-4 flex flex-col">
      <h2 className="text-base font-bold text-gray-800 mb-3">
        Live Risk Feed
        <span className="ml-2 text-xs font-normal text-gray-400">({items.length} shown)</span>
      </h2>
      <div className="overflow-y-auto flex-1 space-y-1" style={{ maxHeight: 380 }}>
        {items.map((event, i) => {
          const v     = event.value ?? {};
          const label = v.risk_label ?? 'LOW';
          const s     = RISK_STYLE[label] ?? RISK_STYLE.LOW;
          const score = ((v.risk_score ?? 0) * 100).toFixed(0);
          return (
            <div key={i} className={`flex items-center justify-between px-3 py-1.5 rounded-lg text-sm ${s.row}`}>
              <span className="font-mono text-xs text-gray-400 w-20 truncate">
                {(v.transaction_id ?? '').slice(0, 8)}
              </span>
              <span className="text-gray-700 truncate flex-1 mx-2">
                {v.merchant_category ?? '—'} · {v.city ?? '—'}
              </span>
              <span className={`px-2 py-0.5 rounded text-xs font-semibold ${s.badge}`}>
                {label}
              </span>
              <span className="ml-2 font-mono font-bold text-gray-800 w-10 text-right">
                {score}%
              </span>
            </div>
          );
        })}
        {items.length === 0 && (
          <p className="text-center text-gray-400 text-sm py-8">
            Waiting for transactions…
          </p>
        )}
      </div>
    </div>
  );
}
