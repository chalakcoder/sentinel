import React from 'react';

const REASON_LABELS = {
  VELOCITY_FRAUD:  { icon: '⚡', label: 'Velocity' },
  GEO_JUMP:        { icon: '📍', label: 'Geo-jump' },
  AMOUNT_ANOMALY:  { icon: '💸', label: 'Amount anomaly' },
  HIGH_FREQUENCY:  { icon: '🔁', label: 'High frequency' },
  CRITICAL_SCORE:  { icon: '🚨', label: 'Critical score' },
  ATM_GEO_JUMP:    { icon: '🏧', label: 'ATM geo-jump' },
};

export function FraudAlertLog({ events }) {
  const alerts = events
    .filter(e => e.topic === 'fraud-alerts')
    .slice(0, 20);

  return (
    <div className="bg-white rounded-xl shadow p-4 flex flex-col">
      <h2 className="text-base font-bold text-gray-800 mb-3">
        Fraud Alerts
        <span className="ml-2 text-xs font-normal text-gray-400">(risk ≥ 70%, via Flink SQL)</span>
      </h2>
      <div className="overflow-y-auto flex-1 space-y-2" style={{ maxHeight: 380 }}>
        {alerts.map((event, i) => {
          const a = event.value ?? {};
          const reasons = a.alert_reasons ?? [];
          const isCritical = (a.risk_score ?? 0) >= 0.9;
          return (
            <div
              key={i}
              className={`p-3 rounded-lg border ${
                isCritical ? 'bg-red-50 border-red-200' : 'bg-orange-50 border-orange-200'
              }`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className={`font-bold text-sm ${isCritical ? 'text-red-800' : 'text-orange-800'}`}>
                    ₹{Number(a.amount ?? 0).toLocaleString('en-IN')}
                  </span>
                  <span className="text-xs text-gray-500">
                    {a.merchant_category} · {a.city}
                  </span>
                </div>
                <span className="font-mono font-bold text-sm text-gray-800">
                  {((a.risk_score ?? 0) * 100).toFixed(0)}%
                </span>
              </div>
              <div className="flex flex-wrap gap-1.5 mt-1.5">
                {reasons.map((r, j) => {
                  const meta = REASON_LABELS[r] ?? { icon: '⚠️', label: r };
                  return (
                    <span key={j} className="text-xs bg-white border border-gray-200 rounded px-1.5 py-0.5 text-gray-600">
                      {meta.icon} {meta.label}
                    </span>
                  );
                })}
              </div>
              <div className="flex gap-3 mt-1 text-xs text-gray-400">
                <span>{a.user_id}</span>
                {a.geo_deviation_km > 100 && <span>{Math.round(a.geo_deviation_km)} km from home ({a.home_city})</span>}
                {a.txn_count_5min > 5 && <span>{a.txn_count_5min} txns / 5min</span>}
              </div>
            </div>
          );
        })}
        {alerts.length === 0 && (
          <p className="text-center text-gray-400 text-sm py-8">
            Waiting for fraud alerts…
          </p>
        )}
      </div>
    </div>
  );
}
