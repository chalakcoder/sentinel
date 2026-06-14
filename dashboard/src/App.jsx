import React from 'react';
import { MetricsBar }      from './components/MetricsBar';
import { TransactionFeed } from './components/TransactionFeed';
import { FraudAlertLog }   from './components/FraudAlertLog';
import { RiskScoreChart }  from './components/RiskScoreChart';
import { useWebSocket }    from './hooks/useWebSocket';

const API_URL = process.env.REACT_APP_API_URL || '';
const WS_URL  = (API_URL || window.location.origin).replace(/^http/, 'ws') + '/ws/events';

const STATUS_COLOR = {
  CONNECTED:    'bg-green-500 animate-pulse',
  CONNECTING:   'bg-yellow-400 animate-pulse',
  RECONNECTING: 'bg-yellow-400 animate-pulse',
  ERROR:        'bg-red-500',
};

const ARCH_STEPS = [
  'UPI Transactions',
  'Confluent Kafka + Schema Registry',
  'Flink SQL (windows + joins + scoring)',
  'fraud-alerts',
  'MongoDB Atlas Sink Connector',
  'This Dashboard',
];

export default function App() {
  const { events, connectionStatus } = useWebSocket(WS_URL);

  return (
    <div className="min-h-screen bg-gray-100 p-4 sm:p-6">
      <div className="max-w-7xl mx-auto">

        {/* ── Header ─────────────────────────────────────────── */}
        <div className="flex items-start justify-between mb-6">
          <div>
            <h1 className="text-3xl font-extrabold text-gray-900 tracking-tight">Sentinel</h1>
            <p className="text-gray-500 text-sm mt-0.5">
              Real-Time UPI Fraud Detection on Confluent
            </p>
          </div>
          <div className="flex items-center gap-2 mt-1">
            <span className={`w-2.5 h-2.5 rounded-full ${STATUS_COLOR[connectionStatus] ?? 'bg-gray-400'}`} />
            <span className="text-xs text-gray-500">{connectionStatus}</span>
          </div>
        </div>

        {/* ── KPI metrics ────────────────────────────────────── */}
        <MetricsBar events={events} />

        {/* ── Risk score stream ───────────────────────────────── */}
        <RiskScoreChart events={events} />

        {/* ── Transaction feed + Fraud alerts ─────────────────── */}
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 mb-6">
          <TransactionFeed events={events} />
          <FraudAlertLog   events={events} />
        </div>

        {/* ── Architecture diagram ────────────────────────────── */}
        <div className="bg-white rounded-xl shadow p-4">
          <h2 className="text-sm font-bold text-gray-700 mb-3">Architecture</h2>
          <div className="flex flex-wrap items-center gap-1.5 text-xs">
            {ARCH_STEPS.map((step, i) => (
              <React.Fragment key={i}>
                <span className="bg-blue-50 text-blue-700 px-2.5 py-1 rounded-md font-medium">
                  {step}
                </span>
                {i < ARCH_STEPS.length - 1 && (
                  <span className="text-gray-300 font-bold text-base">→</span>
                )}
              </React.Fragment>
            ))}
          </div>
          <p className="mt-3 text-xs text-gray-400">
            UPI / card transactions → Kafka (Avro + governed schemas) → Flink SQL feature
            engineering and risk scoring → fraud alerts sunk to MongoDB Atlas via Confluent
            managed connector → live dashboard. Fraud caught in &lt;2 seconds.
          </p>
        </div>

      </div>
    </div>
  );
}
