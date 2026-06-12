import React from 'react';

const ACTION_STYLE = {
  BLOCK:    { icon: '🚫', bg: 'bg-red-50',    border: 'border-red-200',    text: 'text-red-800' },
  FLAG:     { icon: '🚩', bg: 'bg-yellow-50', border: 'border-yellow-200', text: 'text-yellow-800' },
  APPROVE:  { icon: '✅', bg: 'bg-green-50',  border: 'border-green-200',  text: 'text-green-800' },
  ESCALATE: { icon: '⬆️', bg: 'bg-blue-50',   border: 'border-blue-200',   text: 'text-blue-800' },
};

function formatMs(ms) {
  if (!ms) return '—';
  return ms < 1000 ? `${ms}ms` : `${(ms / 1000).toFixed(1)}s`;
}

export function AgentActionLog({ events }) {
  const decisions = events
    .filter(e => e.topic === 'agent-decisions')
    .slice(0, 20);

  return (
    <div className="bg-white rounded-xl shadow p-4 flex flex-col">
      <h2 className="text-base font-bold text-gray-800 mb-3">
        Agent Decisions
        <span className="ml-2 text-xs font-normal text-gray-400">(IBM WatsonX)</span>
      </h2>
      <div className="overflow-y-auto flex-1 space-y-2" style={{ maxHeight: 380 }}>
        {decisions.map((event, i) => {
          const d = event.value ?? {};
          const s = ACTION_STYLE[d.action] ?? ACTION_STYLE.FLAG;
          return (
            <div key={i} className={`p-3 rounded-lg border ${s.bg} ${s.border}`}>
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span>{s.icon}</span>
                  <span className={`font-bold text-sm ${s.text}`}>{d.action}</span>
                  <span className="font-mono text-xs text-gray-400">
                    {(d.transaction_id ?? '').slice(0, 8)}
                  </span>
                </div>
                <span className="text-xs text-gray-400">{formatMs(d.latency_ms)}</span>
              </div>
              {d.reasoning && (
                <p className="text-xs text-gray-600 mt-1 line-clamp-2">{d.reasoning}</p>
              )}
              <div className="flex gap-3 mt-1 text-xs text-gray-400">
                <span>Conf: {((d.confidence ?? 0) * 100).toFixed(0)}%</span>
                <span>{(d.llm_model ?? '').split('/').pop()}</span>
              </div>
            </div>
          );
        })}
        {decisions.length === 0 && (
          <p className="text-center text-gray-400 text-sm py-8">
            Waiting for agent decisions…
          </p>
        )}
      </div>
    </div>
  );
}
