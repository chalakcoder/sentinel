import React, { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid,
  Tooltip, ReferenceLine, ResponsiveContainer,
} from 'recharts';

const FRAUD_THRESHOLD = 70;

function dotColor(score) {
  if (score >= FRAUD_THRESHOLD) return '#ef4444';
  if (score >= 40) return '#f59e0b';
  return '#22c55e';
}

const CustomDot = ({ cx, cy, payload }) => {
  const color = dotColor(payload.score);
  return <circle cx={cx} cy={cy} r={payload.score >= FRAUD_THRESHOLD ? 5 : 3} fill={color} />;
};

export function RiskScoreChart({ events }) {
  const chartData = useMemo(() => {
    return events
      .filter(e => e.topic === 'risk-scores')
      .slice(0, 80)
      .reverse()
      .map((e, i) => ({
        i,
        score: parseFloat(((e.value?.risk_score ?? 0) * 100).toFixed(1)),
        label: e.value?.risk_label ?? 'LOW',
        city:  e.value?.city ?? '',
      }));
  }, [events]);

  return (
    <div className="bg-white rounded-xl shadow p-4 mb-6">
      <h2 className="text-base font-bold text-gray-800 mb-3">
        Risk Score Stream
        <span className="ml-2 text-xs font-normal text-gray-400">
          last {chartData.length} transactions — red = fraud threshold
        </span>
      </h2>
      <ResponsiveContainer width="100%" height={200}>
        <LineChart data={chartData} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#f0f0f0" />
          <XAxis dataKey="i" tick={false} />
          <YAxis domain={[0, 100]} tickFormatter={v => `${v}%`} width={42} />
          <Tooltip
            formatter={(v, _, props) => [`${v}% (${props.payload.label})`, 'Risk']}
            labelFormatter={() => ''}
          />
          <ReferenceLine
            y={FRAUD_THRESHOLD}
            stroke="#ef4444"
            strokeDasharray="6 3"
            label={{ value: 'Alert threshold', fill: '#ef4444', fontSize: 11, position: 'insideTopRight' }}
          />
          <Line
            type="monotone"
            dataKey="score"
            stroke="#3b82f6"
            strokeWidth={1.5}
            dot={<CustomDot />}
            isAnimationActive={false}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}
