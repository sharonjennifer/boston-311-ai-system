"use client";

import { useEffect, useState } from "react";
import {
  LineChart,
  Line,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend
} from "recharts";

export default function DailyTrendChart() {
  const [data, setData] = useState<any[]>([]);

  useEffect(() => {
    async function load() {
      const res = await fetch("/api/charts/daily");
      const json = await res.json();
      setData(json);
    }
    load();
  }, []);

  return (
    <div className="bg-white p-6 rounded-lg shadow-md mt-10">
      <h2 className="text-lg font-semibold mb-4">Daily Trends (Open vs Closed)</h2>

      <div style={{ width: "100%", height: 350 }}>
        <ResponsiveContainer>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="day" tick={{ fontSize: 10 }} />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line 
              type="monotone" 
              dataKey="opened_count" 
              stroke="#1d4ed8" 
              name="Opened"
              strokeWidth={2}
            />
            <Line 
              type="monotone" 
              dataKey="closed_count" 
              stroke="#ef4444" 
              name="Closed"
              strokeWidth={2}
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
