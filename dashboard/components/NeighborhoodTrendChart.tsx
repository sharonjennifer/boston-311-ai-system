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

export default function NeighborhoodTrendChart() {
  const [data, setData] = useState<any[]>([]);
  const [topNeighborhoods, setTopNeighborhoods] = useState<string[]>([]);

  useEffect(() => {
    async function load() {
      const res = await fetch("/api/charts/neigh-trend");
      const json = await res.json();

      // Determine top 5 neighborhoods by total open cases
      const totals: any = {};
      json.forEach((row: any) => {
        totals[row.neighborhood] = (totals[row.neighborhood] || 0) + row.open_14d;
      });

      const ranked = Object.entries(totals)
        .sort((a: any, b: any) => b[1] - a[1])
        .slice(0, 5)
        .map((x: any) => x[0]);

      setTopNeighborhoods(ranked);
      setData(json);
    }

    load();
  }, []);

  return (
    <div className="bg-white p-6 rounded-lg shadow-md mt-10">
      <h2 className="text-lg font-semibold mb-4">14-Day Open Cases Trend (Top Neighborhoods)</h2>

      <div style={{ width: "100%", height: 350 }}>
        <ResponsiveContainer>
          <LineChart>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis 
              dataKey="day" 
              tick={{ fontSize: 10 }} 
            />
            <YAxis />
            <Tooltip />
            <Legend />

            {topNeighborhoods.map((neigh, idx) => {
              const colorList = ["#1d4ed8", "#10b981", "#ef4444", "#f59e0b", "#6366f1"];
              return (
                <Line
                  key={neigh}
                  dataKey="open_14d"
                  name={neigh}
                  stroke={colorList[idx % colorList.length]}
                  strokeWidth={2}
                  dot={false}
                  data={data.filter((d) => d.neighborhood === neigh)}
                />
              );
            })}
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
