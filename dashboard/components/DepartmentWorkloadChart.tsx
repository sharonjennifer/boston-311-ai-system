"use client";

import { useEffect, useState } from "react";
import {
  BarChart,
  Bar,
  CartesianGrid,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  Legend
} from "recharts";

export default function DepartmentWorkloadChart() {
  const [data, setData] = useState<any[]>([]);

  useEffect(() => {
    async function load() {
      const res = await fetch("/api/charts/departments");
      const json = await res.json();
      setData(json);
    }
    load();
  }, []);

  return (
    <div className="bg-white p-6 rounded-lg shadow-md mt-10">
      <h2 className="text-lg font-semibold mb-4">Department Workload (Open vs Closed)</h2>

      <div style={{ width: "100%", height: 400 }}>
        <ResponsiveContainer>
          <BarChart data={data} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis dataKey="department" type="category" width={120} />
            <Tooltip />
            <Legend />
            <Bar dataKey="avg_open_30d" fill="#1d4ed8" name="Open (30-day avg)" />
            <Bar dataKey="avg_closed_30d" fill="#10b981" name="Closed (30-day avg)" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
