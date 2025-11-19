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
} from "recharts";

export default function CaseTypeBarChart() {
  const [data, setData] = useState<any[]>([]);

  useEffect(() => {
    async function load() {
      const res = await fetch("/api/charts/types");
      const json = await res.json();
      setData(json);
    }
    load();
  }, []);

  return (
    <div className="bg-white p-6 rounded-lg shadow-md mt-10">
      <h2 className="text-lg font-semibold mb-4">Cases by Type</h2>

      <div style={{ width: "100%", height: 350 }}>
        <ResponsiveContainer>
          <BarChart data={data} layout="vertical">
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis type="number" />
            <YAxis dataKey="case_type" type="category" width={160} />
            <Tooltip />
            <Bar dataKey="total" fill="#1e40af" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
