"use client";

import { useEffect, useState } from "react";

export default function PriorityTable() {
  const [rows, setRows] = useState<any[]>([]);

  useEffect(() => {
    async function load() {
      const res = await fetch("/api/prioritization");
      const json = await res.json();
      setRows(json);
    }
    load();
  }, []);

  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
      <h2 className="text-lg font-semibold mb-4">
        High Priority Cases (ML Ranked)
      </h2>

      <div className="overflow-x-auto">
        <table className="min-w-full text-sm">
          <thead>
            <tr className="bg-gray-100 text-left">
              <th className="px-4 py-2">Case ID</th>
              <th className="px-4 py-2">Priority</th>
              <th className="px-4 py-2">Type</th>
              <th className="px-4 py-2">Neighborhood</th>
              <th className="px-4 py-2">Department</th>
              <th className="px-4 py-2">Status</th>
              <th className="px-4 py-2">Opened</th>
            </tr>
          </thead>

          <tbody>
            {rows.map((row, idx) => (
              <tr key={idx} className="border-b hover:bg-gray-50 cursor-pointer"
                onClick={() => window.location.href = `/dashboard/case/${row.case_id}`}
              >
                <td className="px-4 py-2">{row.case_id}</td>
                <td className="px-4 py-2 font-semibold text-red-600">
                  {row.priority_score.toFixed(1)}
                </td>
                <td className="px-4 py-2">{row.case_type}</td>
                <td className="px-4 py-2">{row.neighborhood}</td>
                <td className="px-4 py-2">{row.department}</td>
                <td className="px-4 py-2">{row.case_status}</td>
                <td className="px-4 py-2">{row.open_dt?.split("T")[0]}</td>
              </tr>
            ))}
          </tbody>

        </table>
      </div>
    </div>
  );
}
