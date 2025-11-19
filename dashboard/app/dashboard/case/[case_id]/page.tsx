"use client";

import { useEffect, useState } from "react";
import MapWrapper from "../../../../components/MapWrapper";

export default function CaseDetailPage({ params }: any) {
  const { case_id } = params;
  const [data, setData] = useState<any>(null);

  useEffect(() => {
    async function load() {
      const res = await fetch(`/api/case/${case_id}`);
      const json = await res.json();
      setData(json);
    }
    load();
  }, [case_id]);

  if (!data) return <p className="text-gray-600">Loading case details...</p>;

  return (
    <div className="space-y-8">
      <h1 className="text-2xl font-semibold">Case #{case_id}</h1>

      {/* BASIC INFO */}
      <div className="bg-white p-6 rounded-lg shadow-md">
        <h2 className="text-xl font-semibold mb-4">Case Information</h2>
        <p><strong>Title:</strong> {data.case_title}</p>
        <p><strong>Type:</strong> {data.case_type}</p>
        <p><strong>Neighborhood:</strong> {data.neighborhood}</p>
        <p><strong>Status:</strong> {data.case_status}</p>
        <p><strong>Department:</strong> {data.department}</p>
        <p><strong>Priority Score:</strong> {data.priority_score?.toFixed(2)}</p>
      </div>

      {/* TIMELINE */}
      <div className="bg-white p-6 rounded-lg shadow-md">
        <h2 className="text-xl font-semibold mb-4">Timeline</h2>
        <p><strong>Opened:</strong> {data.open_dt?.split("T")[0]}</p>
        <p><strong>Closed:</strong> {data.closed_dt?.split("T")[0] ?? "Not closed"}</p>
        <p><strong>SLA:</strong> {data.on_time === "ontime" ? "On Time" : "Overdue"}</p>
      </div>

      {/* MAP SECTION */}
      <div className="bg-white p-6 rounded-lg shadow-md">
        <h2 className="text-xl font-semibold mb-4">Location</h2>
        <div className="h-[400px] w-full rounded-lg overflow-hidden">
          <MapWrapper
            singlePoint={{
              lat: data.latitude,
              lon: data.longitude,
              id: data.case_id
            }}
          />
        </div>
      </div>
    </div>
  );
}
