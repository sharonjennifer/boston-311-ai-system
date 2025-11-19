"use client";

import { useEffect, useState } from "react";
import KPICard from "../../../components/KPICard";
import NeighborhoodBarChart from "../../../components/NeighborhoodBarChart";
import CaseTypeBarChart from "../../../components/CaseTypeBarChart";
import DailyTrendChart from "../../../components/DailyTrendChart";
import DepartmentWorkloadChart from "../../../components/DepartmentWorkloadChart";
import NeighborhoodTrendChart from "../../../components/NeighborhoodTrendChart";

export default function OverviewDashboard() {
  const [kpis, setKpis] = useState<any>(null);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/kpis");
        const data = await res.json();
        setKpis(data);
      } catch (error) {
        console.error("Failed to load KPIs:", error);
      }
    }

    load();
  }, []);

  if (!kpis) {
    return <p className="text-gray-600">Loading KPIs...</p>;
  }

  const totalOpen = kpis.total_open ?? 0;
  const slaRate = kpis.sla_rate ?? 0;
  const avgResolution = kpis.avg_resolution_days ?? 0;

  return (
    <div className="space-y-10">

      {/* KPI GRID */}
      <div>
        <h1 className="text-2xl font-semibold mb-6">Overview</h1>
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
          <KPICard title="Total Open Cases" value={totalOpen.toLocaleString()} />
          <KPICard title="SLA Compliance" value={(slaRate * 100).toFixed(1) + "%"} />
          <KPICard title="Avg Resolution Time (days)" value={avgResolution.toFixed(1)} />
        </div>
      </div>

      {/* ROW 1 — Neighborhood + Case Type */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <NeighborhoodBarChart />
        <CaseTypeBarChart />
      </div>

      {/* ROW 2 — Daily trend (full width) */}
      <div>
        <DailyTrendChart />
      </div>

      {/* ROW 3 — Dept workload + 14-day neighborhood trend */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <DepartmentWorkloadChart />
        <NeighborhoodTrendChart />
      </div>

    </div>
  );
}
