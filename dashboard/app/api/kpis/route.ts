import { NextResponse } from "next/server";
import bigquery from "../../../lib/bigquery";

export async function GET() {
  try {
    // 1. Total Open Cases
    const [openCases] = await bigquery.query(`
      SELECT COUNT(*) AS total_open
      FROM \`boston311-mlops.boston311.service_requests_2025\`
      WHERE case_status != 'Closed' OR closed_dt IS NULL;
    `);

    // 2. SLA Compliance
    // on_time = 'ontime' means SLA met
    const [slaData] = await bigquery.query(`
      SELECT SAFE_DIVIDE(
        SUM(CASE WHEN on_time = 'ontime' THEN 1 ELSE 0 END),
        SUM(CASE WHEN closed_dt IS NOT NULL THEN 1 ELSE 0 END)
      ) AS sla_rate
      FROM \`boston311-mlops.boston311.service_requests_2025\`;
    `);

    // 3. Average Resolution Time
    const [resolutionData] = await bigquery.query(`
      SELECT AVG(
        TIMESTAMP_DIFF(
          CAST(closed_dt AS TIMESTAMP),
          CAST(open_dt AS TIMESTAMP),
          DAY
        )
      ) AS avg_days
      FROM \`boston311-mlops.boston311.service_requests_2025\`
      WHERE closed_dt IS NOT NULL;
    `);

    return NextResponse.json({
      total_open: openCases[0]?.total_open ?? 0,
      sla_rate: slaData[0]?.sla_rate ?? 0,
      avg_resolution_days: resolutionData[0]?.avg_days ?? 0,
    });

  } catch (err: any) {
    console.error("KPI ERROR:", err);
    return NextResponse.json(
      { error: "Failed to load KPIs", details: String(err) },
      { status: 500 }
    );
  }
}
