import { NextResponse } from "next/server";
import bigquery from "../../../lib/bigquery";

export async function GET() {
  try {
    const query = `
      SELECT
        case_id,
        priority_score,
        neighborhood,
        type AS case_type,
        case_status,
        open_dt,
        closed_dt,
        department,
        case_title,
        latitude,
        longitude
      FROM \`boston311-mlops.boston311.v_cases_priority_scored\`
      ORDER BY priority_score DESC
      LIMIT 100;
    `;

    const [rows] = await bigquery.query(query);
    return NextResponse.json(rows || []);
  } catch (error) {
    console.error("PRIORITY API ERROR:", error);
    return NextResponse.json([], { status: 200 });
  }
}
