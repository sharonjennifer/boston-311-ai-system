import { NextResponse } from "next/server";
import bigquery from "../../../../lib/bigquery";

export async function GET() {
  try {
    const query = `
      SELECT 
        type AS case_type,
        COUNT(*) AS total
      FROM \`boston311-mlops.boston311.service_requests_2025\`
      WHERE type IS NOT NULL
      GROUP BY case_type
      ORDER BY total DESC
      LIMIT 15;
    `;

    const [rows] = await bigquery.query(query);

    return NextResponse.json(rows || []);
  } catch (error) {
    console.error("Case type chart API error:", error);
    return NextResponse.json([], { status: 200 });
  }
}
