import { NextResponse } from "next/server";
import bigquery from "../../../../lib/bigquery";

export async function GET() {
  try {
    const query = `
      SELECT 
        neighborhood,
        COUNT(*) AS total
      FROM \`boston311-mlops.boston311.service_requests_2025\`
      WHERE neighborhood IS NOT NULL
      GROUP BY neighborhood
      ORDER BY total DESC
      LIMIT 15;
    `;

    const [rows] = await bigquery.query(query);

    return NextResponse.json(rows || []);
  } catch (error) {
    console.error("Neighborhood chart API error:", error);
    return NextResponse.json([], { status: 200 });
  }
}
