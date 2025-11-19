import { NextResponse } from "next/server";
import bigquery from "../../../../lib/bigquery";

export async function GET() {
  try {
    const query = `
      WITH opened AS (
        SELECT 
          DATE(open_dt) AS day,
          COUNT(*) AS opened_count
        FROM \`boston311-mlops.boston311.service_requests_2025\`
        WHERE open_dt IS NOT NULL
        GROUP BY day
      ),
      closed AS (
        SELECT 
          DATE(closed_dt) AS day,
          COUNT(*) AS closed_count
        FROM \`boston311-mlops.boston311.service_requests_2025\`
        WHERE closed_dt IS NOT NULL
        GROUP BY day
      )
      SELECT 
        o.day,
        opened_count,
        IFNULL(closed_count, 0) AS closed_count
      FROM opened o
      LEFT JOIN closed c ON o.day = c.day
      ORDER BY day ASC;
    `;

    const [rows] = await bigquery.query(query);

    return NextResponse.json(rows || []);
  } catch (error) {
    console.error("Daily trends API error:", error);
    return NextResponse.json([], { status: 200 });
  }
}
