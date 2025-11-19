import { NextResponse } from "next/server";
import bigquery from "../../../../lib/bigquery";

export async function GET() {
  try {
    const query = `
      SELECT 
        department,
        AVG(open_30d) AS avg_open_30d,
        AVG(closed_30d) AS avg_closed_30d,
        AVG(dept_pressure_30d) AS avg_pressure
      FROM \`boston311-mlops.boston311_service.tbl_dept_daily\`
      GROUP BY department
      ORDER BY avg_open_30d DESC;
    `;

    const [rows] = await bigquery.query(query);

    return NextResponse.json(rows || []);
  } catch (error) {
    console.error("Department chart API error:", error);
    return NextResponse.json([], { status: 200 });
  }
}
