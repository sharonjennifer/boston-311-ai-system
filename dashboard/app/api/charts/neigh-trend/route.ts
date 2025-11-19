import { NextResponse } from "next/server";
import bigquery from "../../../../lib/bigquery";

export async function GET() {
  try {
    const query = `
      SELECT 
        neighborhood,
        d AS day,
        open_14d
      FROM \`boston311-mlops.boston311_service.tbl_neigh_open_14d\`
      WHERE neighborhood IS NOT NULL
      ORDER BY day ASC;
    `;

    const [rows] = await bigquery.query(query);

    return NextResponse.json(rows || []);
  } catch (error) {
    console.error("Neighborhood trend API error:", error);
    return NextResponse.json([], { status: 200 });
  }
}
