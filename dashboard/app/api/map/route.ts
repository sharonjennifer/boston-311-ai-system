import { NextResponse } from "next/server";
import bigquery from "../../../lib/bigquery";

export async function GET() {
  try {
    const query = `
      SELECT
        _id AS id,
        type AS case_type,
        case_status,
        CAST(latitude AS FLOAT64) AS lat,
        CAST(longitude AS FLOAT64) AS lon
      FROM \`boston311-mlops.boston311.service_requests_2025\`
      WHERE latitude IS NOT NULL
        AND longitude IS NOT NULL
      LIMIT 3000;
    `;

    const [rows] = await bigquery.query(query);

    return NextResponse.json(Array.isArray(rows) ? rows : []);
  } catch (error) {
    console.error("MAP API ERROR:", error);
    return NextResponse.json([], { status: 200 });
  }
}
