import { NextResponse } from "next/server";
import bigquery from "../../../../lib/bigquery";

export async function GET(req: Request, { params }: any) {
  const { case_id } = params;

  try {
    const query = `
      SELECT
        case_id,
        case_title,
        type AS case_type,
        neighborhood,
        department,
        case_status,
        open_dt,
        closed_dt,
        on_time,
        priority_score,
        latitude,
        longitude
      FROM \`boston311-mlops.boston311.v_cases_priority_scored\`
      WHERE case_id = @case_id
      LIMIT 1;
    `;

    const options = {
      query,
      params: { case_id },
    };

    const [rows] = await bigquery.query(options);

    if (!rows || rows.length === 0) {
      return NextResponse.json({ error: "Case not found" }, { status: 404 });
    }

    return NextResponse.json(rows[0]);
  } catch (error) {
    console.error("CASE API ERROR:", error);
    return NextResponse.json({ error: "Failed to load case" }, { status: 500 });
  }
}

