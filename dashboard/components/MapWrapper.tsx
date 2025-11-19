"use client";

import { useEffect, useRef } from "react";
import mapboxgl from "mapbox-gl";

mapboxgl.accessToken = process.env.NEXT_PUBLIC_MAPBOX_TOKEN || "";

interface SinglePoint {
  lat: number;
  lon: number;
  id?: string;
  case_type?: string;
  case_status?: string;
}

export default function MapWrapper({ singlePoint }: { singlePoint?: SinglePoint }) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<mapboxgl.Map | null>(null);

  useEffect(() => {
    if (map.current || !mapContainer.current) return;

    // Default center (Boston)
    let center: [number, number] = [-71.0589, 42.3601];
    let zoom = 11;

    // If single case is provided → center map on it
    if (singlePoint) {
      center = [Number(singlePoint.lon), Number(singlePoint.lat)];
      zoom = 14;
    }

    // Initialize map
    map.current = new mapboxgl.Map({
      container: mapContainer.current,
      style: "mapbox://styles/mapbox/light-v11",
      center,
      zoom,
    });

    map.current.on("load", async () => {
      let data: any[] = [];

      /** -----------------------------------
       *  SINGLE CASE MODE
       * ----------------------------------- */
      if (singlePoint) {
        data = [
          {
            id: singlePoint.id || "Case",
            case_type: singlePoint.case_type || "Selected Case",
            case_status: singlePoint.case_status || "Unknown",
            lat: singlePoint.lat,
            lon: singlePoint.lon,
          }
        ];
      }

      /** -----------------------------------
       *  DASHBOARD MODE (load multiple points)
       * ----------------------------------- */
      else {
        try {
          const res = await fetch("/api/map");
          const json = await res.json();

          if (Array.isArray(json)) {
            data = json;
          } else {
            console.error("Invalid map data:", json);
            data = [];
          }
        } catch (err) {
          console.error("Failed to load map data:", err);
          data = [];
        }
      }

      /** -----------------------------------
       *  BUILD GEOJSON
       * ----------------------------------- */
      const geojson = {
        type: "FeatureCollection",
        features: data.map((d: any) => ({
          type: "Feature",
          properties: {
            id: d.id,
            category: d.case_type,
            status: d.case_status,
          },
          geometry: {
            type: "Point",
            coordinates: [Number(d.lon), Number(d.lat)],
          },
        })),
      };

      // Add source
      map.current!.addSource("cases", {
        type: "geojson",
        data: geojson,
      });

      /** MULTI-POINT MODE LAYERS */
      if (!singlePoint) {
        map.current!.addLayer({
          id: "heatmap",
          type: "heatmap",
          source: "cases",
          paint: {
            "heatmap-intensity": 1,
            "heatmap-radius": 20,
            "heatmap-opacity": 0.6,
          },
        });

        map.current!.addLayer({
          id: "points",
          type: "circle",
          source: "cases",
          paint: {
            "circle-radius": 4,
            "circle-color": "#1d4ed8",
            "circle-opacity": 0.9,
          },
        });
      }

      /** SINGLE POINT MODE — A single, large marker */
      else {
        map.current!.addLayer({
          id: "single-point",
          type: "circle",
          source: "cases",
          paint: {
            "circle-radius": 10,
            "circle-color": "#ef4444", // red
            "circle-stroke-color": "#ffffff",
            "circle-stroke-width": 2,
          },
        });
      }
    });
  }, [singlePoint]);

  return (
    <div className="w-full h-[80vh] rounded-lg overflow-hidden shadow-md">
      <div ref={mapContainer} className="w-full h-full" />
    </div>
  );
}
