import MapWrapper from "../../../components/MapWrapper";

export default function MapPage() {
  return (
    <div>
      <h1 className="text-2xl font-semibold mb-6">Boston 311 Map</h1>

      <p className="text-gray-600 mb-4">
        Interactive geospatial view of 311 service requests.
      </p>

      <MapWrapper />
    </div>
  );
}
