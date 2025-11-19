interface KPICardProps {
    title: string;
    value: string | number;
  }
  
  export default function KPICard({ title, value }: KPICardProps) {
    return (
      <div className="bg-white p-6 rounded-lg shadow-md">
        <p className="text-gray-500 text-sm">{title}</p>
        <h3 className="text-2xl font-bold text-gray-800 mt-2">{value}</h3>
      </div>
    );
  }
  