"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const menuItems = [
  { name: "Overview", path: "/dashboard/overview" },
  { name: "Map", path: "/dashboard/map" },
  { name: "Prioritization", path: "/dashboard/prioritization" },
  { name: "Clusters", path: "/dashboard/clusters" },
  { name: "Departments", path: "/dashboard/departments" }
];

export default function Sidebar() {
  const pathname = usePathname();

  return (
    <aside className="w-64 bg-blue-900 text-white min-h-screen px-4 py-6 shadow-md">
      <h1 className="text-xl font-bold mb-8">Boston 311</h1>
      <nav className="space-y-2">
        {menuItems.map((item) => (
          <Link key={item.path} href={item.path}>
            <div className={`px-4 py-2 rounded cursor-pointer
              ${pathname.startsWith(item.path)
                ? "bg-blue-700 text-white"
                : "hover:bg-blue-800"
              }`}>
              {item.name}
            </div>
          </Link>
        ))}
      </nav>
    </aside>
  );
}
