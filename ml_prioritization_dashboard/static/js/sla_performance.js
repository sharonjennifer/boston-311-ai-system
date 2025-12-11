// Simple JS for the SLA Performance page: apply dark mode, render charts, and drive the overdue map + filter.
document.addEventListener("DOMContentLoaded", () => {
  // Apply the same dark mode preference used on other pages
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (_) {
    // If localStorage is blocked (some browsers/privacy modes), we just skip it
  }

  // Read the pre-computed SLA metrics and overdue cases that the backend embedded as JSON
  let pageData = {
    overallSlaRate: 0,
    deptLabels: [],
    deptRates: [],
    artServiceLabels: [],
    artServiceHours: [],
    overdueMapCases: []
  };

  const dataScript = document.getElementById("sla-data");
  if (dataScript) {
    try {
      pageData = JSON.parse(dataScript.textContent);
    } catch (e) {
      console.error("Failed to parse SLA page data JSON:", e);
    }
  }

  const {
    overallSlaRate,
    deptLabels,
    deptRates,
    artServiceLabels,
    artServiceHours,
    overdueMapCases
  } = pageData;

  // Small Chart.js plugin so we can write text in the center of the gauge
  if (window.Chart) {
    Chart.register({
      id: "centerTextPlugin",
      afterDraw(chart, args, options) {
        if (!options || !options.text) return;

        const { ctx, chartArea } = chart;
        const text = options.text;

        ctx.save();
        ctx.font =
          '600 18px Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif';
        ctx.fillStyle = "#111827";
        ctx.textAlign = "center";
        ctx.textBaseline = "middle";

        const x = (chartArea.left + chartArea.right) / 2;
        const y = (chartArea.top + chartArea.bottom) / 2;
        ctx.fillText(text, x, y);
        ctx.restore();
      }
    });
  }

  // Render a half-doughnut gauge that shows the overall SLA compliance percentage
  (function renderSlaGauge() {
    const canvas = document.getElementById("slaGaugeChart");
    if (!canvas || !window.Chart) return;

    const gaugePct = typeof overallSlaRate === "number" ? overallSlaRate : 0;

    new Chart(canvas, {
      type: "doughnut",
      data: {
        labels: ["On time", "Late"],
        datasets: [
          {
            data: [gaugePct, Math.max(0, 100 - gaugePct)],
            backgroundColor: ["#16a34a", "#fee2e2"],
            borderWidth: 0
          }
        ]
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "65%",        // makes the doughnut thick so there is room for center text
        rotation: -90,        // start angle (to draw a half gauge)
        circumference: 180,   // only draw half the circle
        plugins: {
          legend: { display: false },
          tooltip: { enabled: true },
          centerTextPlugin: {
            text: gaugePct.toFixed(1) + "%"
          }
        }
      }
    });
  })();

  // Render a horizontal bar chart that compares SLA compliance by department
  (function renderSlaDeptBar() {
    const canvas = document.getElementById("slaDeptBarChart");
    if (!canvas || !window.Chart || !deptLabels || !deptLabels.length) return;

    new Chart(canvas, {
      type: "bar",
      data: {
        labels: deptLabels,
        datasets: [
          {
            label: "SLA compliance (%)",
            data: deptRates || []
          }
        ]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        scales: {
          x: {
            beginAtZero: true,
            max: 100,
            ticks: {
              // Show values like “87%” on the x-axis
              callback: (value) => value + "%"
            }
          }
        },
        plugins: {
          legend: { display: false }
        }
      }
    });
  })();

  // Render a vertical bar chart that shows average resolution time per service in hours
  (function renderArtServiceBar() {
    const canvas = document.getElementById("artServiceBarChart");
    if (!canvas || !window.Chart || !artServiceLabels || !artServiceLabels.length) {
      return;
    }

    new Chart(canvas, {
      type: "bar",
      data: {
        labels: artServiceLabels,
        datasets: [
          {
            label: "Avg resolution time (hrs)",
            data: artServiceHours || []
          }
        ]
      },
      options: {
        indexAxis: "x",
        responsive: true,
        scales: {
          y: { beginAtZero: true }
        },
        plugins: {
          legend: { display: false }
        }
      }
    });
  })();

  // Shared variables so the map and filter logic can talk to each other
  let mapInstance = null;
  let markerGroup = null;
  const markerIndex = []; // store { marker, neighborhood } so we can filter them later

  // Build a Leaflet map that plots overdue cases as small circle markers
  (function renderSlaMap() {
    const mapDiv = document.getElementById("slaMap");
    if (!mapDiv || !window.L) return;

    // If we don’t have any overdue cases with coordinates, show a friendly message instead
    if (!overdueMapCases || !overdueMapCases.length) {
      mapDiv.innerHTML =
        '<p class="empty-text">No overdue cases with map coordinates.</p>';
      return;
    }

    // Center the map roughly on Boston with scroll zoom disabled (so the page scroll feels normal)
    mapInstance = L.map("slaMap", { scrollWheelZoom: false }).setView(
      [42.32, -71.06],
      11
    );

    // Use OpenStreetMap tiles as the base layer
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 18,
      attribution: "&copy; OpenStreetMap contributors"
    }).addTo(mapInstance);

    // Group of markers so we can easily show/hide them when filtering
    markerGroup = L.layerGroup().addTo(mapInstance);

    const bounds = [];

    overdueMapCases.forEach((c) => {
      // Handle both numeric and string lat/lon
      const lat = typeof c.latitude === "number" ? c.latitude : parseFloat(c.latitude);
      const lon = typeof c.longitude === "number" ? c.longitude : parseFloat(c.longitude);
      if (!isFinite(lat) || !isFinite(lon)) return;

      const neighborhood = c.neighborhood || "Unknown";

      // Show how many days late the case is (fall back to days open if needed)
      const daysOver =
        c.days_overdue != null
          ? c.days_overdue
          : c.days_open != null
          ? c.days_open
          : "—";
      const caseId = c.case_enquiry_id || "";

      const marker = L.circleMarker([lat, lon], {
        radius: 5,
        stroke: false,
        fillOpacity: 0.8
      }).bindPopup(
        `<strong>Case ${caseId}</strong><br/>
         ${neighborhood}<br/>
         Dept: ${c.department || "—"}<br/>
         Reason: ${c.reason || "—"}<br/>
         Days overdue: ${daysOver}`
      );

      marker.addTo(markerGroup);
      markerIndex.push({ marker, neighborhood });

      bounds.push([lat, lon]);
    });

    // Zoom the map so all overdue markers are in view
    if (bounds.length) {
      const group = L.featureGroup(
        bounds.map((b) => L.marker(b)) // we only use these markers to compute bounds
      );
      mapInstance.fitBounds(group.getBounds().pad(0.2));
    }
  })();

  // Hook up the neighborhood dropdown so it filters both the overdue table and the map
  (function setupOverdueFilter() {
    const select = document.getElementById("overdueNeighborhoodFilter");
    const table = document.getElementById("overdueTable");
    if (!select || !table) return;

    const rows = table.querySelectorAll("tbody tr");
    if (!rows.length) return;

    function applyFilter() {
      const val = select.value || "ALL";

      // Filter table rows by checking the data-neighborhood attribute
      rows.forEach((tr) => {
        const n = tr.getAttribute("data-neighborhood") || "Unknown";
        tr.style.display = val === "ALL" || n === val ? "" : "none";
      });

      // Filter the map markers using the same neighborhood value
      if (markerGroup && markerIndex.length) {
        markerIndex.forEach(({ marker, neighborhood }) => {
          const show = val === "ALL" || neighborhood === val;
          if (show) {
            if (!markerGroup.hasLayer(marker)) {
              markerGroup.addLayer(marker);
            }
          } else if (markerGroup.hasLayer(marker)) {
            markerGroup.removeLayer(marker);
          }
        });
      }
    }

    // Re-run the filter whenever the dropdown changes
    select.addEventListener("change", applyFilter);
    // Run once on page load so everything starts in a consistent state
    applyFilter();
  })();
});
