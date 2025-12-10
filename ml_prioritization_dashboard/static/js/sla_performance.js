document.addEventListener("DOMContentLoaded", () => {
  // ---------------- Dark mode ----------------
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (_) {
    // ignore if localStorage blocked
  }

  // ---------------- Data from inline JSON ----------------
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

  // ---------------- Chart.js plugin: center text for gauge ----------------
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

  // ---------------- SLA Gauge ----------------
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
        cutout: "65%",
        rotation: -90,
        circumference: 180,
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

  // ---------------- SLA by Department (horizontal bar) ----------------
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

  // ---------------- Average Resolution Time by Service ----------------
  (function renderArtServiceBar() {
    const canvas = document.getElementById("artServiceBarChart");
    if (!canvas || !window.Chart || !artServiceLabels || !artServiceLabels.length)
      return;

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

  // -------------------------------------------------------------------
  // Shared state for overdue map + dropdown filter
  // -------------------------------------------------------------------
  let mapInstance = null;
  let markerGroup = null;
  const markerIndex = []; // { marker, neighborhood }

  // ---------------- Overdue Map (Leaflet) ----------------
  (function renderSlaMap() {
    const mapDiv = document.getElementById("slaMap");
    if (!mapDiv || !window.L) return;

    if (!overdueMapCases || !overdueMapCases.length) {
      mapDiv.innerHTML =
        '<p class="empty-text">No overdue cases with map coordinates.</p>';
      return;
    }

    mapInstance = L.map("slaMap", { scrollWheelZoom: false }).setView(
      [42.32, -71.06],
      11
    );

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 18,
      attribution: "&copy; OpenStreetMap contributors"
    }).addTo(mapInstance);

    markerGroup = L.layerGroup().addTo(mapInstance);

    const bounds = [];

    overdueMapCases.forEach((c) => {
      const lat = typeof c.latitude === "number" ? c.latitude : parseFloat(c.latitude);
      const lon = typeof c.longitude === "number" ? c.longitude : parseFloat(c.longitude);
      if (!isFinite(lat) || !isFinite(lon)) return;

      const neighborhood = c.neighborhood || "Unknown";
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

    if (bounds.length) {
      const group = L.featureGroup(
        bounds.map((b) => L.marker(b)) // only for bounds calc
      );
      mapInstance.fitBounds(group.getBounds().pad(0.2));
    }
  })();

  // ---------------- Overdue table + map neighborhood filter ----------------
  (function setupOverdueFilter() {
    const select = document.getElementById("overdueNeighborhoodFilter");
    const table = document.getElementById("overdueTable");
    if (!select || !table) return;

    const rows = table.querySelectorAll("tbody tr");
    if (!rows.length) return;

    function applyFilter() {
      const val = select.value || "ALL";

      // Filter table rows
      rows.forEach((tr) => {
        const n = tr.getAttribute("data-neighborhood") || "Unknown";
        tr.style.display = val === "ALL" || n === val ? "" : "none";
      });

      // Filter map markers
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

    select.addEventListener("change", applyFilter);
    // Initial run
    applyFilter();
  })();
});
