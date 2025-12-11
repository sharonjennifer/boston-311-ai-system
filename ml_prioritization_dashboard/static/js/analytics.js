// Wait until the HTML is loaded before running any chart logic
document.addEventListener("DOMContentLoaded", () => {
  // Try to apply the user's dark mode preference from localStorage
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (_) {
    // If localStorage is blocked (e.g., in strict browser mode), we simply skip it
  }

  // Default empty structure in case the inline JSON script is missing or invalid
  let data = {
    neighLabels: [],
    neighCounts: [],
    deptLabels: [],
    deptCounts: [],
    reasonLabels: [],
    reasonCounts: [],
    slaDeptLabels: [],
    slaDeptRates: [],
    srcLabels: [],
    srcCounts: [],
    srcArtLabels: [],
    srcArtHours: []
  };

  // Read the inline JSON block that the Flask template rendered into the page
  const dataScript = document.getElementById("analytics-data");
  if (dataScript) {
    try {
      data = JSON.parse(dataScript.textContent);
    } catch (e) {
      // If JSON parsing fails, log it and keep going with the empty defaults
      console.error("Failed to parse analytics data JSON:", e);
    }
  }

  // Destructure the values we care about, so it's easier to pass them into charts
  const {
    neighLabels,
    neighCounts,
    deptLabels,
    deptCounts,
    reasonLabels,
    reasonCounts,
    slaDeptLabels,
    slaDeptRates,
    srcLabels,
    srcCounts,
    srcArtLabels,
    srcArtHours
  } = data;

  // If Chart.js didn't load for some reason, we don't try to build charts
  if (typeof Chart === "undefined") {
    console.warn("Chart.js not loaded; skipping analytics charts.");
    return;
  }

  // 1. Neighborhood cases (horizontal bar)
  (function () {
    // Only draw the chart if we actually have neighborhood data
    if (!neighLabels || !neighLabels.length) return;
    const canvas = document.getElementById("neighCasesChart");
    if (!canvas) return;

    // Horizontal bar chart showing the top neighborhoods by case volume
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: neighLabels,
        datasets: [
          {
            label: "Cases",
            data: neighCounts || [],
            backgroundColor: "rgba(37,99,235,0.2)",
            borderColor: "#2563eb",
            borderWidth: 1
          }
        ]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          },
          y: { grid: { display: false } }
        }
      }
    });
  })();

  // 2. Department cases (vertical bar)
  (function () {
    // Department chart only appears when there are labels
    if (!deptLabels || !deptLabels.length) return;
    const canvas = document.getElementById("deptCasesChart");
    if (!canvas) return;

    // Vertical bar chart showing which departments receive the most requests
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: deptLabels,
        datasets: [
          {
            label: "Cases",
            data: deptCounts || [],
            backgroundColor: "rgba(56,189,248,0.25)",
            borderColor: "#0ea5e9",
            borderWidth: 1
          }
        ]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { display: false } },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          }
        }
      }
    });
  })();

  // 3. Top request reasons (horizontal bar)
  (function () {
    // Only render if we have reason/topic information
    if (!reasonLabels || !reasonLabels.length) return;
    const canvas = document.getElementById("reasonCasesChart");
    if (!canvas) return;

    // Horizontal bar chart for the most common request reasons citywide
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: reasonLabels,
        datasets: [
          {
            label: "Cases",
            data: reasonCounts || [],
            backgroundColor: "rgba(249,115,22,0.25)",
            borderColor: "#f97316",
            borderWidth: 1
          }
        ]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          },
          y: { grid: { display: false } }
        }
      }
    });
  })();

  // 4. SLA compliance by department (horizontal bar, percentage)
  (function () {
    // Skip this chart when there is no SLA department data
    if (!slaDeptLabels || !slaDeptLabels.length) return;
    const canvas = document.getElementById("slaDeptChart");
    if (!canvas) return;

    // Horizontal bar chart where each bar shows SLA % for a department
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: slaDeptLabels,
        datasets: [
          {
            label: "SLA compliance (%)",
            data: slaDeptRates || [],
            backgroundColor: "rgba(22,163,74,0.25)",
            borderColor: "#16a34a",
            borderWidth: 1
          }
        ]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: {
            beginAtZero: true,
            max: 100,
            ticks: {
              // Show the x-axis ticks as percentages
              callback: (value) => value + "%"
            },
            grid: { color: "rgba(148,163,184,0.3)" }
          },
          y: { grid: { display: false } }
        }
      }
    });
  })();

  // 5. Volume by report source (doughnut chart)
  (function () {
    // Only render if we know the channels/sources
    if (!srcLabels || !srcLabels.length) return;
    const canvas = document.getElementById("sourceVolumeChart");
    if (!canvas) return;

    // Doughnut chart to show the share of volume by channel (Call, app, Web, etc.)
    new Chart(canvas, {
      type: "doughnut",
      data: {
        labels: srcLabels,
        datasets: [
          {
            data: srcCounts || [],
            // Using multiple colors so each channel is visually distinct
            backgroundColor: [
              "#3b82f6",
              "#22c55e",
              "#f97316",
              "#a855f7",
              "#0ea5e9",
              "#eab308",
              "#ec4899",
              "#10b981",
              "#facc15",
              "#6366f1"
            ]
          }
        ]
      },
      options: {
        responsive: true,
        plugins: {
          legend: { position: "right" }
        }
      }
    });
  })();

  // 6. Average resolution time by source (bar chart)
  (function () {
    // Only draw if we have average resolution time data per source
    if (!srcArtLabels || !srcArtLabels.length) return;
    const canvas = document.getElementById("sourceArtChart");
    if (!canvas) return;

    // Bar chart showing how long (in hours) each channel takes on average to close a case
    new Chart(canvas, {
      type: "bar",
      data: {
        labels: srcArtLabels,
        datasets: [
          {
            label: "Avg resolution time (hrs)",
            data: srcArtHours || [],
            backgroundColor: "rgba(139,92,246,0.25)",
            borderColor: "#8b5cf6",
            borderWidth: 1
          }
        ]
      },
      options: {
        responsive: true,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { display: false } },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          }
        }
      }
    });
  })();
});
