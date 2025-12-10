document.addEventListener("DOMContentLoaded", () => {
  // Dark mode preference
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (_) {
    // ignore if localStorage blocked
  }

  // Load data from inline JSON
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

  const dataScript = document.getElementById("analytics-data");
  if (dataScript) {
    try {
      data = JSON.parse(dataScript.textContent);
    } catch (e) {
      console.error("Failed to parse analytics data JSON:", e);
    }
  }

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

  if (typeof Chart === "undefined") {
    console.warn("Chart.js not loaded; skipping analytics charts.");
    return;
  }

  // 1. Neighborhood cases (horizontal bar)
  (function () {
    if (!neighLabels || !neighLabels.length) return;
    const canvas = document.getElementById("neighCasesChart");
    if (!canvas) return;

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
    if (!deptLabels || !deptLabels.length) return;
    const canvas = document.getElementById("deptCasesChart");
    if (!canvas) return;

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

  // 3. Reasons (horizontal bar)
  (function () {
    if (!reasonLabels || !reasonLabels.length) return;
    const canvas = document.getElementById("reasonCasesChart");
    if (!canvas) return;

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

  // 4. SLA by department (horizontal bar, %)
  (function () {
    if (!slaDeptLabels || !slaDeptLabels.length) return;
    const canvas = document.getElementById("slaDeptChart");
    if (!canvas) return;

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
              callback: (value) => value + "%"
            },
            grid: { color: "rgba(148,163,184,0.3)" }
          },
          y: { grid: { display: false } }
        }
      }
    });
  })();

  // 5. Volume by source (doughnut)
  (function () {
    if (!srcLabels || !srcLabels.length) return;
    const canvas = document.getElementById("sourceVolumeChart");
    if (!canvas) return;

    new Chart(canvas, {
      type: "doughnut",
      data: {
        labels: srcLabels,
        datasets: [
          {
            data: srcCounts || [],
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

  // 6. Avg resolution time by source (bar)
  (function () {
    if (!srcArtLabels || !srcArtLabels.length) return;
    const canvas = document.getElementById("sourceArtChart");
    if (!canvas) return;

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
