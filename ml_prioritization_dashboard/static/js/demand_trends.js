document.addEventListener("DOMContentLoaded", () => {
  // ---------- Dark mode ----------
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (_) {
    // ignore if localStorage is blocked
  }

  // ---------- Load data from inline JSON ----------
  let pageData = {
    histWeekLabels: [],
    histWeekValues: [],
    fcWeekLabels: [],
    fcWeekValues: [],
    monthLabels: [],
    monthTotals: [],
    monthHasData: [],
    seasonalRaw: [],
    topicLabels: [],
    topicTotals: []
  };

  const dataScript = document.getElementById("demand-data");
  if (dataScript) {
    try {
      pageData = JSON.parse(dataScript.textContent);
    } catch (e) {
      console.error("Failed to parse demand trends data JSON:", e);
    }
  }

  const {
    histWeekLabels,
    histWeekValues,
    fcWeekLabels,
    fcWeekValues,
    monthLabels,
    monthTotals,
    monthHasData,
    seasonalRaw,
    topicLabels,
    topicTotals
  } = pageData;

  // Helper to check Chart.js presence
  if (typeof Chart === "undefined") {
    console.warn("Chart.js not loaded; skipping demand trends charts.");
    return;
  }

  // ---------- Weekly history chart ----------
  (function renderWeeklyHistory() {
    if (!histWeekLabels || !histWeekLabels.length) return;
    const canvas = document.getElementById("weeklyHistoryChart");
    if (!canvas) return;

    new Chart(canvas, {
      type: "line",
      data: {
        labels: histWeekLabels,
        datasets: [
          {
            label: "Weekly cases",
            data: histWeekValues || [],
            tension: 0.25,
            borderColor: "#2563eb",
            backgroundColor: "rgba(37,99,235,0.08)",
            fill: true,
            pointRadius: 2
          }
        ]
      },
      options: {
        responsive: true,
        scales: {
          x: {
            ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 8 },
            grid: { display: false }
          },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          }
        },
        plugins: {
          legend: { display: false }
        }
      }
    });
  })();

  // ---------- Forecast chart (baseline) ----------
  (function renderForecast() {
    if (!fcWeekLabels || !fcWeekLabels.length) return;
    const canvas = document.getElementById("forecastChart");
    if (!canvas) return;

    new Chart(canvas, {
      type: "line",
      data: {
        labels: fcWeekLabels,
        datasets: [
          {
            label: "Baseline forecast",
            data: fcWeekValues || [],
            borderColor: "#fb7185",
            borderDash: [4, 4],
            pointRadius: 0,
            fill: false
          }
        ]
      },
      options: {
        responsive: true,
        scales: {
          x: {
            ticks: { maxRotation: 0, autoSkip: true, maxTicksLimit: 6 },
            grid: { display: false }
          },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          }
        },
        plugins: {
          legend: { display: false }
        }
      }
    });
  })();

  // ---------- Monthly seasonality – total volume ----------
  (function renderMonthlySeasonality() {
    if (!monthLabels || !monthLabels.length) return;
    const canvas = document.getElementById("monthlySeasonChart");
    if (!canvas) return;

    const baseColor = "rgba(37,99,235,0.3)";
    const baseBorder = "#2563eb";
    const emptyColor = "rgba(148,163,184,0.3)";
    const emptyBorder = "#9ca3af";

    const bgColors = (monthHasData || []).map((ok) => (ok ? baseColor : emptyColor));
    const borderColors = (monthHasData || []).map((ok) =>
      ok ? baseBorder : emptyBorder
    );

    new Chart(canvas, {
      type: "bar",
      data: {
        labels: monthLabels,
        datasets: [
          {
            label: "Cases",
            data: monthTotals || [],
            backgroundColor: bgColors,
            borderColor: borderColors,
            borderWidth: 1
          }
        ]
      },
      options: {
        responsive: true,
        scales: {
          x: { grid: { display: false } },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          }
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: function (context) {
                const idx = context.dataIndex;
                const hasData =
                  (monthHasData && monthHasData[idx]) ? true : false;
                if (!hasData) {
                  return "Data to be loaded";
                }
                const v = context.parsed.y || 0;
                return "Cases: " + v.toLocaleString();
              }
            }
          }
        }
      }
    });
  })();

  // ---------- Top topics (horizontal bar) ----------
  (function renderTopTopics() {
    if (!topicLabels || !topicLabels.length) return;
    const canvas = document.getElementById("topTopicsChart");
    if (!canvas) return;

    new Chart(canvas, {
      type: "bar",
      data: {
        labels: topicLabels,
        datasets: [
          {
            label: "Cases",
            data: topicTotals || [],
            backgroundColor: "rgba(37,99,235,0.2)",
            borderColor: "#2563eb",
            borderWidth: 1
          }
        ]
      },
      options: {
        indexAxis: "y",
        responsive: true,
        scales: {
          x: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          },
          y: {
            grid: { display: false }
          }
        },
        plugins: {
          legend: { display: false }
        }
      }
    });
  })();

  // ---------- Seasonal peaks by topic (multi-series line) ----------
  (function renderSeasonalPeaks() {
    if (!seasonalRaw || !seasonalRaw.length || !monthLabels || !monthLabels.length) {
      return;
    }
    const canvas = document.getElementById("seasonalTopicsFullChart");
    if (!canvas) return;

    const palette = [
      "#3b82f6", // blue
      "#f97316", // orange
      "#22c55e", // green
      "#a855f7", // purple
      "#0ea5e9"  // cyan
    ];

    const datasetsSeason = seasonalRaw.map((ds, idx) => {
      const color = palette[idx % palette.length];
      return {
        label: ds.label,
        data: ds.data,
        borderColor: color,
        backgroundColor: color + "33", // ~20% opacity
        tension: 0.3,
        fill: true,
        pointRadius: 0
      };
    });

    new Chart(canvas, {
      type: "line",
      data: {
        labels: monthLabels,
        datasets: datasetsSeason
      },
      options: {
        responsive: true,
        scales: {
          x: { grid: { display: false } },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(148,163,184,0.3)" }
          }
        },
        plugins: {
          legend: {
            position: "top",
            labels: {
              boxWidth: 12,
              boxHeight: 12
            }
          }
        }
      }
    });
  })();
});
