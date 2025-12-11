// Run the demand trends logic after the page has fully loaded
document.addEventListener("DOMContentLoaded", () => {
  // Try to apply dark mode based on the same preference as the main dashboard
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (_) {
    // If localStorage is blocked, we just skip this and stay in light mode
  }

  // Prepare a default object so the charts do not crash if no data is injected
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

  // Read the inline JSON blob from the template (server passes arrays here)
  const dataScript = document.getElementById("demand-data");
  if (dataScript) {
    try {
      pageData = JSON.parse(dataScript.textContent);
    } catch (e) {
      console.error("Failed to parse demand trends data JSON:", e);
    }
  }

  // Destructure out the pieces we care about for each chart
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

  // If Chart.js failed to load, do not try to render anything
  if (typeof Chart === "undefined") {
    console.warn("Chart.js not loaded; skipping demand trends charts.");
    return;
  }

  // Draw weekly history line chart using last-12-months weekly case counts
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

  // Draw a simple baseline forecast chart (derived from recent monthly volumes)
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

  // Draw month-of-year seasonality chart for total volume across all topics
  (function renderMonthlySeasonality() {
    if (!monthLabels || !monthLabels.length) return;
    const canvas = document.getElementById("monthlySeasonChart");
    if (!canvas) return;

    // Use different colors if the month is missing historical data
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
            // Tooltip explains when a bar represents “no data yet”
            callbacks: {
              label: function (context) {
                const idx = context.dataIndex;
                const hasData = (monthHasData && monthHasData[idx]) ? true : false;
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

  // Draw horizontal bar chart with top topics (reasons) by volume
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

  // Draw multi-series line chart to show seasonal peaks by topic
  (function renderSeasonalPeaks() {
    if (!seasonalRaw || !seasonalRaw.length || !monthLabels || !monthLabels.length) {
      return;
    }
    const canvas = document.getElementById("seasonalTopicsFullChart");
    if (!canvas) return;

    // Palette of colors used to differentiate each topic line
    const palette = [
      "#3b82f6", // blue
      "#f97316", // orange
      "#22c55e", // green
      "#a855f7", // purple
      "#0ea5e9"  // cyan
    ];

    // Transform seasonalRaw into Chart.js datasets
    const datasetsSeason = seasonalRaw.map((ds, idx) => {
      const color = palette[idx % palette.length];
      return {
        label: ds.label,
        data: ds.data,
        borderColor: color,
        backgroundColor: color + "33", // ~20% opacity for the fill
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