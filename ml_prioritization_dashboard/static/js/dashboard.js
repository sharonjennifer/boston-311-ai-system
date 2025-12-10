document.addEventListener("DOMContentLoaded", () => {
  /* ---------- Dark mode ---------- */
  function applyDarkFromStorage() {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  }
  applyDarkFromStorage();

  const darkModeToggle = document.getElementById("darkModeToggle");
  if (darkModeToggle) {
    darkModeToggle.addEventListener("click", () => {
      document.body.classList.toggle("dark-mode");
      const enabled = document.body.classList.contains("dark-mode");
      localStorage.setItem("b311_dark", enabled ? "true" : "false");
    });
  }

  /* ---------- What’s Near Me ---------- */
  const nearMeBtn     = document.getElementById("nearMeBtn");
  const filterForm    = document.getElementById("filterForm");
  const nearLat       = document.getElementById("near_lat");
  const nearLon       = document.getElementById("near_lon");
  const nearRadiusM   = document.getElementById("near_radius_m");
  const nearMeMessage = document.getElementById("nearMeMessage");

  if (nearMeBtn && navigator.geolocation) {
    nearMeBtn.addEventListener("click", () => {
      nearMeBtn.disabled = true;
      nearMeBtn.textContent = "Locating…";
      if (nearMeMessage) nearMeMessage.textContent = "";

      navigator.geolocation.getCurrentPosition(
        pos => {
          const { latitude, longitude } = pos.coords;
          nearLat.value = latitude.toString();
          nearLon.value = longitude.toString();
          nearRadiusM.value = "402"; // ~0.25 miles
          filterForm.submit();
        },
        err => {
          if (nearMeMessage) {
            nearMeMessage.textContent =
              err.code === err.PERMISSION_DENIED
                ? "Location access blocked. Allow it in browser settings and try again."
                : "Could not get your location. Please try again.";
          }
          nearMeBtn.disabled = false;
          nearMeBtn.textContent = "What’s Near Me?";
        }
      );
    });
  } else if (nearMeMessage && !navigator.geolocation) {
    nearMeMessage.textContent = "Geolocation not supported in this browser.";
  }

  // Clear near-me if filters change
  ["neighborhood","reason","department"].forEach(id => {
    const el = document.getElementById(id);
    if (!el) return;
    el.addEventListener("change", () => {
      if (nearLat) nearLat.value = "";
      if (nearLon) nearLon.value = "";
      if (nearRadiusM) nearRadiusM.value = "";
    });
  });

  /* ---------- Cascading dropdowns using combos ---------- */
  const data    = window.B311_DATA || {};
  const combos  = data.combos || [];
  const mapCases = data.mapCases || [];

  const neighSelect = document.getElementById("neighborhood");
  const reasonSelect = document.getElementById("reason");
  const deptSelect   = document.getElementById("department");

  function computeOptions(selNeigh, selReason) {
    const reasonSet = new Set();
    const deptSet   = new Set();

    combos.forEach(row => {
      const n = row.neighborhood;
      const r = row.reason;
      const d = row.department;

      if (selNeigh === "ALL" || n === selNeigh) {
        reasonSet.add(r);
      }

      const neighOK  = (selNeigh === "ALL" || n === selNeigh);
      const reasonOK = (selReason === "ALL" || r === selReason);
      if (neighOK && reasonOK) {
        deptSet.add(d);
      }
    });

    return {
      reasons: Array.from(reasonSet).sort(),
      departments: Array.from(deptSet).sort()
    };
  }

  function repopulate(selectElem, values, selectedValue) {
    selectElem.innerHTML = "";
    const optAll = document.createElement("option");
    optAll.value = "ALL";
    optAll.textContent = "All";
    selectElem.appendChild(optAll);

    values.forEach(v => {
      const opt = document.createElement("option");
      opt.value = v;
      opt.textContent = v;
      selectElem.appendChild(opt);
    });

    if (selectedValue && selectedValue !== "ALL" && values.includes(selectedValue)) {
      selectElem.value = selectedValue;
    } else {
      selectElem.value = "ALL";
    }
  }

  function handleFilterChange() {
    const selNeigh  = neighSelect.value;
    const selReason = reasonSelect.value;
    const selDept   = deptSelect.value;

    const opts = computeOptions(selNeigh, selReason);
    const newReasonSelected = opts.reasons.includes(selReason) ? selReason : "ALL";
    const newDeptSelected   = opts.departments.includes(selDept) ? selDept : "ALL";

    repopulate(reasonSelect, opts.reasons, newReasonSelected);
    repopulate(deptSelect, opts.departments, newDeptSelected);

    // reset near-me when filters change
    if (nearLat && nearLon && nearRadiusM) {
      nearLat.value = "";
      nearLon.value = "";
      nearRadiusM.value = "";
    }
  }

  if (neighSelect && reasonSelect && deptSelect) {
    neighSelect.addEventListener("change", handleFilterChange);
    reasonSelect.addEventListener("change", handleFilterChange);
    handleFilterChange(); // run once on load to sync options
  }

  /* ---------- MAP ---------- */
  function initMap() {
    const mapEl = document.getElementById("map");
    if (!mapEl) return;

    if (!mapCases || mapCases.length === 0) {
      mapEl.innerHTML =
        '<p style="font-size:12px;color:#6b7280;padding:8px;">No map cases for current filters.</p>';
      return;
    }

    const map = L.map("map").setView([42.36, -71.06], 12);

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 19,
      attribution: "&copy; OpenStreetMap contributors"
    }).addTo(map);

    const bounds = [];
    mapCases.forEach(c => {
      if (c.latitude == null || c.longitude == null) return;

      const lat = parseFloat(c.latitude);
      const lon = parseFloat(c.longitude);
      if (Number.isNaN(lat) || Number.isNaN(lon)) return;

      let baseRadius = 5;
      if (typeof c.priority_score === "number") {
        baseRadius = 4 + Math.min(6, c.priority_score * 8);
      }

      const marker = L.circleMarker([lat, lon], {
        radius: baseRadius,
        color: "#1f2937",
        weight: 1,
        fillColor: "#ff4c4c",
        fillOpacity: 0.85
      }).addTo(map);

      const scoreText =
        typeof c.priority_score === "number"
          ? c.priority_score.toFixed(3)
          : (c.priority_score ?? "");

      marker.bindPopup(
        `<b>Case ${c.case_enquiry_id}</b><br/>
         ${c.neighborhood || ""}<br/>
         ${c.reason || ""}<br/>
         Dept: ${c.department || ""}<br/>
         Score: ${scoreText}`
      );

      bounds.push([lat, lon]);
    });

    if (bounds.length > 0) {
      map.fitBounds(bounds, { padding: [20, 20] });
    }
  }

  // Leaflet is loaded with defer; wait a tiny bit to be safe
  if (window.L) {
    initMap();
  } else {
    const interval = setInterval(() => {
      if (window.L) {
        clearInterval(interval);
        initMap();
      }
    }, 50);
  }

  /* ---------- Guided tour ---------- */
  const tourOverlay  = document.getElementById("tourOverlay");
  const tourTooltip  = document.getElementById("tourTooltip");

  const filtersCard  = document.getElementById("filtersCard");
  const kpiRow       = document.getElementById("kpiRow");
  const mapPanel     = document.getElementById("mapPanel");
  const caseListMain = document.getElementById("caseListMain");
  const tourRestart  = document.getElementById("tourRestartBtn");

  let tourIndex = 0;

  const tourSteps = [
    {
      title: "Filters",
      text: "Start here. Choose neighborhood, reason, department, or a specific case ID.",
      target: () => filtersCard
    },
    {
      title: "Live metrics",
      text: "These KPIs update every time you apply filters, giving quick signal of load and risk.",
      target: () => kpiRow
    },
    {
      title: "What’s Near Me?",
      text: "Use this button in the field to see high-priority cases within a short walk or drive.",
      target: () => nearMeBtn
    },
    {
      title: "City map",
      text: "Each circle is an open case. Look for clusters of red markers to spot hot spots.",
      target: () => mapPanel
    },
    {
      title: "Priority-ordered queue",
      text: "These are the top 10 cases for your current slice, with priority score and context.",
      target: () => caseListMain
    },
    {
      title: "Replay this tour",
      text: "Need a refresher later? Click here anytime to replay this guide.",
      target: () => tourRestart
    }
  ];

  function clearHighlight() {
    document.querySelectorAll(".tour-highlight")
      .forEach(el => el.classList.remove("tour-highlight"));
  }

  function positionTooltip(targetRect) {
    const tooltipRect = tourTooltip.getBoundingClientRect();
    let top  = targetRect.bottom + 10;
    let left = targetRect.left;

    if (left + tooltipRect.width > window.innerWidth - 12) {
      left = window.innerWidth - tooltipRect.width - 12;
    }
    if (top + tooltipRect.height > window.innerHeight - 12) {
      top = targetRect.top - tooltipRect.height - 10;
    }
    if (top < 8) top = 8;
    if (left < 8) left = 8;

    tourTooltip.style.top  = top + "px";
    tourTooltip.style.left = left + "px";
  }

  function showTourStep(index) {
    clearHighlight();
    if (index < 0 || index >= tourSteps.length) {
      endTour();
      return;
    }
    tourIndex = index;
    const step = tourSteps[index];
    const targetEl = step.target && step.target();
    if (!targetEl) {
      showTourStep(index + 1);
      return;
    }

    tourOverlay.style.display = "block";
    tourTooltip.style.display = "block";
    targetEl.classList.add("tour-highlight");

    tourTooltip.innerHTML = `
      <div class="tour-tooltip-title">${step.title}</div>
      <div class="tour-tooltip-body">${step.text}</div>
      <div class="tour-tooltip-actions">
        ${index > 0 ? '<button class="tour-btn tour-btn-secondary" id="tourPrevBtn">Back</button>' : ""}
        <button class="tour-btn tour-btn-secondary" id="tourSkipBtn">Skip</button>
        <button class="tour-btn tour-btn-primary" id="tourNextBtn">
          ${index === tourSteps.length - 1 ? "Done" : "Next"}
        </button>
      </div>
    `;

    const rect = targetEl.getBoundingClientRect();
    positionTooltip(rect);

    const nextBtn = document.getElementById("tourNextBtn");
    const prevBtn = document.getElementById("tourPrevBtn");
    const skipBtn = document.getElementById("tourSkipBtn");

    if (nextBtn) nextBtn.onclick = () => showTourStep(tourIndex + 1);
    if (prevBtn) prevBtn.onclick = () => showTourStep(tourIndex - 1);
    if (skipBtn) skipBtn.onclick = () => endTour();
  }

  function startTour() {
    showTourStep(0);
  }

  function endTour() {
    clearHighlight();
    tourOverlay.style.display = "none";
    tourTooltip.style.display = "none";
  }

  if (tourRestart) {
    tourRestart.addEventListener("click", () => {
      startTour();
    });
  }

  // Optional: auto-run tour only on first visit
  try {
    const seen = localStorage.getItem("b311_seen_tour");
    if (!seen) {
      localStorage.setItem("b311_seen_tour", "true");
      startTour();
    }
  } catch (e) {
    // localStorage might be blocked; ignore
  }
});
