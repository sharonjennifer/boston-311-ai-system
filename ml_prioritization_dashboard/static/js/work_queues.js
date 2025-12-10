document.addEventListener("DOMContentLoaded", () => {
  // Reuse dark mode setting from Command Center
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (e) {
    // ignore if localStorage blocked
  }

  // Prototype actions for the three queue buttons
  const actionButtons = document.querySelectorAll(".queue-btn-action");
  actionButtons.forEach(btn => {
    btn.addEventListener("click", () => {
      alert(
        "Prototype only. In a full version, this button would open an assignment / in-progress / notes panel."
      );
    });
  });
});
