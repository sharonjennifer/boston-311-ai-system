// Small JS file for the Work Queues page:
// - Re-apply dark mode from localStorage
// - Attach simple prototype behavior to queue buttons
document.addEventListener("DOMContentLoaded", () => {
  // Reuse dark mode setting from the Command Center and other pages
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (e) {
    // If localStorage is blocked (e.g., incognito with strict privacy), just ignore
  }

  // Attach a basic click handler to all prototype action buttons
  // In the real app, these would open assignment/in-progress/note side panels.
  const actionButtons = document.querySelectorAll(".queue-btn-action");
  actionButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      alert(
        "Prototype only. In a full version, this button would open an assignment / in-progress / notes panel."
      );
    });
  });
});