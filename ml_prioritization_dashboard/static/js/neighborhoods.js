document.addEventListener("DOMContentLoaded", () => {
  // Reuse dark mode preference from other pages
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (e) {
    // ignore if localStorage is blocked
  }

  // (Optional) you can add more UX behavior here later,
  // e.g., row hover effects, simple sorting, etc.
});
