// Run after the DOM is ready so we can safely touch the page elements
document.addEventListener("DOMContentLoaded", () => {
  // Apply the same dark mode preference that we use on the main dashboard
  try {
    if (localStorage.getItem("b311_dark") === "true") {
      document.body.classList.add("dark-mode");
    }
  } catch (e) {
    // If localStorage is blocked, we just skip applying the preference
  }

  // At the moment this page is static (server renders the table),
  // but this JS file is here so we can easily add interactivity later
  // (for example: sorting columns or adding row click behavior).
});
