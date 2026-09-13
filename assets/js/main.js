/* =========================================================
   SHARED SITE BEHAVIOR
   SECTION: Mobile navigation
   PURPOSE:
   Control the responsive navigation menu, maintain
   accessibility state, and reset the menu when needed.
   ========================================================= */

const menuToggle = document.querySelector(".mobile-menu-toggle");
const mainNavigation = document.querySelector(".main-nav");

if (menuToggle && mainNavigation) {

    // Update the visual and accessibility state of the mobile menu.
    const setMenuState = (isOpen) => {
        mainNavigation.classList.toggle("is-open", isOpen);
        menuToggle.classList.toggle("is-open", isOpen);
        menuToggle.setAttribute("aria-expanded", String(isOpen));
    };


    // Toggle the menu when the mobile navigation button is selected.
    menuToggle.addEventListener("click", () => {
        const isOpen =
            menuToggle.getAttribute("aria-expanded") === "true";

        setMenuState(!isOpen);
    });


    // Close the menu after a navigation link is selected.
    mainNavigation.querySelectorAll("a").forEach((link) => {
        link.addEventListener("click", () => {
            setMenuState(false);
        });
    });


    // Allow keyboard users to close the menu with Escape.
    document.addEventListener("keydown", (event) => {
        if (event.key === "Escape") {
            setMenuState(false);
        }
    });


    // Reset the mobile menu when returning to desktop layout.
    window.addEventListener("resize", () => {
        if (window.innerWidth > 850) {
            setMenuState(false);
        }
    });
}