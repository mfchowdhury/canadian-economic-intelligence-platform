document.addEventListener("DOMContentLoaded", () => {
    const tabs = document.querySelectorAll("[data-economic-theme]");
    const title = document.getElementById("economic-theme-title");
    const description = document.getElementById("economic-theme-description");
    const indicators = document.getElementById("economic-theme-indicators");
    const purpose = document.getElementById("economic-theme-grain");
    const mart = document.getElementById("economic-theme-use");
    const output = document.getElementById("economic-theme-output");

    // Stop safely if the interactive Analytics explorer is not on the page.
    if (
        !tabs.length ||
        !title ||
        !description ||
        !indicators ||
        !purpose ||
        !mart ||
        !output
    ) {
        return;
    }

    // Analytical outputs align with the five Gold marts
    // and the five Power BI dashboard pages.
    const outputs = {
        overview: {
            title: "Integrated Economic Conditions",
            description:
                "Brings together retail sales, inflation, unemployment, GDP growth, USD/CAD, and the policy rate to monitor changes in Canadian economic conditions.",
            indicators:
                "Retail Sales · CPI · Labour · GDP · USD/CAD · Policy Rate",
            purpose:
                "Integrated economic conditions",
            mart:
                "Economic Overview",
            output:
                "Power BI Overview"
        },

        regional: {
            title: "Regional Comparison",
            description:
                "Compares retail growth, inflation, unemployment, and employment conditions across provinces and examines regional trends against national conditions.",
            indicators:
                "Retail Sales · CPI · Employment · Unemployment",
            purpose:
                "Cross-province and national comparison",
            mart:
                "Regional Analysis",
            output:
                "Power BI Regional"
        },

        industry: {
            title: "Industry Performance",
            description:
                "Analyzes GDP growth, retail performance, and labour productivity across industries and over time.",
            indicators:
                "GDP by Industry · Retail Sales · Labour Productivity",
            purpose:
                "Industry performance and trend analysis",
            mart:
                "Industry Analysis",
            output:
                "Power BI Industry"
        },

        fiscal: {
            title: "Fiscal & Affordability",
            description:
                "Connects inflation, housing, and fiscal indicators to examine affordability conditions alongside federal and Ontario fiscal trends.",
            indicators:
                "CPI · Housing Starts · Federal Fiscal · Ontario Fiscal",
            purpose:
                "Affordability and fiscal-condition analysis",
            mart:
                "Fiscal & Affordability",
            output:
                "Power BI Fiscal & Affordability"
        },

        forecasting: {
            title: "Retail Sales Forecasting",
            description:
                "Evaluates alternative forecasting approaches to estimate Canadian retail sales for the next reporting month and assess forecast performance.",
            indicators:
                "Retail Sales · Lagged and engineered forecasting features",
            purpose:
                "One-month retail sales forecasting",
            mart:
                "Forecasting Features",
            output:
                "Power BI Forecasting"
        }
    };

    // Update the explorer when a visitor selects an analytical output.
    tabs.forEach((tab) => {
        tab.addEventListener("click", () => {
            const selected = outputs[tab.dataset.economicTheme];

            if (!selected) {
                return;
            }

            // Reset all tabs before activating the selected one.
            tabs.forEach((item) => {
                item.classList.remove("active");
                item.setAttribute("aria-pressed", "false");
            });

            tab.classList.add("active");
            tab.setAttribute("aria-pressed", "true");

            // Update the analytical-output detail panel.
            title.textContent = selected.title;
            description.textContent = selected.description;
            indicators.textContent = selected.indicators;
            purpose.textContent = selected.purpose;
            mart.textContent = selected.mart;
            output.textContent = selected.output;
        });
    });
});