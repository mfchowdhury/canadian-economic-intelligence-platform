/* =========================================================
   PAGE: Analytics
   SECTION: Interactive economic explorer
   ========================================================= */

const economicThemeTabs = document.querySelectorAll(
    ".economic-explorer-tab"
);

const economicThemeTitle = document.getElementById(
    "economic-theme-title"
);

const economicThemeDescription = document.getElementById(
    "economic-theme-description"
);

const economicThemeIndicators = document.getElementById(
    "economic-theme-indicators"
);

const economicThemeGrain = document.getElementById(
    "economic-theme-grain"
);

const economicThemeUse = document.getElementById(
    "economic-theme-use"
);

const economicThemeOutput = document.getElementById(
    "economic-theme-output"
);


/* =========================================================
   PAGE: Analytics
   SECTION: Economic theme content
   ========================================================= */

const economicThemeDetails = {
    growth: {
        title: "Growth & Consumption",

        description:
            "GDP by industry and retail sales provide complementary views of economic activity, production, and household consumption.",

        indicators:
            "GDP by Industry · Retail Sales",

        grain:
            "Monthly",

        use:
            "Growth and consumption monitoring",

        output:
            "Economic Overview · Industry Analysis"
    },

    prices: {
        title: "Prices & Monetary Policy",

        description:
            "Consumer prices and the Bank of Canada policy rate connect inflation conditions with the monetary policy environment.",

        indicators:
            "Consumer Price Index · BoC Policy Rate",

        grain:
            "Monthly alignment",

        use:
            "Inflation and monetary policy analysis",

        output:
            "Economic Overview · Affordability Analysis"
    },

    labour: {
        title: "Labour Market",

        description:
            "Employment and unemployment indicators provide a national and provincial view of labour-market conditions across Canada.",

        indicators:
            "Employment · Unemployment · Provincial Labour Data",

        grain:
            "Monthly",

        use:
            "Labour-market and regional comparison",

        output:
            "Economic Overview · Regional Analysis"
    },

    housing: {
        title: "Housing & Population",

        description:
            "Housing starts and population estimates provide complementary measures of housing supply and demographic demand.",

        indicators:
            "Housing Starts · Population Estimates",

        grain:
            "Monthly and quarterly alignment",

        use:
            "Housing supply and demographic analysis",

        output:
            "Regional Analysis · Affordability Analysis"
    },

    external: {
        title: "External Conditions & Productivity",

        description:
            "USD/CAD exchange rates provide a view of external currency conditions, while labour productivity measures changes in productive performance.",

        indicators:
            "USD/CAD Exchange Rate · Labour Productivity",

        grain:
            "Monthly and annual alignment",

        use:
            "Currency and productivity analysis",

        output:
            "Economic Overview · Industry Analysis"
    }
};


/* =========================================================
   PAGE: Analytics
   SECTION: Update selected economic theme
   ========================================================= */

function updateEconomicTheme(themeButton) {
    const themeKey = themeButton.dataset.economicTheme;
    const themeDetails = economicThemeDetails[themeKey];

    if (!themeDetails) {
        return;
    }

    economicThemeTabs.forEach((tab) => {
        tab.classList.remove("active");
        tab.setAttribute("aria-pressed", "false");
    });

    themeButton.classList.add("active");
    themeButton.setAttribute("aria-pressed", "true");

    economicThemeTitle.textContent =
        themeDetails.title;

    economicThemeDescription.textContent =
        themeDetails.description;

    economicThemeIndicators.textContent =
        themeDetails.indicators;

    economicThemeGrain.textContent =
        themeDetails.grain;

    economicThemeUse.textContent =
        themeDetails.use;

    economicThemeOutput.textContent =
        themeDetails.output;
}


/* =========================================================
   PAGE: Analytics
   SECTION: Economic explorer interaction events
   ========================================================= */

economicThemeTabs.forEach((themeButton) => {
    themeButton.addEventListener("click", () => {
        updateEconomicTheme(themeButton);
    });
});