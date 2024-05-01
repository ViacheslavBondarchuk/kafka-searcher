var ThemeSwitcher = function (selector = "", themes = []) {
    const SWAGGER_UI_THEME_KEY = "swagger-ui-theme";

    this.themes = ["agate", "arta", "monokai", "nord", "obsidian", "tomorrow-night", ...themes]
    this.defaultTheme = this.themes[Math.floor(Math.random() * themes.length)]
    this.currentTheme = localStorage.getItem(SWAGGER_UI_THEME_KEY) || this.defaultTheme;
    this.selector = selector;

    this.init = function () {
        let container = document.querySelector(this.selector);
        let themesSwitcherContainer = document.createElement("div")
        themesSwitcherContainer.classList.add("swagger-ui-theme-switcher-container");
        let label = document.createElement("label");
        label.classList.add("swagger-ui-theme-switcher-label");
        label.innerText = "Themes";

        if (container) {
            let select = document.createElement("select");
            select.classList.add("swagger-ui-theme-switcher");
            select.addEventListener("change", (e) => {
                localStorage.setItem(SWAGGER_UI_THEME_KEY, e.currentTarget.options[e.currentTarget.selectedIndex].value);
                location.reload();
            });

            for (let index in this.themes) {
                let option = document.createElement("option");
                option.value = this.themes[index];
                option.label = this.themes[index];
                option.index = parseInt(index);
                select.appendChild(option);
            }
            select.selectedIndex = this.themes.indexOf(this.currentTheme);
            localStorage.setItem(SWAGGER_UI_THEME_KEY, this.currentTheme);
            themesSwitcherContainer.appendChild(label);
            themesSwitcherContainer.appendChild(select);
            container.appendChild(themesSwitcherContainer);
        }
    }
}
