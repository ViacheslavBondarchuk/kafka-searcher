window.onload = function () {
    let themeSwitcher = new ThemeSwitcher(".wrapper .topbar-wrapper");

    window.ui = SwaggerUIBundle({
        url: 'kafka-searcher/swagger/openapi',
        dom_id: '#swagger-ui',
        docExpansion: 'none',
        syntaxHighlight: {
            activate: true,
            theme: themeSwitcher.currentTheme
        },
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        layout: "StandaloneLayout",
    });

    themeSwitcher.init();
}
