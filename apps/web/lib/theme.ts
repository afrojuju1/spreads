export type ThemePreference = "light" | "dark";

export const THEME_STORAGE_KEY = "spreads-theme";
export const DEFAULT_THEME_PREFERENCE: ThemePreference = "dark";

export function parseThemePreference(
  value: string | null | undefined,
): ThemePreference | null {
  return value === "light" || value === "dark" ? value : null;
}

export function applyThemePreference(
  themePreference: ThemePreference,
  root: HTMLElement,
): void {
  root.classList.toggle("dark", themePreference === "dark");
  root.dataset.theme = themePreference;
  root.style.colorScheme = themePreference;
}

export function buildThemeInitScript(): string {
  return `
    (() => {
      try {
        const stored = localStorage.getItem(${JSON.stringify(THEME_STORAGE_KEY)});
        const theme =
          stored === "light" || stored === "dark"
            ? stored
            : ${JSON.stringify(DEFAULT_THEME_PREFERENCE)};
        const root = document.documentElement;
        root.classList.toggle("dark", theme === "dark");
        root.dataset.theme = theme;
        root.style.colorScheme = theme;
      } catch (_error) {}
    })();
  `;
}
