// brand/brand.js - Unified design tokens for ChurPay web + app
export const Brand = {
  colors: {
    light: {
      background: "#F7F9FC",
      card: "#FFFFFF",
      text: "#0B1324",
      muted: "#667085",
      border: "#E4E8F0",

      // ChurPay brand (teal primary + navy accent, like the reference)
      primary: "#0EA5A3", // teal button
      accent: "#1F2B4F",  // navy wordmark

      // subtle highlight for selected cards / chips
      focus: "#E6F7FA",

      // text on primary buttons
      onPrimary: "#FFFFFF",
    },
    dark: {
      background: "#061A3A",
      card: "#0B244A",
      text: "#EAF2FF",
      muted: "#9AA8C0",
      border: "#17335D",

      // Dark mode: teal primary pops
      primary: "#0EA5A3",
      accent: "#37C6D3",

      focus: "#0E3B6F",

      onPrimary: "#03121F",
      success: "#22C55E",
      danger: "#EF4444",
    },
  },

  spacing: { xs: 4, sm: 8, md: 12, lg: 20, xl: 28, xxl: 36 },

  radius: { sm: 12, md: 16, lg: 20, pill: 999 },

  typography: { h1: 28, h2: 22, body: 16, small: 14 },
};
