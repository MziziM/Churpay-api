// brand/tokens.js - Single source of truth for Churpay design system
export const Brand = {
  name: "Churpay",

  colors: {
    // Brand colors from official logo
    navy: "#1E3A5F",          // Chur (navy)
    teal: "#0EA5B7",          // Pay + signal (teal)

    // UI backgrounds (dark, calm, trustworthy)
    bg: "#070A12",            // app background
    surface: "#0B1020",       // cards / panels
    card: "#0B1020",
    cardBorder: "rgba(255,255,255,0.10)",
    line: "rgba(248,250,252,0.10)",
    lineStrong: "rgba(248,250,252,0.14)",

    // Text colors
    text: "#F8FAFC",
    textMuted: "rgba(248,250,252,0.72)",
    textFaint: "rgba(248,250,252,0.55)",
    textOnPrimary: "#FFFFFF", // white text on teal

    // Semantic colors
    primary: "#0EA5B7",       // teal (logo)
    primaryDark: "#0C8A9A",   // hover/press state
    accent: "#1E3A5F",        // navy accent
    tealSoft: "rgba(14,165,183,0.20)",
    danger: "#F87171",
    success: "#34D399",
    warn: "#FCA5A5",
  },

  radius: {
    sm: 10,
    md: 16,
    lg: 22,
    pill: 999,
  },

  spacing: {
    xs: 6,
    sm: 10,
    md: 16,
    lg: 24,
    xl: 32,
  },

  type: {
    h1: 28,
    h2: 22,
    body: 16,
    small: 13,
    weightBold: "700",
    weightSemi: "600",
    weightRegular: "400",
  },

  shadow: {
    // RN uses shadow props; web uses box-shadow. We'll map both.
    web: "0 18px 48px rgba(0,0,0,0.45)",
  },

  gradients: {
    // Dark hero using brand teal + navy
    hero: "linear-gradient(140deg, rgba(14,165,183,0.30), rgba(27,59,95,0.28), #070A12)",
  },
};
