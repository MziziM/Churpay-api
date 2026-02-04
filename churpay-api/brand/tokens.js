// brand/tokens.js - Single source of truth for Churpay design system
export const Brand = {
  name: "Churpay",

  colors: {
    // Brand colors from logo
    navy: "#1E3A5F",          // Chur (navy)
    teal: "#0EA5B7",          // Pay (teal)
    
    // UI backgrounds
    bg: "#F7F9FC",            // light background
    surface: "#FFFFFF",       // white surface
    card: "#FFFFFF",
    cardBorder: "#E2E8F0",

    // Text colors
    text: "#1E3A5F",          // navy for primary text
    textMuted: "#64748B",     // muted gray
    textFaint: "#94A3B8",     // faint gray
    textOnPrimary: "#FFFFFF", // white text on teal buttons

    // Semantic colors
    primary: "#0EA5B7",       // teal (from logo)
    primaryDark: "#0C8A9A",   // darker teal for hover
    accent: "#1E3A5F",        // navy accent
    danger: "#EF4444",
    success: "#22C55E",
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
    web: "0 12px 40px rgba(0,0,0,0.35)",
  },

  gradients: {
    // For web background if you want it
    hero: "linear-gradient(135deg, rgba(14,165,183,0.25), rgba(29,78,216,0.20), rgba(11,18,32,1))",
  },
};
