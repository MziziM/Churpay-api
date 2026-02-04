// brand/tokens.js - Single source of truth for Churpay design system
export const Brand = {
  name: "Churpay",

  colors: {
    bg: "#0B1220",
    surface: "#111A2E",
    card: "rgba(255,255,255,0.06)",
    cardBorder: "rgba(255,255,255,0.10)",

    text: "#FFFFFF",
    textMuted: "rgba(255,255,255,0.72)",
    textFaint: "rgba(255,255,255,0.55)",

    primary: "#0EA5B7",       // teal
    primary2: "#1D4ED8",      // blue accent
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
