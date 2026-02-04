// brand/logo.js - Official Churpay logo usage
// All surfaces must source the logo from here to avoid drift.

const logoPng = require("../assets/churpay-logo.png");

export const Logo = {
  source: logoPng,
  aspectRatio: 1, // square mark
  sizes: {
    sm: 32,
    md: 44,
    lg: 64,
  },
  usage: {
    lightBg: {
      recommendedBg: "#F8FAFC",
      preferredText: "#1E3A5F",
    },
    darkBg: {
      recommendedBg: "#070A12",
      preferredText: "#F8FAFC",
    },
  },
};
