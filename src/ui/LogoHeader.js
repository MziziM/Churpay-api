import React from "react";
import { View, Text, StyleSheet } from "react-native";
import { useTheme } from "./theme";
import { BrandLogo } from "../components/ui/BrandLogo";

export const LogoHeader = ({ title, subtitle }) => {
  const { palette, spacing, typography } = useTheme();
  const logoWidth = 200;
  const logoHeight = 100;
  return (
    <View style={[styles.wrap, { gap: spacing.sm }]}>
      <BrandLogo width={logoWidth} height={logoHeight} style={styles.logo} />
      {subtitle ? <Text style={{ color: palette.muted, fontSize: typography.body }}>{subtitle}</Text> : null}
      {title ? <Text style={{ color: palette.text, fontSize: typography.h1, fontWeight: "700" }}>{title}</Text> : null}
    </View>
  );
};

const styles = StyleSheet.create({
  wrap: {
    alignItems: "center",
  },
  logo: {
    // Keep this for layout tweaks without encoding a fixed size in styles.
  },
});
