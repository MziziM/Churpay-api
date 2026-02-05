import React from "react";
import { View, Text, Image, StyleSheet } from "react-native";
import { Logo } from "../../brand";
import { useTheme } from "./theme";

export const LogoHeader = ({ title, subtitle }) => {
  const { palette, spacing, typography } = useTheme();
  return (
    <View style={[styles.wrap, { gap: spacing.sm }]}>
      <Image source={Logo.source} style={styles.logo} />
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
    height: 72,
    width: 200,
    resizeMode: "contain",
  },
});
