import React from "react";
import { View, Text, Image, StyleSheet } from "react-native";
import { useTheme } from "./theme";

export const BrandHeader = () => {
  const { palette, spacing, typography } = useTheme();
  return (
    <View style={[styles.wrap, { gap: spacing.xs }]}> 
      <Image source={require("../../../assets/churpay-logo.png")} style={styles.logo} resizeMode="contain" />
      <Text style={[styles.tagline, { color: palette.muted, fontSize: typography.body }]}>Giving made easy.</Text>
    </View>
  );
};

const styles = StyleSheet.create({
  wrap: {
    alignItems: "center",
    justifyContent: "center",
  },
  logo: {
    width: 240,
    height: 90,
  },
  tagline: {
    fontWeight: "600",
  },
});
