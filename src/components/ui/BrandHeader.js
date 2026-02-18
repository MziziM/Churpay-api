import React from "react";
import { View, StyleSheet, useWindowDimensions } from "react-native";

import { BrandLogo } from "./BrandLogo";

export const BrandHeader = ({ compact }) => {
  const { width: viewportWidth } = useWindowDimensions();
  const isCompact = typeof compact === "boolean" ? compact : viewportWidth < 420;
  const logoWidth = isCompact
    ? Math.min(300, Math.max(230, viewportWidth * 0.78))
    : Math.min(420, Math.max(280, viewportWidth * 0.58));
  const logoHeight = Math.round(logoWidth / 2);

  return (
    <View style={styles.wrap}>
      <BrandLogo width={logoWidth} height={logoHeight} />
    </View>
  );
};

const styles = StyleSheet.create({
  wrap: {
    alignItems: "center",
    justifyContent: "center",
    // Prevent the logo from looking tiny on phones, but keep it compact enough for forms.
    paddingVertical: 4,
  },
});
