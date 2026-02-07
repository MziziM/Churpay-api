import React from "react";
import { View, Image, StyleSheet } from "react-native";

export const BrandHeader = ({ compact = false }) => {
  return (
    <View style={styles.wrap}>
      <Image
        source={require("../../../assets/churpay-logo.png")}
        style={compact ? styles.logoCompact : styles.logo}
        resizeMode="contain"
      />
    </View>
  );
};

const styles = StyleSheet.create({
  wrap: {
    alignItems: "center",
    justifyContent: "center",
  },
  logo: {
    width: 220,
    height: 110,
  },
  logoCompact: {
    width: 160,
    height: 80,
  },
});
