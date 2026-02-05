import React from "react";
import { View, StyleSheet } from "react-native";
import { useTheme } from "./theme";

export const Card = ({ children, style, padding }) => {
  const { palette, spacing, radius, scheme } = useTheme();
  const pad = padding ?? spacing.lg;
  const shadowOpacity = scheme === "dark" ? 0.2 : 0.08;

  return (
    <View
      style={[
        styles.base,
        {
          backgroundColor: palette.card,
          borderRadius: radius.lg,
          shadowColor: "#000",
          shadowOpacity,
          shadowRadius: 12,
          shadowOffset: { width: 0, height: 6 },
          elevation: 6,
          padding: pad,
        },
        style,
      ]}
    >
      {children}
    </View>
  );
};

const styles = StyleSheet.create({
  base: {},
});
