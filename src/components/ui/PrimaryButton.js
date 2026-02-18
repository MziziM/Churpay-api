import React from "react";
import { Pressable, Text, StyleSheet, View } from "react-native";
import { useTheme } from "./theme";

export const PrimaryButton = ({ label, onPress, variant = "solid", disabled, loading, style }) => {
  const { palette, spacing, radius } = useTheme();
  const isSolid = variant === "solid";
  const isSecondary = variant === "secondary";
  const isGhost = variant === "ghost";

  const backgroundColor = isSolid ? palette.primary : isSecondary ? palette.focus : "transparent";
  const borderColor = isSolid ? "transparent" : palette.border;
  const textColor = isSolid ? palette.onPrimary : palette.text;
  const shadowOpacity = isSolid ? 0.12 : isSecondary ? 0.06 : 0;
  const elevation = isSolid ? 6 : isSecondary ? 2 : 0;

  return (
    <Pressable
      disabled={disabled || loading}
      onPress={onPress}
      style={({ pressed }) => [
        styles.base,
        {
          backgroundColor,
          borderColor,
          borderWidth: isSolid ? 0 : 1,
          shadowOpacity,
          height: 56,
          borderRadius: radius.pill,
          paddingHorizontal: spacing.xl,
          opacity: disabled || loading ? 0.6 : pressed ? 0.85 : 1,
          elevation,
        },
        style,
      ]}
    >
      <View style={styles.labelWrap}>
        <Text
          style={{
            color: textColor,
            fontWeight: "700",
            fontSize: 17,
            letterSpacing: 0.3,
          }}
        >
          {loading ? "Please wait..." : label}
        </Text>
      </View>
    </Pressable>
  );
};

const styles = StyleSheet.create({
  base: {
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
    shadowColor: "#000",
    shadowRadius: 12,
    shadowOffset: { width: 0, height: 6 },
    elevation: 6,
  },
  labelWrap: {
    alignItems: "center",
    justifyContent: "center",
  },
});
