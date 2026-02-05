import React from "react";
import { Pressable, Text, StyleSheet, View } from "react-native";
import { useTheme } from "./theme";

export const PrimaryButton = ({ label, onPress, variant = "solid", disabled, style }) => {
  const { palette, spacing, radius } = useTheme();
  const isSolid = variant === "solid";

  return (
    <Pressable
      disabled={disabled}
      onPress={onPress}
      style={[
        styles.base,
        {
          backgroundColor: isSolid ? palette.primary : "transparent",
          borderColor: isSolid ? "transparent" : palette.border,
          borderWidth: isSolid ? 0 : 1,
          shadowOpacity: isSolid ? 0.12 : 0,
          height: 56,
          borderRadius: radius.pill,
          paddingHorizontal: spacing.xl,
        },
        style,
      ]}
    >
      <View style={styles.labelWrap}>
        <Text
          style={{
            color: isSolid ? palette.onPrimary : palette.text,
            fontWeight: "700",
            fontSize: 17,
            letterSpacing: 0.3,
          }}
        >
          {label}
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
