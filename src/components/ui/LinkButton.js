import React from "react";
import { Pressable, Text } from "react-native";
import { useTheme } from "./theme";

export const LinkButton = ({ label, onPress, align = "center" }) => {
  const { palette, typography } = useTheme();
  return (
    <Pressable onPress={onPress}>
      <Text style={{ color: palette.primary, textAlign: align, fontSize: typography.body, fontWeight: "600" }}>
        {label}
      </Text>
    </Pressable>
  );
};
