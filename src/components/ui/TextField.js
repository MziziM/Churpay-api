import React, { useState } from "react";
import { View, Text, TextInput, StyleSheet } from "react-native";
import { useTheme } from "./theme";

export const TextField = ({
  label,
  placeholder,
  value,
  onChangeText,
  keyboardType = "default",
  secureTextEntry,
  helper,
}) => {
  const { palette, spacing, radius, typography } = useTheme();
  const [focused, setFocused] = useState(false);

  return (
    <View style={{ gap: spacing.xs }}>
      {label ? (
        <Text style={{ color: palette.muted, fontSize: typography.small, fontWeight: "600" }}>{label}</Text>
      ) : null}
      <TextInput
        value={value}
        onChangeText={onChangeText}
        placeholder={placeholder}
        keyboardType={keyboardType}
        secureTextEntry={secureTextEntry}
        placeholderTextColor={palette.muted}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        style={[
          styles.input,
          {
            borderColor: focused ? palette.primary : palette.border,
            backgroundColor: palette.card,
            borderRadius: radius.md,
            paddingHorizontal: spacing.md + spacing.xs,
            paddingVertical: spacing.md,
            color: palette.text,
            fontSize: typography.body,
          },
        ]}
      />
      {helper ? <Text style={{ color: palette.muted, fontSize: typography.small }}>{helper}</Text> : null}
    </View>
  );
};

const styles = StyleSheet.create({
  input: {
    borderWidth: 1,
  },
});
