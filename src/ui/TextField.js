import React from "react";
import { View, Text, TextInput, StyleSheet } from "react-native";
import { useTheme } from "./theme";

export const TextField = ({ label, placeholder, value, onChangeText, keyboardType = "default", secureTextEntry }) => {
  const { palette, spacing, radius, typography } = useTheme();

  return (
    <View style={{ gap: spacing.xs }}>
      {label ? (
        <Text style={{ color: palette.muted, fontSize: typography.small, marginBottom: spacing.xs / 2 }}>{label}</Text>
      ) : null}
      <TextInput
        value={value}
        onChangeText={onChangeText}
        placeholder={placeholder}
        keyboardType={keyboardType}
        secureTextEntry={secureTextEntry}
        placeholderTextColor={palette.muted}
        style={[
          styles.input,
          {
            borderColor: palette.border,
            backgroundColor: palette.card,
            borderRadius: radius.md,
            paddingHorizontal: spacing.md + spacing.xs,
            paddingVertical: spacing.md,
            color: palette.text,
            fontSize: typography.body,
          },
        ]}
      />
    </View>
  );
};

const styles = StyleSheet.create({
  input: {
    borderWidth: 1,
  },
});
