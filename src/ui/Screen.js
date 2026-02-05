import React from "react";
import { SafeAreaView, View, ScrollView, StyleSheet } from "react-native";
import { useTheme } from "./theme";

export const Screen = ({ children, footer }) => {
  const { palette, spacing } = useTheme();
  return (
    <SafeAreaView style={[styles.safe, { backgroundColor: palette.background }]}>
      <View style={[styles.body, { paddingHorizontal: spacing.xl }]}>
        <ScrollView contentContainerStyle={{ paddingVertical: spacing.xl, gap: spacing.xl }}>
          {children}
        </ScrollView>
        {footer ? <View style={{ paddingVertical: spacing.lg }}>{footer}</View> : null}
      </View>
    </SafeAreaView>
  );
};

const styles = StyleSheet.create({
  safe: { flex: 1 },
  body: { flex: 1 },
});
