import React from "react";
import { View, ScrollView, StyleSheet } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useTheme } from "./theme";

export const Screen = ({ children, footer, disableScroll = false }) => {
  const { palette, spacing } = useTheme();
  return (
    <SafeAreaView style={[styles.safe, { backgroundColor: palette.background }]}>
      <View style={[styles.body, { paddingHorizontal: spacing.xl }]}>
        <ScrollView
          contentContainerStyle={{ paddingVertical: spacing.xl, gap: spacing.xl }}
          showsVerticalScrollIndicator={false}
          scrollEnabled={!disableScroll}
        >
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
