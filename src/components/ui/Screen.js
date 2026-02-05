import React from "react";
import { SafeAreaView, View, ScrollView, StyleSheet } from "react-native";
import { useTheme } from "./theme";

export const Screen = ({ children, footer }) => {
  const { palette, spacing } = useTheme();
  return (
    <SafeAreaView style={[styles.safe, { backgroundColor: palette.background }]}> 
      <View style={styles.centerWrap}>
        <View style={[styles.contentWrap, { paddingHorizontal: spacing.xl }]}> 
          <ScrollView contentContainerStyle={{ paddingVertical: spacing.xl, gap: spacing.xl }}>
            {children}
          </ScrollView>
          {footer ? <View style={{ paddingVertical: spacing.lg }}>{footer}</View> : null}
        </View>
      </View>
    </SafeAreaView>
  );
};

const styles = StyleSheet.create({
  safe: { flex: 1 },
  centerWrap: {
    flex: 1,
    alignItems: "center",
  },
  contentWrap: {
    flex: 1,
    width: "100%",
    maxWidth: 520,
  },
});
