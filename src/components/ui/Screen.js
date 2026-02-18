import React from "react";
import { View, ScrollView, StyleSheet, useWindowDimensions } from "react-native";
import { SafeAreaView } from "react-native-safe-area-context";
import { useTheme } from "./theme";

export const Screen = ({ children, footer, disableScroll = false, contentContainerStyle, footerContainerStyle }) => {
  const { palette, spacing } = useTheme();
  const { width: viewportWidth } = useWindowDimensions();

  const horizontalPadding = viewportWidth >= 768 ? spacing.xxl : viewportWidth >= 420 ? spacing.xl : spacing.lg;
  const verticalPadding = viewportWidth >= 768 ? spacing.xxl : viewportWidth >= 420 ? spacing.xl : spacing.lg;
  const sectionGap = viewportWidth >= 420 ? spacing.xl : spacing.lg;
  const maxWidth = viewportWidth >= 1024 ? 820 : viewportWidth >= 768 ? 680 : 520;

  return (
    <SafeAreaView style={[styles.safe, { backgroundColor: palette.background }]}>
      <View style={styles.centerWrap}>
        <View style={[styles.contentWrap, { paddingHorizontal: horizontalPadding, maxWidth }]}>
          <ScrollView
            contentContainerStyle={[{ paddingVertical: verticalPadding, gap: sectionGap }, contentContainerStyle]}
            showsVerticalScrollIndicator={false}
            scrollEnabled={!disableScroll}
            keyboardShouldPersistTaps="handled"
          >
            {children}
          </ScrollView>
          {footer ? <View style={[{ paddingVertical: spacing.lg }, footerContainerStyle]}>{footer}</View> : null}
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
