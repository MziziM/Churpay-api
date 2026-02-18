import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { View, Text, StyleSheet, ActivityIndicator, Linking, Image, Pressable, Alert, ScrollView, RefreshControl, Share, Modal, useWindowDimensions, AppState, Switch, Platform } from "react-native";
import { NavigationContainer, DefaultTheme } from "@react-navigation/native";
import { SafeAreaProvider } from "react-native-safe-area-context";
import QRCode from "react-native-qrcode-svg";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import AsyncStorage from "@react-native-async-storage/async-storage";
import * as Clipboard from "expo-clipboard";
import * as Updates from "expo-updates";
import { Screen } from "./src/components/ui/Screen";
import { Card } from "./src/components/ui/Card";
import { PrimaryButton } from "./src/components/ui/PrimaryButton";
import { TextField } from "./src/components/ui/TextField";
import { BrandHeader } from "./src/components/ui/BrandHeader";
import { BrandLogo } from "./src/components/ui/BrandLogo";
import { LinkButton } from "./src/components/ui/LinkButton";
import { useTheme } from "./src/components/ui/theme";
import { withTimeout, safe } from "./src/utils/boot";
import {
  getToken,
  loadSessionToken,
  setSessionToken,
  registerMember,
  loginMember,
  loginAdmin,
  verifyAdminTwoFactor,
  joinChurch,
  searchChurchesPublic,
  getProfile,
  updateProfile,
  listFunds,
  listTransactions,
  createPaymentIntent,
  createGivingLink,
  createCashGiving,
  getPaymentIntent,
  createFund,
  updateFund as apiUpdateFund,
  getMyChurchProfile,
  createMyChurchProfile,
  updateMyChurchProfile,
  getAdminDashboardTotals,
  getAdminRecentTransactions,
  exportAdminTransactionsCsv,
  getChurchQr,
  logout as apiLogout,
  canUseBiometrics,
  getBiometricEnabled,
  setBiometricEnabled,
  verifyMemberEmail,
  resendMemberVerification,
  requestPasswordReset,
  confirmPasswordReset,
  registerPushToken,
  unregisterPushToken,
  listNotifications,
  getUnreadNotificationCount,
  markNotificationRead,
  markAllNotificationsRead,
  getNotificationsEnabled,
  setNotificationsEnabled as apiSetNotificationsEnabled,
  confirmAdminCashGiving,
  rejectAdminCashGiving,
} from "./src/api";

const Stack = createNativeStackNavigator();
const AuthContext = React.createContext(null);

const money = (n) => `R ${Number(n || 0).toFixed(2)}`;
const PLATFORM_FEE_FIXED = 2.5;
const PLATFORM_FEE_PCT = 0.0075;
const PROFILE_CACHE_KEY = "churpay.profile.cache.v1";
const isAdminRole = (role) => role === "admin" || role === "accountant" || role === "super";
const formatDateInput = (date) => new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
const roundCurrency = (value) => Number((Math.round(Number(value || 0) * 100) / 100).toFixed(2));
const estimateCheckoutPricing = (amount) => {
  const donationAmount = roundCurrency(amount);
  const churpayFee = roundCurrency(PLATFORM_FEE_FIXED + donationAmount * PLATFORM_FEE_PCT);
  const totalCharged = roundCurrency(donationAmount + churpayFee);
  return { donationAmount, churpayFee, totalCharged };
};
const estimateCashPricing = (amount) => {
  // Cash records are FREE for now (fee = 0). Backend has a feature flag for later.
  const donationAmount = roundCurrency(amount);
  const churpayFee = 0;
  const totalCharged = donationAmount;
  return { donationAmount, churpayFee, totalCharged };
};
const normalizeCurrencyInput = (raw) => {
  let next = String(raw || "");
  next = next.replace(/[^0-9.,]/g, "").replace(/,/g, ".");
  const parts = next.split(".");
  if (parts.length > 2) next = `${parts[0]}.${parts.slice(1).join("")}`;
  return next;
};
const normalizeBirthDateInput = (raw) => {
  const value = String(raw || "").trim();
  if (!value) return null;

  let year = "";
  let month = "";
  let day = "";
  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    const [y, m, d] = value.split("-");
    year = y;
    month = m;
    day = d;
  } else if (/^\d{2}[-/]\d{2}[-/]\d{4}$/.test(value)) {
    const [d, m, y] = value.split(/[-/]/);
    year = y;
    month = m;
    day = d;
  } else {
    return null;
  }

  const y = Number.parseInt(year, 10);
  const m = Number.parseInt(month, 10);
  const d = Number.parseInt(day, 10);
  if (!Number.isInteger(y) || !Number.isInteger(m) || !Number.isInteger(d)) return null;
  if (y < 1900 || m < 1 || m > 12 || d < 1 || d > 31) return null;

  const parsed = new Date(Date.UTC(y, m - 1, d));
  if (parsed.getUTCFullYear() !== y || parsed.getUTCMonth() !== m - 1 || parsed.getUTCDate() !== d) return null;

  const today = new Date();
  const todayUtc = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), today.getUTCDate()));
  if (parsed > todayUtc) return null;

  return `${String(y).padStart(4, "0")}-${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
};
const firstNameFromFullName = (raw) => {
  const normalized = String(raw || "").trim();
  if (!normalized) return "";
  return normalized.split(/\s+/)[0] || "";
};
const formatBirthDateForInput = (raw) => {
  const normalized = normalizeBirthDateInput(raw);
  if (!normalized) return "";
  const [y, m, d] = normalized.split("-");
  return `${d}-${m}-${y}`;
};
const pickContactLabel = (name, phone, email, fallback = "-") => name || phone || email || fallback;
const resolveTransactionPersona = (txn) => {
  const paymentSource = String(txn?.paymentSource || "DIRECT_APP").toUpperCase();
  const payerType = String(txn?.payerType || "member").toLowerCase();
  const payerLabel = pickContactLabel(txn?.memberName, txn?.memberPhone, txn?.memberEmail, "Unknown payer");
  const beneficiaryLabel = pickContactLabel(txn?.onBehalfOfMemberName, txn?.onBehalfOfMemberPhone, txn?.onBehalfOfMemberEmail, "Member");
  const onBehalf = paymentSource === "SHARE_LINK" || payerType === "on_behalf";
  const visitor = !onBehalf && payerType === "visitor";
  return {
    onBehalf,
    visitor,
    payerLabel,
    beneficiaryLabel,
    tag: onBehalf ? "Paid on behalf" : visitor ? "Visitor payment" : "Member payment",
  };
};

let notificationModulesPromise = null;
async function loadNotificationModules() {
  if (notificationModulesPromise) return notificationModulesPromise;
  notificationModulesPromise = (async () => {
    try {
      // Metro bundler will fail if we import missing native modules. Use eval-require to keep the app bootable
      // even before the module is installed. Once installed, this resolves normally.
      // eslint-disable-next-line no-eval
      const req = eval("require");
      const Notifications = req("expo-notifications");
      let Constants = null;
      try {
        Constants = req("expo-constants");
      } catch (_err) {
        // Optional. We'll register a push token without projectId if this isn't present.
      }
      return { Notifications, Constants };
    } catch (_) {
      return null;
    }
  })();
  return notificationModulesPromise;
}

function nextSundayIso() {
  const now = new Date();
  const day = now.getUTCDay(); // 0=Sun..6=Sat
  const daysUntilSunday = (7 - day) % 7 || 7;
  const next = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  next.setUTCDate(next.getUTCDate() + daysUntilSunday);
  return next.toISOString().slice(0, 10);
}

const Body = ({ children, muted }) => {
  const { palette, typography } = useTheme();
  return <Text style={[styles.bodyText, { color: muted ? palette.muted : palette.text, fontSize: typography.body }]}>{children}</Text>;
};

const ErrorBanner = ({ message }) => {
  const { palette, spacing, typography } = useTheme();
  if (!message) return null;
  return (
    <Card
      style={{
        borderColor: palette.danger,
        backgroundColor: palette.focus,
        gap: spacing.xs,
      }}
    >
      <Text style={{ color: palette.danger, fontWeight: "700", fontSize: typography.small }}>Something went wrong</Text>
      <Text style={{ color: palette.danger, fontSize: typography.small }}>{message}</Text>
    </Card>
  );
};

const EmptyStateCard = ({ icon = "✨", title, subtitle, actionLabel, onAction }) => {
  const { palette, spacing, typography, radius } = useTheme();
  return (
    <Card
      style={{
        alignItems: "center",
        gap: spacing.sm,
        paddingVertical: spacing.xl,
        borderRadius: radius.lg,
      }}
    >
      <Text style={{ fontSize: 32 }}>{icon}</Text>
      <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "800", textAlign: "center", lineHeight: 30 }}>{title}</Text>
      {subtitle ? (
        <Text
          style={{
            color: palette.muted,
            textAlign: "center",
            fontSize: typography.body,
            lineHeight: 24,
            maxWidth: 420,
          }}
        >
          {subtitle}
        </Text>
      ) : null}
      {actionLabel && onAction ? <PrimaryButton label={actionLabel} variant="secondary" onPress={onAction} style={{ minWidth: 170 }} /> : null}
    </Card>
  );
};

const LoadingCards = ({ count = 3 }) => {
  const { palette, spacing } = useTheme();
  return (
    <View style={{ gap: spacing.sm }}>
      {Array.from({ length: count }).map((_, index) => (
        <View
          key={index}
          style={{
            height: 92,
            borderRadius: 18,
            borderWidth: 1,
            borderColor: palette.border,
            backgroundColor: palette.focus,
          }}
        />
      ))}
    </View>
  );
};

const SectionTitle = ({ title, subtitle, churchName, align = "left" }) => {
  const { palette, spacing, typography } = useTheme();
  return (
    <View style={{ gap: spacing.xs }}>
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1, textAlign: align }]}>{title}</Text>
      {subtitle ? (
        <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body, textAlign: align }]}>{subtitle}</Text>
      ) : null}
      {churchName ? (
        <Text style={{ color: palette.primary, fontSize: typography.small, textAlign: align, fontWeight: "700" }}>{churchName}</Text>
      ) : null}
    </View>
  );
};

const TopHeroHeader = ({ title, subtitle, churchName, badge, tone = "member", actions = [] }) => {
  const { palette, spacing, typography, radius, scheme } = useTheme();
  const { width: viewportWidth } = useWindowDimensions();
  const compact = viewportWidth < 380;
  const isAdmin = tone === "admin";

  const backgroundColor = isAdmin ? (scheme === "dark" ? "#0A2E5F" : "#EAF2FF") : palette.focus;
  const borderColor = isAdmin ? (scheme === "dark" ? "#1A4C89" : "#C8D8F5") : palette.border;
  const badgeBackground = isAdmin ? (scheme === "dark" ? "#0E4A8A" : "#D6E5FF") : palette.card;
  const badgeColor = isAdmin ? (scheme === "dark" ? "#B8DBFF" : "#13407A") : palette.primary;

  return (
    <Card
      padding={compact ? spacing.md : spacing.lg}
      style={{
        gap: spacing.md,
        backgroundColor,
        borderColor,
        overflow: "hidden",
      }}
    >
      <View
        pointerEvents="none"
        style={{
          position: "absolute",
          right: -28,
          top: -24,
          width: 128,
          height: 128,
          borderRadius: 999,
          backgroundColor: scheme === "dark" ? "rgba(255,255,255,0.08)" : "rgba(14,165,163,0.16)",
        }}
      />
      <View
        pointerEvents="none"
        style={{
          position: "absolute",
          right: 18,
          bottom: -34,
          width: 96,
          height: 96,
          borderRadius: 999,
          backgroundColor: scheme === "dark" ? "rgba(14,165,163,0.2)" : "rgba(31,43,79,0.1)",
        }}
      />

      <View style={{ gap: spacing.xs }}>
        {badge ? (
          <View
            style={{
              alignSelf: "flex-start",
              paddingVertical: 4,
              paddingHorizontal: spacing.md,
              borderRadius: radius.pill,
              backgroundColor: badgeBackground,
              borderWidth: 1,
              borderColor,
            }}
          >
            <Text style={{ color: badgeColor, fontSize: typography.small, fontWeight: "800", letterSpacing: 0.25 }}>{badge}</Text>
          </View>
        ) : null}
        <Text style={{ color: palette.text, fontSize: compact ? typography.h2 : typography.h1, fontWeight: "800" }}>{title}</Text>
        {subtitle ? <Text style={{ color: palette.muted, fontSize: typography.body, lineHeight: 24 }}>{subtitle}</Text> : null}
      </View>

      {churchName ? (
        <View
          style={{
            alignSelf: "flex-start",
            paddingVertical: 6,
            paddingHorizontal: spacing.md,
            borderRadius: radius.pill,
            borderWidth: 1,
            borderColor,
            backgroundColor: scheme === "dark" ? "rgba(255,255,255,0.06)" : "rgba(255,255,255,0.78)",
          }}
        >
          <Text style={{ color: palette.primary, fontSize: typography.small, fontWeight: "800" }}>{churchName}</Text>
        </View>
      ) : null}

      {actions.length ? (
        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: spacing.xs, paddingBottom: 2 }}>
          {actions.map((action) => (
            <Pressable
              key={action.label}
              onPress={action.onPress}
              style={({ pressed }) => ({
                minHeight: 40,
                paddingVertical: 8,
                paddingHorizontal: spacing.md,
                borderRadius: radius.pill,
                borderWidth: 1,
                borderColor: action.highlight ? palette.primary : borderColor,
                backgroundColor: action.highlight ? palette.primary : palette.card,
                opacity: pressed ? 0.88 : 1,
                justifyContent: "center",
              })}
            >
              <Text
                style={{
                  color: action.highlight ? palette.onPrimary : palette.text,
                  fontWeight: "700",
                  fontSize: typography.small,
                }}
              >
                {action.label}
              </Text>
            </Pressable>
          ))}
        </ScrollView>
      ) : null}
    </Card>
  );
};

const StatusChip = ({ label, active = true }) => {
  const { palette, spacing, typography } = useTheme();
  return (
    <View
      style={{
        paddingVertical: spacing.xs,
        paddingHorizontal: spacing.md,
        borderRadius: 999,
        backgroundColor: active ? palette.focus : palette.card,
        borderWidth: 1,
        borderColor: active ? palette.primary : palette.border,
      }}
    >
      <Text style={{ color: active ? palette.primary : palette.muted, fontSize: typography.small, fontWeight: "700" }}>{label}</Text>
    </View>
  );
};

const FundCard = ({ fund, selected, onPress }) => {
  const { palette, spacing, typography, radius } = useTheme();
  return (
    <Pressable onPress={onPress} style={({ pressed }) => ({ opacity: pressed ? 0.9 : 1 })}>
      <Card
        padding={spacing.lg}
        style={{
          borderColor: selected ? palette.primary : palette.border,
          borderWidth: 1,
          backgroundColor: selected ? palette.focus : palette.card,
          gap: spacing.sm,
          shadowOpacity: selected ? 0.18 : 0.1,
          elevation: selected ? 8 : 5,
        }}
      >
        <View style={{ flexDirection: "row", alignItems: "center", gap: spacing.md }}>
          <View
            style={{
              width: 42,
              height: 42,
              borderRadius: 21,
              alignItems: "center",
              justifyContent: "center",
              backgroundColor: palette.focus,
              borderWidth: 1,
              borderColor: palette.border,
            }}
          >
            <Text style={{ color: palette.primary, fontWeight: "800" }}>R</Text>
          </View>
          <View style={{ flex: 1 }}>
            <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>{fund.name}</Text>
            <View style={{ marginTop: spacing.xs, alignSelf: "flex-start", paddingHorizontal: spacing.sm, paddingVertical: 4, borderRadius: radius.pill, backgroundColor: palette.card, borderWidth: 1, borderColor: palette.border }}>
              <Text style={{ color: palette.muted, fontSize: typography.small, fontWeight: "700" }}>{String(fund.code || "").toUpperCase()}</Text>
            </View>
          </View>
          {selected ? <StatusChip label="Selected" active /> : null}
        </View>
      </Card>
    </Pressable>
  );
};

const QuickAmountChip = ({ label, active, onPress }) => {
  const { spacing, palette } = useTheme();
  return (
    <Pressable
      onPress={onPress}
      style={{
        minWidth: 74,
        alignItems: "center",
        paddingVertical: spacing.xs,
        paddingHorizontal: spacing.md,
        borderRadius: 999,
        borderWidth: 1,
        borderColor: active ? palette.primary : palette.border,
        backgroundColor: active ? palette.primary : palette.card,
        shadowColor: "#000",
        shadowOpacity: active ? 0.14 : 0.05,
        shadowRadius: active ? 8 : 4,
        shadowOffset: { width: 0, height: 2 },
        elevation: active ? 4 : 1,
      }}
    >
      <Text style={{ color: active ? palette.onPrimary : palette.text, fontWeight: "700" }}>{label}</Text>
    </Pressable>
  );
};

const AdminTabBar = ({ navigation, activeTab }) => {
  const { palette, spacing, typography, radius } = useTheme();
  const tabs = [
    { key: "funds", label: "Funds", screen: "AdminFunds" },
    { key: "qr", label: "QR", screen: "AdminQr" },
    { key: "transactions", label: "Transactions", screen: "AdminTransactions" },
    { key: "profile", label: "Profile", screen: "Profile" },
  ];

  return (
    <Card
      padding={spacing.sm}
      style={{
        backgroundColor: palette.focus,
        borderRadius: radius.lg,
      }}
    >
      <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: spacing.xs }}>
        {tabs.map((tab) => {
          const active = activeTab === tab.key;
          return (
            <Pressable
              key={tab.key}
              onPress={() => {
                if (!active) navigation.replace(tab.screen);
              }}
              style={({ pressed }) => ({
                minHeight: 40,
                paddingVertical: spacing.xs,
                paddingHorizontal: spacing.md,
                borderRadius: radius.pill,
                borderWidth: 1,
                borderColor: active ? palette.primary : palette.border,
                backgroundColor: active ? palette.primary : palette.card,
                flexDirection: "row",
                alignItems: "center",
                gap: spacing.xs,
                opacity: pressed ? 0.88 : 1,
              })}
            >
              <View
                style={{
                  width: 7,
                  height: 7,
                  borderRadius: 999,
                  backgroundColor: active ? palette.onPrimary : palette.border,
                }}
              />
              <Text style={{ color: active ? palette.onPrimary : palette.text, fontWeight: "700", fontSize: typography.small }}>
                {tab.label}
              </Text>
            </Pressable>
          );
        })}
      </ScrollView>
    </Card>
  );
};

function BootScreen() {
  const { palette } = useTheme();
  return (
    <View style={{ flex: 1, alignItems: "center", justifyContent: "center", gap: 16 }}>
      <BrandLogo width={196} height={98} />
      <Text style={{ color: palette.muted, fontSize: 16, fontWeight: "600" }}>Giving made easy.</Text>
      <ActivityIndicator color={palette.primary} />
    </View>
  );
}

function AuthProvider({ children }) {
  const [token, setTokenState] = useState(null);
  const [profile, setProfile] = useState(null);
  const [booting, setBooting] = useState(true);
  const [notificationsEnabled, setNotificationsEnabledState] = useState(true);
  const [notificationsSettingsLoading, setNotificationsSettingsLoading] = useState(true);
  const pushTokenRef = useRef(null);

  const loadCachedProfile = useCallback(async () => {
    try {
      const raw = await AsyncStorage.getItem(PROFILE_CACHE_KEY);
      if (!raw) return null;
      const parsed = JSON.parse(raw);
      return parsed && typeof parsed === "object" ? parsed : null;
    } catch (_err) {
      return null;
    }
  }, []);

  const saveCachedProfile = useCallback(async (nextProfile) => {
    try {
      if (!nextProfile) {
        await AsyncStorage.removeItem(PROFILE_CACHE_KEY);
        return;
      }
      await AsyncStorage.setItem(PROFILE_CACHE_KEY, JSON.stringify(nextProfile));
    } catch (_err) {
      // Non-fatal. Profile cache is best-effort.
    }
  }, []);

  useEffect(() => {
    const hardStop = setTimeout(() => setBooting(false), 7000);
    return () => clearTimeout(hardStop);
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const enabled = await getNotificationsEnabled();
        if (!cancelled) setNotificationsEnabledState(!!enabled);
      } catch (_err) {
        if (!cancelled) setNotificationsEnabledState(true);
      } finally {
        if (!cancelled) setNotificationsSettingsLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const refreshProfile = useCallback(async () => {
    // token state can lag immediately after setSession(); rely on cached token too.
    if (!token && !getToken()) return null;
    const res = await getProfile();
    const next = res?.member || res?.profile || res || null;
    if (next) {
      setProfile(next);
      await saveCachedProfile(next);
    }
    return next;
  }, [saveCachedProfile, token]);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        console.log("[boot] start");
        await withTimeout(
          (async () => {
            const stored = await safe(loadSessionToken());
            console.log("[boot] token loaded", !!stored);
            if (stored) {
              setTokenState(stored);

              // Load a cached profile immediately for offline/slow-network boots.
              const cached = await loadCachedProfile();
              if (cached && !cancelled) setProfile(cached);

              try {
                const profileRes = await withTimeout(getProfile(), 4000);
                const next = profileRes?.member || profileRes?.profile || profileRes || null;
                if (next && !cancelled) {
                  setProfile(next);
                  await saveCachedProfile(next);
                }
              } catch (err) {
                const status = Number(err?.status || 0);
                // Clear only when token is truly invalid/expired.
                if (status === 401 || status === 403) {
                  await setSessionToken(null);
                  await saveCachedProfile(null);
                  if (!cancelled) {
                    setTokenState(null);
                    setProfile(null);
                  }
                }
              }
            }
          })(),
          4000
        );
      } catch (err) {
        console.log("[boot] non-fatal", err?.message || err);
      } finally {
        if (!cancelled) {
          console.log("[boot] done");
          setBooting(false);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      if (!token || notificationsSettingsLoading) return;
      if (!notificationsEnabled) {
        const existingToken = pushTokenRef.current;
        pushTokenRef.current = null;
        if (existingToken) {
          await safe(unregisterPushToken({ token: existingToken }));
        }
        return;
      }

      const mods = await loadNotificationModules();
      if (!mods) {
        console.log("[push] expo-notifications not installed; skipping push registration");
        return;
      }

      const { Notifications, Constants } = mods;

      // Foreground behavior: show alerts. Keep this lightweight and non-blocking.
      try {
        Notifications.setNotificationHandler({
          handleNotification: async () => ({
            shouldShowAlert: true,
            shouldPlaySound: false,
            shouldSetBadge: false,
          }),
        });
      } catch (_err) {}

      try {
        const perms = await Notifications.getPermissionsAsync();
        let status = perms?.status;
        if (status !== "granted") {
          const requested = await Notifications.requestPermissionsAsync();
          status = requested?.status;
        }
        if (status !== "granted") return;

        const projectId = Constants?.expoConfig?.extra?.eas?.projectId || Constants?.easConfig?.projectId || null;
        const tokenRes = await Notifications.getExpoPushTokenAsync(projectId ? { projectId } : {});
        const expoPushToken = tokenRes?.data ? String(tokenRes.data) : "";
        if (!expoPushToken) return;

        if (cancelled) return;
        pushTokenRef.current = expoPushToken;

        await safe(registerPushToken({ token: expoPushToken, platform: Platform.OS }));
      } catch (err) {
        console.log("[push] registration failed", err?.message || err);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [token, notificationsEnabled, notificationsSettingsLoading]);

  const setSession = useCallback(
    async (data) => {
      if (data?.token) {
        await setSessionToken(data.token);
        setTokenState(data.token);
        // Optimistic profile from auth response, then hydrate from /auth/me so churchName is present.
        if (data?.member) {
          setProfile(data.member);
          await saveCachedProfile(data.member);
        } else if (data?.profile) {
          setProfile(data.profile);
          await saveCachedProfile(data.profile);
        }

        // Non-blocking "best effort" refresh; keep app usable even if offline.
        const fresh = await safe(getProfile());
        const next = fresh?.member || fresh?.profile || fresh || null;
        if (next) {
          setProfile(next);
          await saveCachedProfile(next);
        }
      } else {
        await setSessionToken(null);
        await saveCachedProfile(null);
        setTokenState(null);
        setProfile(null);
      }
    },
    [refreshProfile, saveCachedProfile]
  );

  const logout = useCallback(async () => {
    const pushToken = pushTokenRef.current;
    pushTokenRef.current = null;
    if (pushToken) {
      await safe(unregisterPushToken({ token: pushToken }));
    }
    await apiLogout();
    await setSessionToken(null);
    await saveCachedProfile(null);
    setTokenState(null);
    setProfile(null);
  }, [saveCachedProfile]);

  const updateNotificationsEnabled = useCallback(async (enabled) => {
    const next = !!enabled;
    await apiSetNotificationsEnabled(next);
    setNotificationsEnabledState(next);

    if (!next) {
      const pushToken = pushTokenRef.current;
      pushTokenRef.current = null;
      if (pushToken) {
        await safe(unregisterPushToken({ token: pushToken }));
      }
    }
    return next;
  }, []);

  const value = useMemo(
    () => ({
      token,
      profile,
      setSession,
      setProfile,
      refreshProfile,
      logout,
      booting,
      notificationsEnabled,
      notificationsSettingsLoading,
      updateNotificationsEnabled,
    }),
    [
      token,
      profile,
      setSession,
      refreshProfile,
      logout,
      booting,
      notificationsEnabled,
      notificationsSettingsLoading,
      updateNotificationsEnabled,
    ]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

function WelcomeScreen({ navigation }) {
  const { spacing, palette } = useTheme();
  const { width: viewportWidth } = useWindowDimensions();
  const { token, profile } = useContext(AuthContext);
  const heroLogoWidth = Math.min(500, Math.max(320, viewportWidth * 0.9));
  const heroLogoHeight = heroLogoWidth / 2;

  const continueFlow = () => {
    if (token && isAdminRole(profile?.role)) {
      return navigation.replace(profile?.churchId ? "AdminFunds" : "AdminChurch");
    }
    if (token && profile?.churchId) return navigation.replace("Give");
    if (token) return navigation.replace("JoinChurch");
    return navigation.navigate("Login");
  };

  return (
    <Screen
      disableScroll
      contentContainerStyle={{
        flexGrow: 1,
        justifyContent: "center",
        gap: spacing.lg,
      }}
      footer={
        <View>
          <PrimaryButton label="Continue" onPress={continueFlow} />
        </View>
      }
    >
      <View style={styles.hero}>
        <BrandLogo width={heroLogoWidth} height={heroLogoHeight} style={styles.heroLogo} />
        <Text style={[styles.heroTagline, { color: palette.muted }]}>Giving made easy.</Text>
      </View>
    </Screen>
  );
}

function LoginScreen({ navigation, route }) {
  const { spacing, palette, typography } = useTheme();
  const { setSession } = useContext(AuthContext);
  const [authMode, setAuthMode] = useState("member");
  const [identifier, setIdentifier] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [adminTwoFactor, setAdminTwoFactor] = useState(null);
  const [adminOtp, setAdminOtp] = useState("");
  const [adminOtpLoading, setAdminOtpLoading] = useState(false);
  const [adminOtpError, setAdminOtpError] = useState("");
  const isAdminAuth = authMode === "admin";

  useEffect(() => {
    const mode = route?.params?.mode;
    if (mode === "member" || mode === "admin") {
      setAuthMode(mode);
    }
  }, [route?.params?.mode]);

  useEffect(() => {
    // If user switches away from admin mode, close OTP prompt.
    if (!isAdminAuth && adminTwoFactor) {
      setAdminTwoFactor(null);
      setAdminOtp("");
      setAdminOtpError("");
    }
  }, [isAdminAuth, adminTwoFactor]);

  const onSubmit = async () => {
    try {
      setLoading(true);
      setError("");
      const data = isAdminAuth
        ? await loginAdmin({ identifier, password })
        : await loginMember({ identifier, password });

      if (isAdminAuth && data?.requiresTwoFactor && data?.twoFactor?.challengeId) {
        setAdminTwoFactor(data.twoFactor);
        setAdminOtp("");
        setAdminOtpError("");
        return;
      }

      await setSession(data);
      if (isAdminRole(data?.member?.role)) {
        navigation.replace(data?.member?.churchId ? "AdminFunds" : "AdminChurch");
      } else {
        navigation.replace(data?.member?.churchId ? "Give" : "JoinChurch");
      }
    } catch (e) {
      if (!isAdminAuth && e?.code === "EMAIL_VERIFICATION_REQUIRED") {
        navigation.replace("VerifyEmail", {
          identifier,
          email: e?.email || e?.data?.email,
          joinCode: null,
        });
        return;
      }
      setError(e?.message || "Login failed");
    } finally {
      setLoading(false);
    }
  };

  const onForgotPassword = () => {
    navigation.navigate("PasswordReset", { identifier, mode: authMode });
  };

  const closeAdminOtp = () => {
    setAdminTwoFactor(null);
    setAdminOtp("");
    setAdminOtpError("");
    setAdminOtpLoading(false);
  };

  const resendAdminOtp = async () => {
    try {
      setAdminOtpLoading(true);
      setAdminOtpError("");
      const data = await loginAdmin({ identifier, password });
      if (data?.requiresTwoFactor && data?.twoFactor?.challengeId) {
        setAdminTwoFactor(data.twoFactor);
        setAdminOtp("");
        return;
      }
      // If 2FA is disabled (or already verified), fall back to normal login completion.
      await setSession(data);
      if (isAdminRole(data?.member?.role)) {
        navigation.replace(data?.member?.churchId ? "AdminFunds" : "AdminChurch");
      } else {
        navigation.replace(data?.member?.churchId ? "Give" : "JoinChurch");
      }
    } catch (e) {
      setAdminOtpError(e?.message || "Failed to resend code");
    } finally {
      setAdminOtpLoading(false);
    }
  };

  const verifyAdminOtp = async () => {
    try {
      const challengeId = adminTwoFactor?.challengeId;
      if (!challengeId) throw new Error("Missing sign-in challenge. Please try again.");

      const code = String(adminOtp || "").trim();
      if (!code) throw new Error("Enter the 6-digit code.");

      setAdminOtpLoading(true);
      setAdminOtpError("");
      const data = await verifyAdminTwoFactor({ challengeId, code });
      closeAdminOtp();
      await setSession(data);
      navigation.replace(data?.member?.churchId ? "AdminFunds" : "AdminChurch");
    } catch (e) {
      setAdminOtpError(e?.message || "Could not verify code");
    } finally {
      setAdminOtpLoading(false);
    }
  };

  const modeHintTitle = isAdminAuth ? "Admin mode" : "Member mode";
  const modeHintText = isAdminAuth
    ? "You can switch back to member sign in at any time."
    : "Give quickly with your church and fund preferences.";

  return (
    <Screen
      contentContainerStyle={{
        flexGrow: 1,
        justifyContent: "flex-start",
        // Keep this screen compact so the mode hint + CTA is visible without scrolling.
        paddingVertical: spacing.md,
        gap: spacing.md,
      }}
      footerContainerStyle={{ paddingVertical: spacing.md }}
      footer={
        <View style={{ gap: spacing.sm }}>
          <View style={{ gap: 2 }}>
            <Text style={{ color: palette.text, fontWeight: "700" }}>{modeHintTitle}</Text>
            <Text style={{ color: palette.muted, fontSize: typography.small }}>{modeHintText}</Text>
          </View>
          <PrimaryButton label={loading ? "Signing in..." : "Sign In"} onPress={onSubmit} disabled={!identifier || !password || loading} />
          {!isAdminAuth ? <LinkButton label="Create an account" onPress={() => navigation.navigate("Register")} /> : null}
        </View>
      }
    >
      <View style={{ marginTop: -10, gap: spacing.md }}>
        <BrandHeader />
        <SectionTitle
          title="Welcome back"
          subtitle={isAdminAuth ? "Admin sign in with phone or email." : "Member sign in with phone or email."}
          align="center"
        />
      </View>
      <Card style={{ gap: spacing.md }}>
        <View
          style={{
            flexDirection: "row",
            alignSelf: "center",
            borderRadius: 999,
            borderWidth: 1,
            borderColor: palette.border,
            padding: spacing.xs,
            backgroundColor: palette.focus,
          }}
        >
          <Pressable
            onPress={() => setAuthMode("member")}
            style={{
              paddingVertical: spacing.xs,
              paddingHorizontal: spacing.md + spacing.xs,
              borderRadius: 999,
              backgroundColor: authMode === "member" ? palette.primary : "transparent",
            }}
          >
            <Text style={{ color: authMode === "member" ? palette.onPrimary : palette.text, fontWeight: "700" }}>Member</Text>
          </Pressable>
          <Pressable
            onPress={() => setAuthMode("admin")}
            style={{
              paddingVertical: spacing.xs,
              paddingHorizontal: spacing.md + spacing.xs,
              borderRadius: 999,
              backgroundColor: authMode === "admin" ? palette.primary : "transparent",
            }}
          >
            <Text style={{ color: authMode === "admin" ? palette.onPrimary : palette.text, fontWeight: "700" }}>Admin</Text>
          </Pressable>
        </View>
        <TextField
          label={isAdminAuth ? "Admin phone or email" : "Phone or email"}
          value={identifier}
          onChangeText={setIdentifier}
          placeholder="0712345678 or you@example.com"
        />
        <TextField label="Password" value={password} onChangeText={setPassword} placeholder="Enter your password" secureTextEntry />
        <LinkButton label="Forgot password?" align="left" onPress={onForgotPassword} />
      </Card>
      <ErrorBanner message={error} />

      <Modal transparent animationType="fade" visible={!!adminTwoFactor?.challengeId} onRequestClose={closeAdminOtp}>
        <View
          style={{
            flex: 1,
            backgroundColor: "rgba(0,0,0,0.55)",
            justifyContent: "center",
            padding: spacing.lg,
          }}
        >
          <Card style={{ gap: spacing.md }}>
            <View style={{ gap: 6 }}>
              <Text style={{ color: palette.text, fontWeight: "800", fontSize: typography.h2 }}>Enter admin code</Text>
              <Text style={{ color: palette.muted }}>
                We sent a 6-digit sign-in code to your email{adminTwoFactor?.emailMasked ? ` (${adminTwoFactor.emailMasked})` : ""}.
              </Text>
            </View>
            <TextField
              label="Code"
              value={adminOtp}
              onChangeText={setAdminOtp}
              placeholder="123456"
              keyboardType="number-pad"
            />
            <ErrorBanner message={adminOtpError} />
            <View style={{ gap: spacing.sm }}>
              <PrimaryButton
                label={adminOtpLoading ? "Verifying..." : "Verify and sign in"}
                onPress={verifyAdminOtp}
                disabled={adminOtpLoading || !adminOtp.trim()}
              />
              <PrimaryButton label="Resend code" variant="secondary" onPress={resendAdminOtp} disabled={adminOtpLoading} />
              <PrimaryButton label="Cancel" variant="ghost" onPress={closeAdminOtp} disabled={adminOtpLoading} />
            </View>
          </Card>
        </View>
      </Modal>
    </Screen>
  );
}

function PasswordResetScreen({ navigation, route }) {
  const { palette, spacing } = useTheme();
  const mode = route?.params?.mode === "admin" ? "admin" : "member";

  const [step, setStep] = useState("request"); // request -> confirm
  const [identifier, setIdentifier] = useState(String(route?.params?.identifier || "").trim());
  const [code, setCode] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newPasswordConfirm, setNewPasswordConfirm] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [status, setStatus] = useState("");

  const onRequest = async () => {
    try {
      setLoading(true);
      setError("");
      setStatus("");
      await requestPasswordReset({ identifier });
      setStatus("If the account exists, we sent a reset code to the email on file.");
      setStep("confirm");
    } catch (e) {
      setError(e?.message || "Failed to request password reset");
    } finally {
      setLoading(false);
    }
  };

  const onConfirm = async () => {
    try {
      setLoading(true);
      setError("");
      setStatus("");
      await confirmPasswordReset({
        identifier,
        code,
        newPassword,
        newPasswordConfirm,
      });
      Alert.alert("Password updated", "You can now sign in with your new password.", [
        {
          text: "OK",
          onPress: () => navigation.replace("Login", { mode }),
        },
      ]);
    } catch (e) {
      setError(e?.message || "Failed to reset password");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Screen
      disableScroll
      contentContainerStyle={{
        flexGrow: 1,
        justifyContent: "center",
        gap: spacing.lg,
      }}
      footer={
        <View style={{ gap: spacing.sm }}>
          {step === "request" ? (
            <PrimaryButton label={loading ? "Sending..." : "Send reset code"} onPress={onRequest} disabled={!identifier || loading} />
          ) : (
            <PrimaryButton
              label={loading ? "Updating..." : "Reset password"}
              onPress={onConfirm}
              disabled={!identifier || !code || !newPassword || !newPasswordConfirm || loading}
            />
          )}
          <LinkButton label="Back to sign in" onPress={() => navigation.replace("Login", { mode })} />
        </View>
      }
    >
      <View style={{ marginTop: -10, gap: spacing.md }}>
        <BrandHeader />
        <SectionTitle
          title="Reset password"
          subtitle={
            step === "request"
              ? "We’ll email you a code to reset your password."
              : "Enter the code from your email and choose a new password."
          }
          align="center"
        />
      </View>

      <Card style={{ gap: spacing.md }}>
        <TextField
          label="Phone or email"
          value={identifier}
          onChangeText={setIdentifier}
          placeholder="0712345678 or you@example.com"
          editable={!loading}
        />

        {step === "confirm" ? (
          <>
            <TextField label="Reset code" value={code} onChangeText={setCode} placeholder="6-digit code" editable={!loading} />
            <TextField
              label="New password"
              value={newPassword}
              onChangeText={setNewPassword}
              placeholder="Enter a new password"
              secureTextEntry
              editable={!loading}
            />
            <TextField
              label="Confirm new password"
              value={newPasswordConfirm}
              onChangeText={setNewPasswordConfirm}
              placeholder="Confirm your new password"
              secureTextEntry
              editable={!loading}
            />
            <LinkButton
              label={loading ? "Resend disabled..." : "Resend code"}
              align="left"
              onPress={onRequest}
              disabled={loading || !identifier}
            />
          </>
        ) : null}
      </Card>

      {status ? (
        <Card style={{ backgroundColor: palette.focus, borderColor: palette.border }}>
          <Text style={{ color: palette.muted }}>{status}</Text>
        </Card>
      ) : null}

      <ErrorBanner message={error} />
    </Screen>
  );
}

function RegisterScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { setSession, setProfile, refreshProfile } = useContext(AuthContext);
  const [fullName, setFullName] = useState("");
  const [birthDate, setBirthDate] = useState("");
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("");
  const [confirmEmail, setConfirmEmail] = useState("");
  const [password, setPassword] = useState("");
  const [joinCode, setJoinCode] = useState("");
  const [churchQuery, setChurchQuery] = useState("");
  const [churchMatches, setChurchMatches] = useState([]);
  const [churchSearching, setChurchSearching] = useState(false);
  const [acceptTerms, setAcceptTerms] = useState(false);
  const [acceptCookies, setAcceptCookies] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    let cancelled = false;
    const q = String(churchQuery || "").trim();
    if (q.length < 2) {
      setChurchMatches([]);
      return;
    }
    const t = setTimeout(async () => {
      try {
        setChurchSearching(true);
        const res = await searchChurchesPublic(q, { limit: 10 });
        const list = res?.churches || res?.data?.churches || [];
        if (!cancelled) setChurchMatches(Array.isArray(list) ? list : []);
      } catch (_e) {
        if (!cancelled) setChurchMatches([]);
      } finally {
        if (!cancelled) setChurchSearching(false);
      }
    }, 450);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [churchQuery]);

  const onSubmit = async () => {
    const normalizedEmail = String(email || "").trim().toLowerCase();
    const normalizedConfirmEmail = String(confirmEmail || "").trim().toLowerCase();
    const requestedJoinCode = String(joinCode || "").trim().toUpperCase();
    const normalizedBirthDate = normalizeBirthDateInput(birthDate);
    if (!normalizedEmail) {
      setError("Email is required");
      return;
    }
    if (!normalizedBirthDate) {
      setError("Date of birth is required (DD-MM-YYYY)");
      return;
    }
    if (normalizedEmail !== normalizedConfirmEmail) {
      setError("Email addresses do not match");
      return;
    }
    if (!requestedJoinCode) {
      setError("Join code is required");
      return;
    }
    if (!acceptTerms) {
      setError("Please accept the Terms and Conditions to create an account.");
      return;
    }
    if (!acceptCookies) {
      setError("Please accept cookie consent to create an account.");
      return;
    }

    try {
      setLoading(true);
      setError("");
      const data = await registerMember({
        fullName,
        dateOfBirth: normalizedBirthDate,
        phone,
        email: normalizedEmail,
        emailConfirm: normalizedConfirmEmail,
        password,
        joinCode: requestedJoinCode,
        acceptTerms: true,
        acceptCookies: true,
      });

      // Member registration requires email verification before issuing a JWT.
      if (data?.verificationRequired && !data?.token) {
        navigation.replace("VerifyEmail", {
          identifier: phone || normalizedEmail,
          email: normalizedEmail,
          joinCode: requestedJoinCode || null,
        });
        return;
      }

      await setSession(data);
      if (requestedJoinCode) {
        try {
          const res = await joinChurch(requestedJoinCode);
          if (res?.token) await setSession(res);
          else if (res?.member) setProfile(res.member);
          await refreshProfile();
          navigation.replace("Give");
          return;
        } catch (e) {
          // Account exists; let them finish joining on the join screen with the code prefilled.
          navigation.replace("JoinChurch", { joinCode: requestedJoinCode, mode: "join" });
          return;
        }
      }
      navigation.replace("JoinChurch", { mode: "join" });
    } catch (e) {
      setError(e?.message || "Registration failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Screen
      footer={
        <PrimaryButton
          label={loading ? "Creating..." : "Create account"}
          onPress={onSubmit}
          disabled={
            !fullName ||
            !normalizeBirthDateInput(birthDate) ||
            !phone ||
            !email ||
            !confirmEmail ||
            !password ||
            !String(joinCode || "").trim() ||
            !acceptTerms ||
            !acceptCookies ||
            loading
          }
        />
      }
    >
      <BrandHeader />
      <SectionTitle title="Create account" subtitle="We’ll keep your details safe." />
      <Card style={{ gap: spacing.md }}>
        <TextField label="Full name" value={fullName} onChangeText={setFullName} placeholder="e.g. Thandi Dlamini" />
        <TextField
          label="Date of birth (required)"
          value={birthDate}
          onChangeText={setBirthDate}
          placeholder="DD-MM-YYYY"
          keyboardType="numbers-and-punctuation"
          helper="Use day-month-year, e.g. 27-05-1994"
        />
        <TextField label="Mobile number" value={phone} onChangeText={setPhone} placeholder="e.g. 0712345678" keyboardType="phone-pad" />
        <TextField label="Email" value={email} onChangeText={setEmail} placeholder="you@example.com" keyboardType="email-address" />
        <TextField
          label="Confirm email"
          value={confirmEmail}
          onChangeText={setConfirmEmail}
          placeholder="re-enter your email"
          keyboardType="email-address"
        />
        <TextField label="Password" value={password} onChangeText={setPassword} placeholder="••••••" secureTextEntry />
      </Card>

      <Card style={{ gap: spacing.md }}>
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Join your church (required)</Text>
        <Text style={{ color: palette.muted }}>Paste the join code, or search your church name and pick the code.</Text>

        <TextField
          label="Join code"
          value={joinCode}
          onChangeText={(value) => setJoinCode(String(value || "").toUpperCase())}
          placeholder="e.g. GCCOC-1234"
          autoCapitalize="characters"
        />

        <TextField
          label="Search church name"
          value={churchQuery}
          onChangeText={setChurchQuery}
          placeholder="Start typing your church name..."
        />

        {churchSearching ? <Text style={{ color: palette.muted, fontSize: typography.small }}>Searching…</Text> : null}
        {churchMatches?.length ? (
          <View style={{ gap: spacing.xs }}>
            {churchMatches.map((c) => (
              <Pressable
                key={c.id || c.joinCode || c.name}
                onPress={() => {
                  setJoinCode(String(c.joinCode || "").toUpperCase());
                  setChurchQuery(String(c.name || ""));
                  setChurchMatches([]);
                }}
                style={{
                  paddingVertical: spacing.sm,
                  paddingHorizontal: spacing.md,
                  borderRadius: 12,
                  borderWidth: 1,
                  borderColor: palette.border,
                  backgroundColor: palette.focus,
                }}
              >
                <Text style={{ color: palette.text, fontWeight: "700" }}>{c.name}</Text>
                <Text style={{ color: palette.muted, marginTop: 2 }}>{String(c.joinCode || "").toUpperCase()}</Text>
              </Pressable>
            ))}
          </View>
        ) : null}
      </Card>

      <Card style={{ gap: spacing.sm }}>
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Terms & Conditions</Text>
        <Text style={{ color: palette.muted }}>
          To create an account, you must agree to the Terms, Privacy, and cookie consent.
        </Text>
        <Pressable
          onPress={() => Linking.openURL("https://churpay.com/legal/terms")}
          style={({ pressed }) => ({
            paddingVertical: spacing.sm,
            opacity: pressed ? 0.7 : 1,
          })}
        >
          <Text style={{ color: palette.primary, fontWeight: "800" }}>View Terms and Conditions</Text>
        </Pressable>
        <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: spacing.md }}>
          <Text style={{ color: palette.text, flex: 1 }}>
            I agree to the Terms and Conditions
          </Text>
          <Switch value={acceptTerms} onValueChange={(value) => setAcceptTerms(!!value)} />
        </View>
        <Pressable
          onPress={() => Linking.openURL("https://churpay.com/legal/privacy")}
          style={({ pressed }) => ({
            paddingVertical: spacing.sm,
            opacity: pressed ? 0.7 : 1,
          })}
        >
          <Text style={{ color: palette.primary, fontWeight: "800" }}>View Privacy Policy</Text>
        </Pressable>
        <View style={{ flexDirection: "row", alignItems: "center", justifyContent: "space-between", gap: spacing.md }}>
          <Text style={{ color: palette.text, flex: 1 }}>
            I agree to cookie consent and the Privacy Policy
          </Text>
          <Switch value={acceptCookies} onValueChange={(value) => setAcceptCookies(!!value)} />
        </View>
      </Card>
      <ErrorBanner message={error} />
    </Screen>
  );
}

function VerifyEmailScreen({ navigation, route }) {
  const { spacing, palette, typography } = useTheme();
  const { setSession, setProfile, refreshProfile } = useContext(AuthContext);

  const identifier = String(route?.params?.identifier || "").trim();
  const email = String(route?.params?.email || "").trim().toLowerCase();
  const joinCode = route?.params?.joinCode ? String(route.params.joinCode).trim().toUpperCase() : "";

  const [code, setCode] = useState("");
  const [loading, setLoading] = useState(false);
  const [resending, setResending] = useState(false);
  const [error, setError] = useState("");
  const [info, setInfo] = useState("");

  const onVerify = async () => {
    const cleaned = String(code || "").replace(/\s+/g, "");
    if (!cleaned) {
      setError("Enter the 6-digit code from your email.");
      return;
    }

    try {
      setLoading(true);
      setError("");
      setInfo("");
      const data = await verifyMemberEmail({
        identifier: identifier || undefined,
        email: email || undefined,
        code: cleaned,
      });
      await setSession(data); // should include token now

      const requestedJoinCode = joinCode;
      if (requestedJoinCode) {
        try {
          const res = await joinChurch(requestedJoinCode);
          if (res?.token) await setSession(res);
          else if (res?.member) setProfile(res.member);
          await refreshProfile();
          navigation.replace("Give");
          return;
        } catch (_e) {
          navigation.replace("JoinChurch", { joinCode: requestedJoinCode, mode: "join" });
          return;
        }
      }

      // If they already belong to a church, go to giving. Otherwise ask them to join.
      const next = (data?.member?.churchId || data?.profile?.churchId) ? "Give" : "JoinChurch";
      navigation.replace(next, { mode: "join" });
    } catch (e) {
      setError(e?.message || "Verification failed");
    } finally {
      setLoading(false);
    }
  };

  const onResend = async () => {
    try {
      setResending(true);
      setError("");
      setInfo("");
      const res = await resendMemberVerification({
        identifier: identifier || undefined,
        email: email || undefined,
      });
      const expiresAt = res?.data?.expiresAt || res?.expiresAt;
      setInfo(expiresAt ? `Verification code sent. Expires at ${String(expiresAt)}.` : "Verification code sent. Check your email.");
    } catch (e) {
      setError(e?.message || "Could not resend code");
    } finally {
      setResending(false);
    }
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={loading ? "Verifying..." : "Verify email"} onPress={onVerify} disabled={!code || loading} />
          <PrimaryButton label={resending ? "Resending..." : "Resend code"} variant="secondary" onPress={onResend} disabled={resending || loading} />
          <PrimaryButton label="Back to login" variant="ghost" onPress={() => navigation.replace("Login", { mode: "member" })} />
        </View>
      }
    >
      <BrandHeader />
      <SectionTitle title="Verify your email" subtitle="Enter the 6-digit code we sent to your email address." />

      <Card style={{ gap: spacing.md }}>
        <View style={{ gap: spacing.xs }}>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Verification</Text>
          <Text style={{ color: palette.muted, fontSize: typography.small }}>
            {email ? `Email: ${email}` : "Check your email for a 6-digit code."}
          </Text>
        </View>
        <TextField
          label="6-digit code"
          value={code}
          onChangeText={setCode}
          placeholder="123456"
          keyboardType="number-pad"
        />
        {info ? (
          <Card style={{ borderColor: palette.primary, backgroundColor: palette.focus }}>
            <Text style={{ color: palette.primary, fontWeight: "700" }}>Sent</Text>
            <Text style={{ color: palette.muted, marginTop: spacing.xs }}>{info}</Text>
          </Card>
        ) : null}
      </Card>

      <ErrorBanner message={error} />
    </Screen>
  );
}

function JoinChurchScreen({ navigation, route }) {
  const { spacing, palette, typography } = useTheme();
  const { profile, refreshProfile, setProfile, setSession, token, logout } = useContext(AuthContext);
  const [joinCode, setJoinCode] = useState("");
  const [churchQuery, setChurchQuery] = useState("");
  const [churchMatches, setChurchMatches] = useState([]);
  const [churchSearching, setChurchSearching] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const isSwitchMode = route?.params?.mode === "switch";

  useEffect(() => {
    const prefill = route?.params?.joinCode;
    if (prefill) setJoinCode(String(prefill));
  }, [route?.params?.joinCode]);

  useEffect(() => {
    let cancelled = false;
    const q = String(churchQuery || "").trim();
    if (q.length < 2) {
      setChurchMatches([]);
      return;
    }
    const t = setTimeout(async () => {
      try {
        setChurchSearching(true);
        const res = await searchChurchesPublic(q, { limit: 10 });
        const list = res?.churches || res?.data?.churches || [];
        if (!cancelled) setChurchMatches(Array.isArray(list) ? list : []);
      } catch (_e) {
        if (!cancelled) setChurchMatches([]);
      } finally {
        if (!cancelled) setChurchSearching(false);
      }
    }, 450);
    return () => {
      cancelled = true;
      clearTimeout(t);
    };
  }, [churchQuery]);

  useEffect(() => {
    if (!token) navigation.replace("Welcome");
    if (isAdminRole(profile?.role)) navigation.replace(profile?.churchId ? "AdminFunds" : "AdminChurch");
    if (profile?.churchId && !isSwitchMode) navigation.replace("Give");
  }, [isSwitchMode, profile?.churchId, profile?.role, navigation, token]);

  const onSubmit = async () => {
    const requestedJoinCode = String(joinCode || "").trim().toUpperCase();
    if (!requestedJoinCode) {
      setError("Join code is required");
      return;
    }
    try {
      setLoading(true);
      setError("");
      const res = await joinChurch(requestedJoinCode);
      if (res?.token) await setSession(res);
      else if (res?.member) setProfile(res.member);
      await refreshProfile();
      navigation.replace("Give");
    } catch (e) {
      const message = e?.message || "Could not join church";
      if (String(message).toLowerCase().includes("unauthorized")) {
        await logout();
        navigation.replace("Login");
        return;
      }
      setError(message);
    } finally {
      setLoading(false);
    }
  };

  const title = isSwitchMode ? "Switch church" : "Join your church";
  const subtitle = isSwitchMode
    ? "Enter the new join code shared by your church admin."
    : "Enter the join code from your church admin.";

  return (
    <Screen footer={<PrimaryButton label={loading ? "Saving..." : isSwitchMode ? "Switch church" : "Join church"} onPress={onSubmit} disabled={!joinCode || loading} />}>
      <BrandHeader />
      <SectionTitle title={title} subtitle={subtitle} />
      <Card style={{ gap: spacing.md }}>
        <TextField
          label="Join code"
          value={joinCode}
          onChangeText={(value) => setJoinCode(String(value || "").toUpperCase())}
          placeholder="e.g. GCCOC-1234"
          autoCapitalize="characters"
        />
      </Card>
      <Card style={{ gap: spacing.md }}>
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Find your church</Text>
        <Text style={{ color: palette.muted }}>Search by church name and tap the join code.</Text>
        <TextField
          label="Search church name"
          value={churchQuery}
          onChangeText={setChurchQuery}
          placeholder="Start typing your church name..."
        />
        {churchSearching ? <Text style={{ color: palette.muted, fontSize: typography.small }}>Searching…</Text> : null}
        {churchMatches?.length ? (
          <View style={{ gap: spacing.xs }}>
            {churchMatches.map((c) => (
              <Pressable
                key={c.id || c.joinCode || c.name}
                onPress={() => {
                  setJoinCode(String(c.joinCode || "").toUpperCase());
                  setChurchQuery(String(c.name || ""));
                  setChurchMatches([]);
                }}
                style={{
                  paddingVertical: spacing.sm,
                  paddingHorizontal: spacing.md,
                  borderRadius: 12,
                  borderWidth: 1,
                  borderColor: palette.border,
                  backgroundColor: palette.focus,
                }}
              >
                <Text style={{ color: palette.text, fontWeight: "700" }}>{c.name}</Text>
                <Text style={{ color: palette.muted, marginTop: 2 }}>{String(c.joinCode || "").toUpperCase()}</Text>
              </Pressable>
            ))}
          </View>
        ) : null}
      </Card>
      <ErrorBanner message={error} />
    </Screen>
  );
}

function GiveScreen({ navigation, route }) {
  const { spacing, palette, typography, radius } = useTheme();
  const { profile, token, refreshProfile } = useContext(AuthContext);
  const [funds, setFunds] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [amount, setAmount] = useState("");
  const [titheIncome, setTitheIncome] = useState("");
  const [error, setError] = useState("");
  const [unreadCount, setUnreadCount] = useState(0);
  const lastPrefillKeyRef = useRef("");

  useEffect(() => {
    if (!funds?.length) return;

    const fundCode = String(route?.params?.fundCode || "").trim().toLowerCase();
    const amountRaw = route?.params?.amount;
    const key = `${fundCode}|${String(amountRaw ?? "")}`;

    if (key === "|" || lastPrefillKeyRef.current === key) return;
    lastPrefillKeyRef.current = key;

    if (fundCode) {
      const match = funds.find((f) => String(f?.code || "").trim().toLowerCase() === fundCode);
      if (match) setSelected(match);
    }

    const n = Number(amountRaw);
    if (Number.isFinite(n) && n > 0) {
      setAmount(String(n));
      setError("");
    }
  }, [funds, route?.params?.amount, route?.params?.fundCode]);

  const refreshUnreadCount = useCallback(async () => {
    try {
      const res = await getUnreadNotificationCount();
      setUnreadCount(Number(res?.count || 0));
    } catch (_err) {
      // non-fatal
    }
  }, []);

  const loadFunds = useCallback(async () => {
    if (!token) {
      navigation.replace("Welcome");
      return;
    }
    if (isAdminRole(profile?.role)) {
      navigation.replace(profile?.churchId ? "AdminFunds" : "AdminChurch");
      return;
    }
    if (!profile?.churchId) {
      // Hydrate once from /auth/me before forcing a re-join flow.
      const hydrated = await safe(refreshProfile());
      const hydratedChurchId =
        hydrated?.churchId ||
        hydrated?.member?.churchId ||
        hydrated?.profile?.churchId ||
        null;
      if (!hydratedChurchId) {
        navigation.replace("JoinChurch");
        return;
      }
    }
    try {
      setLoading(true);
      setError("");
      const data = await listFunds();
      const nextFunds = data?.funds || [];
      setFunds(nextFunds);
      // If the fund list changes (e.g. switch church), clear any stale selection.
      setSelected((prev) => (prev && nextFunds.some((f) => f.id === prev.id) ? prev : null));
    } catch (e) {
      setError(e?.message || "Could not load funds");
    } finally {
      setLoading(false);
    }
  }, [navigation, profile?.churchId, profile?.role, refreshProfile, token]);

  useEffect(() => {
    // Load initially and whenever the screen regains focus (e.g. after switching church or admin creating funds).
    loadFunds();
    refreshUnreadCount();
    const unsubscribe = navigation.addListener("focus", () => {
      loadFunds();
      refreshUnreadCount();
    });
    return unsubscribe;
  }, [navigation, loadFunds, refreshUnreadCount]);

  const quickAmounts = [50, 100, 200, 500];

  const onChangeAmount = (raw) => {
    setAmount(normalizeCurrencyInput(raw));
  };

  const isTitheFundSelected = useMemo(() => {
    const code = String(selected?.code || "").toLowerCase();
    const name = String(selected?.name || "").toLowerCase();
    return code.includes("tithe") || name.includes("tithe");
  }, [selected?.code, selected?.name]);

  const titheAmount = useMemo(() => {
    const income = Number.parseFloat(String(titheIncome || ""));
    if (!Number.isFinite(income) || income <= 0) return 0;
    return roundCurrency(income * 0.1);
  }, [titheIncome]);

  const applyTitheAmount = () => {
    if (!titheAmount) return;
    setAmount(String(titheAmount));
    setError("");
  };

  const onContinue = () => {
    const amt = Number.parseFloat(String(amount || ""));
    if (!selected) return setError("Choose a fund");
    if (!Number.isFinite(amt) || amt <= 0) return setError("Enter an amount");
    setError("");
    navigation.navigate("Confirm", { fund: selected, amount: amt });
  };
  const welcomeTitle = useMemo(() => {
    const firstName = firstNameFromFullName(profile?.fullName);
    return firstName ? `Welcome Back ${firstName}` : "Welcome Back";
  }, [profile?.fullName]);

  return (
    <Screen footer={<PrimaryButton label="Continue" onPress={onContinue} disabled={loading || !selected || !Number(amount)} />}>
      <BrandHeader />
      <TopHeroHeader
        tone="member"
        badge="Member Giving"
        title={welcomeTitle}
        subtitle="Choose a fund and set your amount."
        churchName={profile?.churchName}
        actions={[
          {
            label: unreadCount ? `Alerts (${unreadCount})` : "Alerts",
            highlight: !!unreadCount,
            onPress: () => navigation.navigate("Notifications"),
          },
          { label: "History", onPress: () => navigation.navigate("MemberTransactions") },
          { label: "Profile", onPress: () => navigation.navigate("Profile") },
        ]}
      />

      <Card style={{ gap: spacing.md }} padding={spacing.xl}>
        <View style={{ gap: spacing.xs }}>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Choose fund</Text>
          <Text style={{ color: palette.muted }}>Select where this donation should go.</Text>
        </View>
        {loading ? (
          <LoadingCards count={3} />
        ) : funds.length ? (
          <View style={{ gap: spacing.md }}>
            {funds.map((f) => (
              <FundCard key={f.id} fund={f} selected={selected?.id === f.id} onPress={() => setSelected(f)} />
            ))}
          </View>
        ) : (
          <Card style={{ alignItems: "center", gap: spacing.md, borderWidth: 1, borderColor: palette.border, backgroundColor: palette.background }}>
            <Text style={{ fontSize: 28 }}>💒</Text>
            <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700", textAlign: "center" }}>
              No funds available yet
            </Text>
            <Text style={{ color: palette.muted, textAlign: "center", fontSize: typography.body, lineHeight: 24 }}>
              Ask your church admin to create at least one fund, or confirm you’re joined to the right church.
            </Text>
            <View style={{ width: "100%", gap: spacing.xs, maxWidth: 260 }}>
              <PrimaryButton label="Refresh" variant="secondary" onPress={loadFunds} style={{ width: "100%" }} />
              <PrimaryButton
                label="Switch church"
                variant="ghost"
                onPress={() => navigation.navigate("JoinChurch", { mode: "switch" })}
                style={{ width: "100%" }}
              />
            </View>
          </Card>
        )}
      </Card>

      <Card style={{ gap: spacing.md }} padding={spacing.xl}>
        <View style={{ gap: spacing.xs }}>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Amount</Text>
          <Text style={{ color: palette.muted }}>Enter an amount or use a quick amount.</Text>
        </View>
        <TextField label={null} value={amount} onChangeText={onChangeAmount} placeholder="R 200.00" keyboardType="decimal-pad" />
        <View style={{ flexDirection: "row", gap: spacing.xs, flexWrap: "wrap" }}>
          {quickAmounts.map((value) => (
            <QuickAmountChip
              key={value}
              label={`R${value}`}
              active={Number(amount) === value}
              onPress={() => setAmount(String(value))}
            />
          ))}
        </View>
        <View
          style={{
            marginTop: spacing.sm,
            gap: spacing.sm,
            borderWidth: 1,
            borderColor: palette.border,
            borderRadius: radius.md,
            backgroundColor: palette.focus,
            padding: spacing.md,
          }}
        >
          <View style={{ gap: 4 }}>
            <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h3 }}>Tithe calculator</Text>
            <Text style={{ color: palette.muted }}>
              Enter your income and we calculate 10% tithe for you.
            </Text>
            {!selected ? (
              <Text style={{ color: palette.muted, fontSize: typography.small }}>Select a fund, then tap Use amount.</Text>
            ) : !isTitheFundSelected ? (
              <Text style={{ color: palette.muted, fontSize: typography.small }}>
                Tip: calculator is usually used with a Tithes fund.
              </Text>
            ) : null}
          </View>
          <TextField
            label="Income amount"
            value={titheIncome}
            onChangeText={(raw) => setTitheIncome(normalizeCurrencyInput(raw))}
            placeholder="R 5000.00"
            keyboardType="decimal-pad"
          />
          <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
            <Text style={{ color: palette.muted }}>Suggested tithe (10%)</Text>
            <Text style={{ color: palette.text, fontWeight: "800" }}>{money(titheAmount)}</Text>
          </View>
          <PrimaryButton
            label={`Use ${money(titheAmount)} as amount`}
            variant="secondary"
            onPress={applyTitheAmount}
            disabled={!titheAmount}
          />
        </View>
      </Card>

      <ErrorBanner message={error} />
    </Screen>
  );
}

function ConfirmScreen({ navigation, route }) {
  const { spacing, palette, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const [submitting, setSubmitting] = useState(false);
  const [cashSubmitting, setCashSubmitting] = useState(false);
  const [error, setError] = useState("");
  const fund = route.params?.fund;
  const amount = route.params?.amount;
  const pricing = useMemo(() => estimateCheckoutPricing(Number(amount || 0)), [amount]);
  const cashPricing = useMemo(() => estimateCashPricing(Number(amount || 0)), [amount]);
  const [serviceDate, setServiceDate] = useState(nextSundayIso());
  const [notes, setNotes] = useState("");
  const [saveCard, setSaveCard] = useState(false);
  const canSaveCard = Boolean(profile?.email);
  const hasSavedCard = Boolean(profile?.hasSavedCard);

  const createIntent = async ({ useSavedCard = false } = {}) => {
    try {
      setSubmitting(true);
      setError("");
      const res = await createPaymentIntent({
        fundId: fund.id,
        amount: Number(amount),
        saveCard: !useSavedCard && saveCard,
        useSavedCard,
      });
      const checkoutUrl = res?.checkoutUrl || res?.paymentUrl;
      if (checkoutUrl) {
        navigation.navigate("Pending", {
          intent: {
            ...res,
            pricing: res?.pricing || pricing,
          },
        });
        await Linking.openURL(checkoutUrl);
      } else {
        throw new Error("Checkout link missing");
      }
    } catch (e) {
      setError(e?.message || "Could not start payment");
    } finally {
      setSubmitting(false);
    }
  };

  const saveCash = async (flow) => {
    try {
      setCashSubmitting(true);
      setError("");
      const res = await createCashGiving({
        fundId: fund.id,
        amount: Number(amount),
        flow,
        serviceDate,
        notes: notes?.trim() ? notes.trim() : undefined,
      });
      navigation.replace("CashReceipt", { cash: res });
    } catch (e) {
      setError(e?.message || "Could not save cash giving");
    } finally {
      setCashSubmitting(false);
    }
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.md }}>
          {hasSavedCard ? (
            <PrimaryButton
              label={submitting ? "Opening saved card..." : "Pay with saved card"}
              onPress={() => createIntent({ useSavedCard: true })}
              disabled={submitting || cashSubmitting}
            />
          ) : null}
          <PrimaryButton
            label={submitting ? "Opening PayFast..." : saveCard ? "Pay with PayFast (save card)" : "Pay with PayFast"}
            onPress={() => createIntent({ useSavedCard: false })}
            disabled={submitting || cashSubmitting}
          />
          <PrimaryButton
            label="Share a giving link (someone else pays)"
            variant="ghost"
            onPress={() =>
              navigation.navigate("GivingLink", {
                fund,
                amount: Number(amount),
              })
            }
            disabled={submitting || cashSubmitting}
          />
          <PrimaryButton
            label={cashSubmitting ? "Saving cash record..." : "Record as Cash (now)"}
            variant="secondary"
            onPress={() => saveCash("recorded")}
            disabled={submitting || cashSubmitting}
          />
          <PrimaryButton
            label={cashSubmitting ? "Saving..." : "Prepare Cash (for service)"}
            variant="ghost"
            onPress={() => saveCash("prepared")}
            disabled={submitting || cashSubmitting}
          />
          <PrimaryButton label="Back" variant="ghost" onPress={() => navigation.goBack()} />
        </View>
      }
    >
      <BrandHeader />
      <SectionTitle
        title="Confirm payment"
        subtitle="Review your giving details before redirecting to PayFast."
        churchName={profile?.churchName}
      />
      <Card style={{ gap: spacing.md }}>
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.muted }}>Fund</Text>
          <Text style={{ color: palette.text, fontWeight: "700", maxWidth: "65%", textAlign: "right" }}>{fund?.name || "-"}</Text>
        </View>
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.muted }}>Donation</Text>
          <Text style={{ color: palette.text, fontWeight: "700" }}>{money(pricing.donationAmount)}</Text>
        </View>
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.muted }}>Churpay fee</Text>
          <Text style={{ color: palette.text, fontWeight: "700" }}>{money(pricing.churpayFee)}</Text>
        </View>
        <View style={{ height: 1, backgroundColor: palette.border }} />
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.text, fontWeight: "700" }}>Total today</Text>
          <Text style={{ color: palette.text, fontWeight: "800" }}>{money(pricing.totalCharged)}</Text>
        </View>
        <Text style={{ color: palette.muted, fontSize: typography.small }}>You will be redirected securely to PayFast to complete this payment.</Text>
        <View style={{ height: 1, backgroundColor: palette.border }} />
        {canSaveCard ? (
          <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
            <View style={{ flex: 1, paddingRight: spacing.md }}>
              <Text style={{ color: palette.text, fontWeight: "700" }}>Save card for next time</Text>
              <Text style={{ color: palette.muted, fontSize: typography.small }}>
                PayFast will remember your card so you can check out faster next time.
              </Text>
            </View>
            <Switch
              value={!!saveCard}
              onValueChange={setSaveCard}
              disabled={submitting || cashSubmitting}
              trackColor={{ false: "#334155", true: palette.primary }}
              thumbColor={saveCard ? "#ffffff" : "#cbd5e1"}
            />
          </View>
        ) : (
          <Text style={{ color: palette.muted, fontSize: typography.small }}>
            Add an email in Profile to enable saved card checkouts.
          </Text>
        )}
      </Card>

      <Card style={{ gap: spacing.md }}>
        <View style={{ gap: spacing.xs }}>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Cash giving (no in-app payment)</Text>
          <Text style={{ color: palette.muted }}>
            Record your cash giving for receipts and church records. You will not be charged in the app.
          </Text>
        </View>

        <TextField
          label="Service date (YYYY-MM-DD)"
          value={serviceDate}
          onChangeText={setServiceDate}
          placeholder={nextSundayIso()}
          autoCapitalize="none"
        />
        <TextField
          label="Notes (optional)"
          value={notes}
          onChangeText={setNotes}
          placeholder="e.g. Sunday service giving"
        />

        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.muted }}>Cash amount</Text>
          <Text style={{ color: palette.text, fontWeight: "700" }}>{money(cashPricing.donationAmount)}</Text>
        </View>
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.muted }}>Churpay record fee</Text>
          <Text style={{ color: palette.text, fontWeight: "700" }}>{money(cashPricing.churpayFee)}</Text>
        </View>
        <View style={{ height: 1, backgroundColor: palette.border }} />
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.text, fontWeight: "700" }}>Total (cash)</Text>
          <Text style={{ color: palette.text, fontWeight: "800" }}>{money(cashPricing.totalCharged)}</Text>
        </View>
        <Text style={{ color: palette.muted, fontSize: typography.small }}>
          Prepared cash records can be confirmed by your church admin after counting.
        </Text>
      </Card>
      <ErrorBanner message={error} />
    </Screen>
  );
}

function GivingLinkScreen({ navigation, route }) {
  const { spacing, palette, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const fund = route.params?.fund;
  const defaultAmount = Number(route.params?.amount || 0) || 0;

  const [allowOpenAmount, setAllowOpenAmount] = useState(false);
  const [amountFixed, setAmountFixed] = useState(defaultAmount ? String(defaultAmount) : "");
  const [expiresInHours, setExpiresInHours] = useState("48");
  const [maxUses, setMaxUses] = useState("1");
  const [message, setMessage] = useState("");
  const [creating, setCreating] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);

  const pricingPreview = useMemo(() => {
    const amt = Number(amountFixed || 0);
    if (!Number.isFinite(amt) || amt <= 0) return estimateCheckoutPricing(0);
    return estimateCheckoutPricing(amt);
  }, [amountFixed]);

  const shareUrl = result?.shareUrl || "";

  const onCreate = async () => {
    try {
      setCreating(true);
      setError("");
      if (!fund?.id) throw new Error("Choose a fund first");

      const hours = Number(expiresInHours || 48);
      const uses = Number(maxUses || 1);

      const payload = {
        fundId: fund.id,
        amountType: allowOpenAmount ? "OPEN" : "FIXED",
        expiresInHours: Number.isFinite(hours) ? hours : 48,
        maxUses: Number.isFinite(uses) ? uses : 1,
        message: message?.trim() ? message.trim() : undefined,
      };

      if (!allowOpenAmount) {
        const amt = Number(amountFixed);
        if (!Number.isFinite(amt) || amt <= 0) throw new Error("Enter a valid amount");
        payload.amountFixed = amt;
      }

      const res = await createGivingLink(payload);
      const share = res?.data?.shareUrl || "";
      if (!share) throw new Error("Share link missing");
      setResult({
        givingLink: res?.data?.givingLink || null,
        shareUrl: share,
        fund: res?.data?.fund || fund,
      });
    } catch (e) {
      setError(e?.message || "Could not create giving link");
    } finally {
      setCreating(false);
    }
  };

  const copyLink = async () => {
    if (!shareUrl) return;
    await Clipboard.setStringAsync(shareUrl);
    Alert.alert("Copied", "Giving link copied.");
  };

  const shareLink = async () => {
    if (!shareUrl) return;
    const title = `${profile?.churchName || "Churpay"} giving link`;
    const lines = [
      "Someone wants you to give on their behalf.",
      fund?.name ? `Fund: ${fund.name}` : null,
      allowOpenAmount ? "Amount: choose any amount" : `Amount: ${money(Number(amountFixed || 0))}`,
      message?.trim() ? `Message: ${message.trim()}` : null,
      `Link: ${shareUrl}`,
    ].filter(Boolean);
    await Share.share({ title, message: lines.join("\n") });
  };

  const openLink = async () => {
    if (!shareUrl) return;
    await Linking.openURL(shareUrl);
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          {!result ? (
            <PrimaryButton label={creating ? "Creating..." : "Create link"} onPress={onCreate} disabled={creating} />
          ) : (
            <View style={{ gap: spacing.sm }}>
              <PrimaryButton label="Share link" variant="secondary" onPress={shareLink} />
              <PrimaryButton label="Copy link" variant="ghost" onPress={copyLink} />
              <PrimaryButton label="Open link" variant="ghost" onPress={openLink} />
            </View>
          )}
          <PrimaryButton label="Back" variant="ghost" onPress={() => navigation.goBack()} />
        </View>
      }
    >
      <BrandHeader />
      <SectionTitle
        title="Share a giving link"
        subtitle="Let someone give on your behalf. They can open a link or scan a QR code to pay with PayFast."
        churchName={profile?.churchName}
      />

      <Card style={{ gap: spacing.md }}>
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Details</Text>
        <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
          <Text style={{ color: palette.muted }}>Fund</Text>
          <Text style={{ color: palette.text, fontWeight: "700", maxWidth: "65%", textAlign: "right" }}>{fund?.name || "-"}</Text>
        </View>

        <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
          <View style={{ flex: 1, paddingRight: spacing.md }}>
            <Text style={{ color: palette.text, fontWeight: "700" }}>Allow payer to choose amount</Text>
            <Text style={{ color: palette.muted, fontSize: typography.small }}>If enabled, the payer can enter any amount.</Text>
          </View>
          <Switch value={allowOpenAmount} onValueChange={setAllowOpenAmount} />
        </View>

        {!allowOpenAmount ? (
          <>
            <TextField label="Amount (fixed)" value={amountFixed} onChangeText={setAmountFixed} keyboardType="decimal-pad" placeholder="100" />
            <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
              <Text style={{ color: palette.muted }}>Churpay fee</Text>
              <Text style={{ color: palette.text, fontWeight: "700" }}>{money(pricingPreview.churpayFee)}</Text>
            </View>
            <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
              <Text style={{ color: palette.muted }}>Total charged</Text>
              <Text style={{ color: palette.text, fontWeight: "800" }}>{money(pricingPreview.totalCharged)}</Text>
            </View>
          </>
        ) : null}

        <TextField label="Message (optional)" value={message} onChangeText={setMessage} placeholder="e.g. Please give on my behalf" />

        <View style={{ flexDirection: "row", gap: spacing.sm }}>
          <View style={{ flex: 1 }}>
            <TextField label="Expires (hours)" value={expiresInHours} onChangeText={setExpiresInHours} keyboardType="number-pad" placeholder="48" />
          </View>
          <View style={{ flex: 1 }}>
            <TextField label="Max uses" value={maxUses} onChangeText={setMaxUses} keyboardType="number-pad" placeholder="1" />
          </View>
        </View>

        <Text style={{ color: palette.muted, fontSize: typography.small }}>
          This creates a secure link that expires automatically. It can be used up to the max uses you set.
        </Text>
      </Card>

      {result ? (
        <Card style={{ alignItems: "center", gap: spacing.md }}>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Scan QR to pay</Text>
          <Text style={{ color: palette.muted, textAlign: "center" }}>
            The payer can scan this QR code to open the link and pay with PayFast. No login required.
          </Text>
          <View style={{ padding: spacing.md, backgroundColor: "#fff", borderRadius: 16 }}>
            <QRCode value={shareUrl} size={200} />
          </View>
          <Text style={{ color: palette.muted, fontSize: typography.small }} numberOfLines={2}>
            {shareUrl}
          </Text>
        </Card>
      ) : null}

      <ErrorBanner message={error} />
    </Screen>
  );
}

function PendingScreen({ navigation, route }) {
  const { spacing, palette, radius, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const intent = route.params?.intent || {};
  const paymentIntentId = intent?.paymentIntentId || intent?.id || null;
  const fallbackRef = intent?.mPaymentId || intent?.m_payment_id || null;
  const [status, setStatus] = useState("PENDING");
  const [checking, setChecking] = useState(false);
  const [error, setError] = useState("");

  const checkStatus = useCallback(async () => {
    if (!paymentIntentId) return;
    try {
      setChecking(true);
      const latest = await getPaymentIntent(paymentIntentId);
      const nextStatus = String(latest?.status || "PENDING").toUpperCase();
      setStatus(nextStatus);

      if (nextStatus === "PAID") {
        navigation.replace("Success", {
          intent: {
            ...intent,
            ...latest,
            paymentIntentId,
            mPaymentId: latest?.m_payment_id || fallbackRef,
          },
        });
        return;
      }

      if (nextStatus === "FAILED" || nextStatus === "CANCELLED") {
        setError(`Payment ${nextStatus.toLowerCase()}. You can try again.`);
      } else {
        setError("");
      }
    } catch (e) {
      setError(e?.message || "Could not verify payment status");
    } finally {
      setChecking(false);
    }
  }, [fallbackRef, intent, navigation, paymentIntentId]);

  useEffect(() => {
    if (!paymentIntentId) return;
    checkStatus();
    const timer = setInterval(checkStatus, 7000);
    return () => clearInterval(timer);
  }, [checkStatus, paymentIntentId]);

  const onCopyReference = async () => {
    if (!fallbackRef) return;
    await Clipboard.setStringAsync(fallbackRef);
    Alert.alert("Copied", "Payment reference copied.");
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={checking ? "Checking..." : "Refresh status"} onPress={checkStatus} disabled={checking || !paymentIntentId} />
          <PrimaryButton label="Back to start" variant="ghost" onPress={() => navigation.popToTop()} />
        </View>
      }
    >
      <BrandHeader />
      <SectionTitle
        title="Payment pending"
        subtitle="Waiting for PayFast confirmation. Keep this screen open while payment completes."
        churchName={profile?.churchName}
      />
      <Card style={{ alignItems: "center", gap: spacing.md }}>
        <View
          style={{
            width: 104,
            height: 104,
            borderRadius: 52,
            alignItems: "center",
            justifyContent: "center",
            backgroundColor: palette.focus,
            borderWidth: 1,
            borderColor: palette.border,
          }}
        >
          <Text style={{ color: palette.primary, fontSize: 44 }}>⏳</Text>
        </View>
        <StatusChip label={status} active={status === "PAID" || status === "PENDING"} />
        <Text style={{ color: palette.muted, textAlign: "center" }}>
          {status === "PENDING" ? "Waiting for PayFast confirmation" : `Payment ${status.toLowerCase()}`}
        </Text>
        {fallbackRef ? (
          <View
            style={{
              width: "100%",
              paddingHorizontal: spacing.lg,
              paddingVertical: spacing.sm,
              borderRadius: radius.md,
              backgroundColor: palette.focus,
              borderWidth: 1,
              borderColor: palette.border,
              gap: spacing.sm,
            }}
          >
            <Text style={{ color: palette.muted, fontSize: typography.small }}>Reference</Text>
            <Text style={{ color: palette.text, fontWeight: "700" }}>{fallbackRef}</Text>
            <PrimaryButton label="Copy reference" variant="ghost" onPress={onCopyReference} />
          </View>
        ) : null}
      </Card>
      <ErrorBanner message={error} />
    </Screen>
  );
}

function SuccessScreen({ navigation, route }) {
  const { spacing, palette, radius, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const intent = route.params?.intent || {};
  const isPaid = String(intent?.status || "").toUpperCase() === "PAID";
  const paymentRef = intent?.mPaymentId || intent?.m_payment_id || null;
  const onShareReceipt = async () => {
    const message = [
      "Churpay receipt",
      `Amount: ${money(intent?.amount || 0)}`,
      intent?.fundName ? `Fund: ${intent.fundName}` : null,
      paymentRef ? `Reference: ${paymentRef}` : null,
      profile?.churchName ? `Church: ${profile.churchName}` : null,
    ]
      .filter(Boolean)
      .join("\n");
    await Share.share({ title: "Churpay receipt", message });
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label="Back home" onPress={() => navigation.popToTop()} />
          <PrimaryButton
            label="View transactions"
            variant="ghost"
            onPress={() => {
              if (isAdminRole(profile?.role)) {
                navigation.navigate("AdminTransactions");
              } else {
                navigation.navigate("MemberTransactions");
              }
            }}
          />
          <PrimaryButton label="Share receipt" variant="secondary" onPress={onShareReceipt} />
        </View>
      }
    >
      <BrandHeader />
      <SectionTitle title="Thank you for giving" subtitle="Your generosity makes a difference." churchName={profile?.churchName} />
      <Card style={{ alignItems: "center", gap: spacing.md }}>
        <View
          style={{
            width: 104,
            height: 104,
            borderRadius: 52,
            alignItems: "center",
            justifyContent: "center",
            backgroundColor: palette.focus,
            borderWidth: 1,
            borderColor: palette.border,
          }}
        >
          <Text style={{ color: palette.primary, fontSize: 48 }}>❤</Text>
        </View>
        <Body muted>{isPaid ? "Payment confirmed" : "Payment created"}</Body>
        {paymentRef ? (
          <View
            style={{
              paddingHorizontal: spacing.lg,
              paddingVertical: spacing.sm,
              borderRadius: radius.pill,
              backgroundColor: palette.focus,
            }}
          >
            <Text style={{ color: palette.text }}>Ref: {paymentRef}</Text>
          </View>
        ) : null}
      </Card>
    </Screen>
  );
}

function CashReceiptScreen({ navigation, route }) {
  const { spacing, palette, radius, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const cash = route.params?.cash || {};
  const status = String(cash?.status || "").toUpperCase() || "RECORDED";
  const reference = cash?.reference || null;
  const fundName = cash?.fund?.name || cash?.fundName || "-";
  const serviceDate = cash?.serviceDate || null;

  const statusLabel =
    status === "PREPARED"
      ? "Cash (Prepared)"
      : status === "CONFIRMED"
        ? "Cash (Confirmed)"
        : status === "REJECTED"
          ? "Cash (Rejected)"
          : "Cash (Recorded)";

  const onShareReceipt = async () => {
    const message = [
      "Churpay receipt (Cash)",
      `Amount: ${money(cash?.amount || 0)}`,
      `Status: ${statusLabel}`,
      serviceDate ? `Service date: ${serviceDate}` : null,
      fundName ? `Fund: ${fundName}` : null,
      reference ? `Reference: ${reference}` : null,
      profile?.churchName ? `Church: ${profile.churchName}` : null,
    ]
      .filter(Boolean)
      .join("\n");
    await Share.share({ title: "Churpay receipt", message });
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label="Back home" onPress={() => navigation.popToTop()} />
          <PrimaryButton label="View transactions" variant="ghost" onPress={() => navigation.navigate("MemberTransactions")} />
          <PrimaryButton label="Share receipt" variant="secondary" onPress={onShareReceipt} />
        </View>
      }
    >
      <BrandHeader />
      <SectionTitle
        title="Cash giving saved"
        subtitle="Recorded for receipts and church records."
        churchName={profile?.churchName}
      />
      <Card style={{ alignItems: "center", gap: spacing.md }}>
        <View
          style={{
            width: 104,
            height: 104,
            borderRadius: 52,
            alignItems: "center",
            justifyContent: "center",
            backgroundColor: palette.focus,
            borderWidth: 1,
            borderColor: palette.border,
          }}
        >
          <Text style={{ color: palette.primary, fontSize: 46 }}>💵</Text>
        </View>
        <StatusChip label={statusLabel} active={status !== "REJECTED"} />
        <Body muted>
          {status === "PREPARED"
            ? "This is a prepared record. Your church admin can confirm it after counting."
            : status === "REJECTED"
              ? "This cash record was rejected by an admin."
              : "Cash giving recorded."}
        </Body>

        <View
          style={{
            width: "100%",
            paddingHorizontal: spacing.lg,
            paddingVertical: spacing.sm,
            borderRadius: radius.md,
            backgroundColor: palette.focus,
            borderWidth: 1,
            borderColor: palette.border,
            gap: spacing.sm,
          }}
        >
          <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
            <Text style={{ color: palette.muted, fontSize: typography.small }}>Fund</Text>
            <Text style={{ color: palette.text, fontWeight: "700", maxWidth: "65%", textAlign: "right" }}>{fundName}</Text>
          </View>
          {serviceDate ? (
            <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
              <Text style={{ color: palette.muted, fontSize: typography.small }}>Service date</Text>
              <Text style={{ color: palette.text, fontWeight: "700" }}>{serviceDate}</Text>
            </View>
          ) : null}
          <View style={{ flexDirection: "row", justifyContent: "space-between" }}>
            <Text style={{ color: palette.muted, fontSize: typography.small }}>Amount</Text>
            <Text style={{ color: palette.text, fontWeight: "800" }}>{money(cash?.amount || 0)}</Text>
          </View>
          {reference ? (
            <View style={{ gap: spacing.xs }}>
              <Text style={{ color: palette.muted, fontSize: typography.small }}>Reference</Text>
              <Text style={{ color: palette.text, fontWeight: "700" }}>{reference}</Text>
            </View>
          ) : null}
        </View>
      </Card>
    </Screen>
  );
}

function MemberTransactionsScreen({ navigation }) {
  const { spacing, palette, typography, radius } = useTheme();
  const { profile, token, logout } = useContext(AuthContext);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [txns, setTxns] = useState([]);

  const load = useCallback(async () => {
    if (!token) {
      navigation.replace("Welcome");
      return;
    }
    if (!profile?.churchId) {
      navigation.replace("JoinChurch");
      return;
    }
    try {
      setLoading(true);
      setError("");
      const res = await listTransactions({ limit: 50 });
      setTxns(res?.transactions || []);
    } catch (e) {
      const message = e?.message || "Could not load transactions";
      if (String(message).toLowerCase().includes("unauthorized")) {
        await logout();
        navigation.replace("Login");
        return;
      }
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [logout, navigation, profile?.churchId, token]);

  useEffect(() => {
    load();
  }, [load]);

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={loading ? "Refreshing..." : "Refresh"} onPress={load} disabled={loading} />
          <PrimaryButton label="Back home" variant="ghost" onPress={() => navigation.popToTop()} />
        </View>
      }
    >
      <BrandHeader />
      <SectionTitle title="Your giving history" subtitle="PayFast and Cash records for receipts and church records." churchName={profile?.churchName} />
      {loading ? <LoadingCards count={3} /> : null}
      {!loading && txns.length ? (
        <View style={{ gap: spacing.sm }}>
          {txns.map((t) => {
            const status = String(t.status || "").toUpperCase();
            const provider = String(t.provider || "").toLowerCase();
            const isCash = provider === "cash";
            const persona = resolveTransactionPersona(t);
            const badge = isCash
              ? status === "PREPARED"
                ? "Cash (Prepared)"
                : status === "CONFIRMED"
                  ? "Cash (Confirmed)"
                  : status === "REJECTED"
                    ? "Cash (Rejected)"
                    : "Cash (Recorded)"
              : "PayFast";
            const when = t.serviceDate || t.createdAt;

            return (
              <Card key={t.id} style={{ gap: spacing.xs }}>
                <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
                  <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>{t.fundName || t.fundCode || "Fund"}</Text>
                  <View style={{ paddingHorizontal: spacing.sm, paddingVertical: 6, borderRadius: radius.pill, backgroundColor: palette.focus, borderWidth: 1, borderColor: palette.border }}>
                    <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.small }}>{badge}</Text>
                  </View>
                </View>
                <Text style={{ color: palette.muted, fontSize: typography.small }}>
                  {when ? String(when).slice(0, 10) : "-"} • Ref: {t.reference}
                </Text>
                {persona.onBehalf ? (
                  <Text style={{ color: palette.muted, fontSize: typography.small }}>
                    Paid for {persona.beneficiaryLabel} • Payer: {persona.payerLabel}
                  </Text>
                ) : persona.visitor ? (
                  <Text style={{ color: palette.muted, fontSize: typography.small }}>Visitor: {persona.payerLabel}</Text>
                ) : null}
                <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
                  <StatusChip label={status || "UNKNOWN"} active={status === "PAID" || status === "RECORDED" || status === "CONFIRMED" || status === "PREPARED"} />
                  <Text style={{ color: palette.text, fontWeight: "800" }}>{money(t.amount || 0)}</Text>
                </View>
              </Card>
            );
          })}
        </View>
      ) : null}
      {!loading && !txns.length ? (
        <EmptyStateCard icon="📜" title="No records yet" subtitle="Your PayFast and cash records will appear here." actionLabel="Refresh" onAction={load} />
      ) : null}
      <ErrorBanner message={error} />
    </Screen>
  );
}

function NotificationsScreen({ navigation }) {
  const { spacing, palette, typography, radius } = useTheme();
  const { token, logout, notificationsEnabled, notificationsSettingsLoading } = useContext(AuthContext);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [items, setItems] = useState([]);
  const [unreadCount, setUnreadCount] = useState(0);

  const load = useCallback(async () => {
    if (notificationsSettingsLoading) {
      setLoading(true);
      return;
    }
    if (!token) {
      navigation.replace("Welcome");
      return;
    }
    if (!notificationsEnabled) {
      setError("");
      setUnreadCount(0);
      setItems([]);
      setLoading(false);
      return;
    }
    try {
      setLoading(true);
      setError("");
      const [countRes, listRes] = await Promise.all([
        safe(getUnreadNotificationCount()),
        listNotifications({ limit: 50 }),
      ]);
      setUnreadCount(Number(countRes?.count || 0));
      setItems(listRes?.notifications || []);
    } catch (e) {
      const message = e?.message || "Could not load alerts";
      if (String(message).toLowerCase().includes("unauthorized")) {
        await logout();
        navigation.replace("Login");
        return;
      }
      setError(message);
    } finally {
      setLoading(false);
    }
  }, [logout, navigation, notificationsEnabled, notificationsSettingsLoading, token]);

  useEffect(() => {
    load();
    const unsub = navigation.addListener("focus", load);
    return unsub;
  }, [load, navigation]);

  const onMarkAllRead = async () => {
    try {
      setError("");
      await markAllNotificationsRead();
      await load();
    } catch (e) {
      setError(e?.message || "Could not mark all as read");
    }
  };

  const openNotification = async (n) => {
    try {
      setError("");
      if (n?.id && !n?.readAt) {
        await markNotificationRead(n.id);
        setItems((prev) =>
          prev.map((it) => (it.id === n.id ? { ...it, readAt: new Date().toISOString() } : it))
        );
        setUnreadCount((c) => Math.max(0, Number(c || 0) - 1));
      }
      const title = n?.title || "Alert";
      const body = n?.body || "";
      Alert.alert(title, body || " ");
    } catch (e) {
      setError(e?.message || "Could not open alert");
    }
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          {!notificationsEnabled ? (
            <PrimaryButton label="Notification settings" onPress={() => navigation.navigate("Profile")} variant="secondary" />
          ) : (
            <>
              <PrimaryButton label={loading ? "Refreshing..." : "Refresh"} onPress={load} disabled={loading} variant="secondary" />
              <PrimaryButton
                label={unreadCount ? `Mark all read (${unreadCount})` : "Mark all read"}
                onPress={onMarkAllRead}
                disabled={loading || !unreadCount}
                variant="secondary"
              />
            </>
          )}
          <PrimaryButton label="Back home" variant="ghost" onPress={() => navigation.popToTop()} />
        </View>
      }
    >
      <BrandHeader />
      <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center", gap: spacing.md }}>
        <View style={{ flex: 1 }}>
          <SectionTitle title="Alerts" subtitle="Updates about your giving and account." />
        </View>
        {unreadCount ? <StatusChip label={`${unreadCount} new`} active /> : null}
      </View>

      {loading ? <LoadingCards count={3} /> : null}

      {!loading && items?.length ? (
        <View style={{ gap: spacing.sm }}>
          {items.map((n) => {
            const isUnread = !n.readAt;
            const when = n.createdAt ? new Date(n.createdAt).toLocaleString() : "";
            return (
              <Pressable key={n.id} onPress={() => openNotification(n)} style={{ opacity: 0.98 }}>
                <Card
                  style={{
                    gap: spacing.xs,
                    borderColor: isUnread ? palette.primary : palette.border,
                    borderWidth: 1,
                    backgroundColor: isUnread ? palette.focus : palette.card,
                  }}
                >
                  <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "flex-start", gap: spacing.sm }}>
                    <View style={{ flex: 1 }}>
                      <Text style={{ color: palette.text, fontWeight: "800", fontSize: typography.h2 }}>{n.title}</Text>
                      {when ? <Text style={{ color: palette.muted, fontSize: typography.small, marginTop: 2 }}>{when}</Text> : null}
                    </View>
                    {isUnread ? (
                      <View style={{ paddingHorizontal: spacing.sm, paddingVertical: 6, borderRadius: radius.pill, backgroundColor: palette.primary }}>
                        <Text style={{ color: palette.onPrimary, fontWeight: "800", fontSize: typography.small }}>NEW</Text>
                      </View>
                    ) : null}
                  </View>
                  {n.body ? <Text style={{ color: palette.muted, fontSize: typography.body, lineHeight: 20 }}>{n.body}</Text> : null}
                </Card>
              </Pressable>
            );
          })}
        </View>
      ) : null}

      {!loading && !notificationsEnabled ? (
        <EmptyStateCard
          icon="🔕"
          title="Alerts are turned off"
          subtitle="Enable notifications in Profile when you want updates about giving and account activity."
          actionLabel="Open settings"
          onAction={() => navigation.navigate("Profile")}
        />
      ) : null}

      {!loading && notificationsEnabled && !items?.length ? <EmptyStateCard icon="🔔" title="No alerts yet" subtitle="When something happens, you’ll see it here." actionLabel="Refresh" onAction={load} /> : null}

      <ErrorBanner message={error} />
    </Screen>
  );
}

function ProfileScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const {
    profile,
    refreshProfile,
    setProfile,
    logout,
    notificationsEnabled,
    notificationsSettingsLoading,
    updateNotificationsEnabled,
  } = useContext(AuthContext);
  const [fullName, setFullName] = useState(profile?.fullName || "");
  const [phone, setPhone] = useState(profile?.phone || "");
  const [email, setEmail] = useState(profile?.email || "");
  const [dateOfBirth, setDateOfBirth] = useState(profile?.dateOfBirth || "");
  const [biometricSupported, setBiometricSupported] = useState(false);
  const [biometricEnabled, setBiometricEnabledState] = useState(false);
  const [biometricLoading, setBiometricLoading] = useState(true);
  const [notificationsSaving, setNotificationsSaving] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const isAdmin = isAdminRole(profile?.role);

  useEffect(() => {
    if (!profile) navigation.replace("Welcome");
  }, [navigation, profile]);

  useEffect(() => {
    setFullName(profile?.fullName || "");
    setPhone(profile?.phone || "");
    setEmail(profile?.email || "");
    setDateOfBirth(profile?.dateOfBirth || "");
  }, [profile]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setBiometricLoading(true);
        const capability = await canUseBiometrics();
        const enabled = await getBiometricEnabled().catch(() => false);
        if (!cancelled) {
          setBiometricSupported(!!capability?.ok);
          setBiometricEnabledState(!!enabled);
        }
      } finally {
        if (!cancelled) setBiometricLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  const onToggleBiometrics = async (nextValue) => {
    try {
      setError("");
      setBiometricLoading(true);
      const next = !!nextValue;
      const updated = await setBiometricEnabled(next);
      setBiometricEnabledState(!!updated);
      Alert.alert(
        "Biometric unlock",
        next
          ? "Enabled. Next time you open Churpay, you'll be prompted to unlock your session."
          : "Disabled. You can sign in with your password as normal."
      );
    } catch (e) {
      setBiometricEnabledState(false);
      setError(e?.message || "Could not update biometric settings");
    } finally {
      setBiometricLoading(false);
    }
  };

  const onToggleNotifications = async (nextValue) => {
    try {
      setError("");
      setNotificationsSaving(true);
      const next = !!nextValue;
      await updateNotificationsEnabled(next);
      Alert.alert(
        "Notifications",
        next
          ? "Alerts enabled. You’ll receive in-app updates and push notifications."
          : "Alerts disabled. You can re-enable them anytime from Profile."
      );
    } catch (e) {
      setError(e?.message || "Could not update notification settings");
    } finally {
      setNotificationsSaving(false);
    }
  };

  const onSave = async () => {
    try {
      setLoading(true);
      setError("");
      const updates = {};

      const nextFullName = String(fullName || "").trim();
      const nextPhone = String(phone || "").trim();
      const nextEmail = String(email || "").trim();

      if (nextFullName !== String(profile?.fullName || "").trim()) updates.fullName = nextFullName;
      if (nextPhone !== String(profile?.phone || "").trim()) updates.phone = nextPhone;
      if (nextEmail !== String(profile?.email || "").trim()) updates.email = nextEmail;

      const dobRaw = String(dateOfBirth || "").trim();
      const currentDob = String(profile?.dateOfBirth || "").trim();
      const currentDobNormalized = currentDob ? normalizeBirthDateInput(currentDob) : null;
      if (!dobRaw && currentDob) {
        updates.dateOfBirth = null; // allow clearing
      } else if (dobRaw) {
        const normalizedDob = normalizeBirthDateInput(dobRaw);
        if (!normalizedDob) {
          throw new Error("Date of birth must be YYYY-MM-DD (or DD-MM-YYYY) and cannot be in the future.");
        }
        if (normalizedDob !== currentDobNormalized) updates.dateOfBirth = normalizedDob;
      }

      if (!Object.keys(updates).length) {
        Alert.alert("No changes", "Nothing to update.");
        return;
      }

      const res = await updateProfile(updates);
      if (res?.member) setProfile(res.member);
      await refreshProfile();
      Alert.alert("Profile updated");
    } catch (e) {
      setError(e?.message || "Could not update profile");
    } finally {
      setLoading(false);
    }
  };

  const onLogout = async () => {
    await logout();
    navigation.reset({ index: 0, routes: [{ name: "Welcome" }] });
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={loading ? "Saving..." : "Save changes"} onPress={onSave} disabled={loading} />
          {isAdmin ? (
            <View style={{ gap: spacing.xs }}>
              <PrimaryButton label="Church settings" variant="secondary" onPress={() => navigation.navigate("AdminChurch")} />
            </View>
          ) : null}
          <PrimaryButton label="Log out" variant="ghost" onPress={onLogout} />
        </View>
      }
    >
      <BrandHeader />
      <TopHeroHeader
        tone={isAdmin ? "admin" : "member"}
        badge={isAdmin ? "Admin Workspace" : "Member Account"}
        title="Your profile"
        subtitle="Manage your details and church."
        churchName={profile?.churchName}
        actions={
          isAdmin
            ? [
                { label: "Church settings", onPress: () => navigation.navigate("AdminChurch") },
                { label: "Transactions", onPress: () => navigation.navigate("AdminTransactions") },
              ]
            : [
                { label: "History", onPress: () => navigation.navigate("MemberTransactions") },
                {
                  label: profile?.churchId ? "Switch church" : "Join church",
                  onPress: () => navigation.navigate("JoinChurch", { mode: profile?.churchId ? "switch" : "join" }),
                },
              ]
        }
      />
      {isAdmin ? <AdminTabBar navigation={navigation} activeTab="profile" /> : null}
      <Card style={{ gap: spacing.md }}>
        <TextField label="Full name" value={fullName} onChangeText={setFullName} />
        <TextField
          label="Date of birth"
          value={dateOfBirth}
          onChangeText={setDateOfBirth}
          placeholder="YYYY-MM-DD (or DD-MM-YYYY)"
        />
        <Body muted>We use this to wish you a happy birthday message. Leave blank if you prefer.</Body>
        <TextField label="Mobile number" value={phone} onChangeText={setPhone} keyboardType="phone-pad" />
        <TextField label="Email" value={email} onChangeText={setEmail} keyboardType="email-address" />
      </Card>
      <Card style={{ gap: spacing.sm, marginTop: spacing.md }}>
        <Body muted>Notifications</Body>
        <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
          <View style={{ flex: 1, paddingRight: spacing.md }}>
            <Body>Alerts & push notifications</Body>
            <Body muted>Turn updates on or off for giving and account activity.</Body>
          </View>
          <Switch
            value={!!notificationsEnabled}
            disabled={notificationsSaving || notificationsSettingsLoading}
            onValueChange={onToggleNotifications}
            trackColor={{ false: "#334155", true: palette.primary }}
            thumbColor={notificationsEnabled ? "#ffffff" : "#cbd5e1"}
          />
        </View>
        <Body muted>
          {notificationsSettingsLoading
            ? "Checking your notification setting..."
            : notificationsEnabled
              ? "Notifications are enabled."
              : "Notifications are disabled."}
        </Body>
      </Card>
      <Card style={{ gap: spacing.sm, marginTop: spacing.md }}>
        <Body muted>Security</Body>
        {biometricSupported ? (
          <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
            <View style={{ flex: 1, paddingRight: spacing.md }}>
              <Body>Biometric unlock</Body>
              <Body muted>Use Face ID / Touch ID / fingerprint to unlock your saved session.</Body>
            </View>
            <Switch
              value={biometricEnabled}
              disabled={biometricLoading}
              onValueChange={onToggleBiometrics}
              trackColor={{ false: "#334155", true: palette.primary }}
              thumbColor={biometricEnabled ? "#ffffff" : "#cbd5e1"}
            />
          </View>
        ) : (
          <Body muted>{biometricLoading ? "Checking device biometrics..." : "Biometric unlock is not available on this device."}</Body>
        )}
      </Card>
      <Card style={{ gap: spacing.sm, marginTop: spacing.md }}>
        <Body muted>Church</Body>
        <Body>{profile?.churchName || "Not joined"}</Body>
        {isAdmin ? (
          <PrimaryButton label="Church settings" variant="ghost" onPress={() => navigation.navigate("AdminChurch")} />
        ) : (
          <PrimaryButton
            label={profile?.churchId ? "Switch church" : "Join a church"}
            variant="ghost"
            onPress={() => navigation.navigate("JoinChurch", { mode: profile?.churchId ? "switch" : "join" })}
          />
        )}
      </Card>
      <Card style={{ gap: spacing.sm, marginTop: spacing.md }}>
        <Body muted>App build</Body>
        <Body muted>
          {`Channel: ${Updates.channel || "-"}`}{`\n`}
          {`Runtime: ${Updates.runtimeVersion || "-"}`}{`\n`}
          {`Update ID: ${Updates.updateId || "-"}`}{`\n`}
          {`Embedded: ${Updates.isEmbeddedLaunch ? "yes" : "no"}`}
        </Body>
      </Card>
      <ErrorBanner message={error} />
    </Screen>
  );
}

function AdminChurchScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile, setProfile, refreshProfile } = useContext(AuthContext);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [churchExists, setChurchExists] = useState(false);
  const [name, setName] = useState("");
  const [joinCode, setJoinCode] = useState("");
  const [error, setError] = useState("");
  const [unreadCount, setUnreadCount] = useState(0);

  const isAdmin = isAdminRole(profile?.role);

  const refreshUnreadCount = useCallback(async () => {
    try {
      const res = await getUnreadNotificationCount();
      setUnreadCount(Number(res?.count || 0));
    } catch (_err) {
      // non-fatal
    }
  }, []);

  const loadChurch = useCallback(async () => {
    try {
      setLoading(true);
      setError("");
      const res = await getMyChurchProfile();
      const church = res?.church || null;
      if (!church) {
        setChurchExists(false);
        setName("");
        setJoinCode("");
        return;
      }
      setChurchExists(true);
      setName(church.name || "");
      setJoinCode(church.joinCode || "");
    } catch (e) {
      if (String(e?.message || "").includes("No church assigned") || String(e?.message || "").includes("HTTP 404")) {
        setChurchExists(false);
        setName("");
        setJoinCode("");
      } else {
        setError(e?.message || "Could not load church profile");
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (!profile) {
      navigation.replace("Welcome");
      return;
    }
    if (!isAdmin) {
      navigation.replace("Give");
      return;
    }
    loadChurch();
    refreshUnreadCount();
    const unsubscribe = navigation.addListener("focus", () => {
      loadChurch();
      refreshUnreadCount();
    });
    return unsubscribe;
  }, [isAdmin, loadChurch, navigation, profile, refreshUnreadCount]);

  const onSave = async () => {
    try {
      setSaving(true);
      setError("");
      const payload = { name: String(name || "").trim() };
      const normalizedJoinCode = String(joinCode || "").trim().toUpperCase();
      if (!payload.name) throw new Error("Church name is required");
      if (normalizedJoinCode) payload.joinCode = normalizedJoinCode;

      const res = churchExists ? await updateMyChurchProfile(payload) : await createMyChurchProfile(payload);
      if (res?.member) setProfile(res.member);
      await refreshProfile();
      await loadChurch();
      Alert.alert(churchExists ? "Church updated" : "Church created");
    } catch (e) {
      setError(e?.message || "Could not save church profile");
    } finally {
      setSaving(false);
    }
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={saving ? "Saving..." : churchExists ? "Save church profile" : "Create church"} onPress={onSave} disabled={saving} />
          <PrimaryButton label="Back" variant="ghost" onPress={() => navigation.goBack()} />
        </View>
      }
    >
      <BrandHeader />
      <TopHeroHeader
        tone="admin"
        badge="Admin Workspace"
        title="Church profile"
        subtitle="Set your church name. Join code can be auto-generated from your church name."
        churchName={profile?.churchName}
        actions={[
          {
            label: unreadCount ? `Alerts (${unreadCount})` : "Alerts",
            highlight: !!unreadCount,
            onPress: () => navigation.navigate("Notifications"),
          },
          { label: "Funds", onPress: () => navigation.replace("AdminFunds") },
          { label: "Transactions", onPress: () => navigation.replace("AdminTransactions") },
        ]}
      />
      {loading ? (
        <LoadingCards count={2} />
      ) : (
        <Card style={{ gap: spacing.md }}>
          <TextField label="Church name" value={name} onChangeText={setName} placeholder="Great Commission Church of Christ" />
          <TextField label="Join code (optional)" value={joinCode} onChangeText={setJoinCode} placeholder="Auto from name (e.g. GCCOC-1234)" autoCapitalize="characters" />
          <Body muted>Members join with this code. Leave blank to auto-generate from church abbreviation.</Body>
        </Card>
      )}
      <ErrorBanner message={error} />
    </Screen>
  );
}

function AdminFundsScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const [funds, setFunds] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [unreadCount, setUnreadCount] = useState(0);
  const [name, setName] = useState("");
  const [code, setCode] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [creating, setCreating] = useState(false);
  const [summary, setSummary] = useState({ activeCount: 0, lastDonationAt: null });

  const isAdmin = isAdminRole(profile?.role);

  const refreshUnreadCount = useCallback(async () => {
    try {
      const res = await getUnreadNotificationCount();
      setUnreadCount(Number(res?.count || 0));
    } catch (_err) {
      // non-fatal
    }
  }, []);

  const loadFunds = useCallback(async () => {
    if (!isAdmin) return;
    try {
      setLoading(true);
      setError("");
      const [res, recentRes] = await Promise.all([listFunds(true), getAdminRecentTransactions({ limit: 1 })]);
      const rows = res?.funds || [];
      setFunds(rows);
      setSummary({
        activeCount: rows.filter((fund) => fund.active).length,
        lastDonationAt: recentRes?.transactions?.[0]?.createdAt || null,
      });
    } catch (e) {
      setError(e?.message || "Could not load funds");
    } finally {
      setLoading(false);
    }
  }, [isAdmin]);

  const onRefresh = useCallback(async () => {
    setRefreshing(true);
    await loadFunds();
    setRefreshing(false);
  }, [loadFunds]);

  useEffect(() => {
    if (!profile) {
      navigation.replace("Welcome");
      return;
    }
    if (!isAdmin) {
      navigation.replace("Give");
      return;
    }
    loadFunds();
    refreshUnreadCount();
    const unsubscribe = navigation.addListener("focus", () => {
      loadFunds();
      refreshUnreadCount();
    });
    return unsubscribe;
  }, [isAdmin, loadFunds, navigation, profile, refreshUnreadCount]);

  const onCreate = async () => {
    try {
      setError("");
      setCreating(true);
      await createFund({ name, code: String(code || "").trim().toUpperCase() || undefined });
      setName("");
      setCode("");
      await loadFunds();
    } catch (e) {
      setError(e?.message || "Could not create fund");
    } finally {
      setCreating(false);
    }
  };

  const toggleActive = async (fund) => {
    try {
      await apiUpdateFund({ fundId: fund.id, active: !fund.active, name: fund.name });
      await loadFunds();
    } catch (e) {
      setError(e?.message || "Could not update fund");
    }
  };

  const shareFundQr = async (fund) => {
    const payload = JSON.stringify({
      type: "churpay_donation",
      churchId: profile?.churchId,
      fundId: fund.id,
      fundCode: fund.code,
    });
    await Share.share({
      title: `${fund.name} QR`,
      message: `${fund.name}\n${payload}`,
    });
  };

  const copyFundPayload = async (fund) => {
    const payload = JSON.stringify({
      type: "churpay_donation",
      churchId: profile?.churchId,
      fundId: fund.id,
      fundCode: fund.code,
    });
    await Clipboard.setStringAsync(payload);
    Alert.alert("Copied", "QR payload copied.");
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label="Church settings" variant="ghost" onPress={() => navigation.navigate("AdminChurch")} />
        </View>
      }
    >
      <BrandHeader />
      <TopHeroHeader
        tone="admin"
        badge="Admin Workspace"
        title="Funds"
        subtitle="Create, edit and control giving funds."
        churchName={profile?.churchName}
        actions={[
          {
            label: unreadCount ? `Alerts (${unreadCount})` : "Alerts",
            highlight: !!unreadCount,
            onPress: () => navigation.navigate("Notifications"),
          },
          { label: "Church settings", onPress: () => navigation.navigate("AdminChurch") },
          { label: "Profile", onPress: () => navigation.navigate("Profile") },
        ]}
      />
      <AdminTabBar navigation={navigation} activeTab="funds" />

      <Card style={{ gap: spacing.md }}>
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Funds overview</Text>
        <View style={{ flexDirection: "row", gap: spacing.md }}>
          <View style={{ flex: 1 }}>
            <Text style={{ color: palette.muted, fontSize: typography.small }}>Funds active</Text>
            <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>{summary.activeCount}</Text>
          </View>
          <View style={{ flex: 1 }}>
            <Text style={{ color: palette.muted, fontSize: typography.small }}>Last donation</Text>
            <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.body }}>
              {summary.lastDonationAt ? new Date(summary.lastDonationAt).toLocaleDateString() : "--"}
            </Text>
          </View>
        </View>
      </Card>

      <Card style={{ gap: spacing.md }}>
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Create fund</Text>
        <TextField label="Fund name" value={name} onChangeText={setName} placeholder="Building Project" />
        <TextField label="Code (optional)" value={code} onChangeText={setCode} placeholder="BLDG" autoCapitalize="characters" />
        <PrimaryButton label={creating ? "Creating..." : "Create fund"} onPress={onCreate} disabled={!name || creating} />
      </Card>

      {loading ? (
        <LoadingCards count={3} />
      ) : (
        <ScrollView refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={palette.primary} />} contentContainerStyle={{ gap: spacing.sm, marginTop: spacing.md }}>
          {funds.map((f) => (
            <Card key={f.id} padding={spacing.md} style={{ gap: spacing.md }}>
              <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "flex-start", gap: spacing.sm }}>
                <View>
                  <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700" }}>{f.name}</Text>
                  <View style={{ flexDirection: "row", alignItems: "center", gap: spacing.xs, marginTop: spacing.xs }}>
                    <StatusChip label={String(f.code || "N/A").toUpperCase()} active={!!f.code} />
                    <StatusChip label={f.active ? "Active" : "Inactive"} active={!!f.active} />
                  </View>
                </View>
                <PrimaryButton label={f.active ? "Disable" : "Enable"} variant="ghost" onPress={() => toggleActive(f)} />
              </View>

              <View style={{ alignItems: "center", gap: spacing.sm }}>
                <Text style={{ color: palette.muted }}>Fund QR</Text>
                <QRCode value={JSON.stringify({ fundId: f.id, churchId: profile?.churchId, fundCode: f.code })} size={112} />
                <View style={{ width: "100%", flexDirection: "row", gap: spacing.xs }}>
                  <View style={{ flex: 1 }}>
                    <PrimaryButton label="Share QR" variant="ghost" onPress={() => shareFundQr(f)} />
                  </View>
                  <View style={{ flex: 1 }}>
                    <PrimaryButton label="Copy payload" variant="ghost" onPress={() => copyFundPayload(f)} />
                  </View>
                </View>
              </View>
            </Card>
          ))}
          {funds.length === 0 ? (
            <EmptyStateCard icon="💸" title="No funds yet" subtitle="Create your first fund to start receiving donations." />
          ) : null}
        </ScrollView>
      )}
      <ErrorBanner message={error} />
    </Screen>
  );
}

function AdminQrScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const [funds, setFunds] = useState([]);
  const [selectedFundId, setSelectedFundId] = useState("");
  const [amount, setAmount] = useState("");
  const [qrValue, setQrValue] = useState("");
  const [qrPayload, setQrPayload] = useState(null);
  const [deepLink, setDeepLink] = useState("");
  const [webLink, setWebLink] = useState("");
  const [loading, setLoading] = useState(true);
  const [generating, setGenerating] = useState(false);
  const [error, setError] = useState("");
  const [unreadCount, setUnreadCount] = useState(0);

  const isAdmin = isAdminRole(profile?.role);

  const refreshUnreadCount = useCallback(async () => {
    try {
      const res = await getUnreadNotificationCount();
      setUnreadCount(Number(res?.count || 0));
    } catch (_err) {
      // non-fatal
    }
  }, []);

  const loadFunds = useCallback(async () => {
    if (!isAdmin) return;
    try {
      setLoading(true);
      setError("");
      const res = await listFunds(true);
      const rows = (res?.funds || []).filter((f) => f.active !== false);
      setFunds(rows);
      if (!selectedFundId && rows.length) {
        setSelectedFundId(rows[0].id);
      }
    } catch (e) {
      setError(e?.message || "Could not load funds");
    } finally {
      setLoading(false);
    }
  }, [isAdmin, selectedFundId]);

  useEffect(() => {
    if (!profile) {
      navigation.replace("Welcome");
      return;
    }
    if (!isAdmin) {
      navigation.replace("Give");
      return;
    }
    loadFunds();
    refreshUnreadCount();
    const unsubscribe = navigation.addListener("focus", () => {
      loadFunds();
      refreshUnreadCount();
    });
    return unsubscribe;
  }, [isAdmin, loadFunds, navigation, profile, refreshUnreadCount]);

  const onGenerate = async () => {
    try {
      setGenerating(true);
      setError("");
      if (!selectedFundId) throw new Error("Select a fund");
      const amountValue = amount ? Number(amount) : undefined;
      const res = await getChurchQr({ fundId: selectedFundId, amount: amountValue });
      setQrValue(res?.qr?.value || "");
      setQrPayload(res?.qr?.payload || res?.qrPayload || null);
      setDeepLink(res?.deepLink || "");
      setWebLink(res?.webLink || "");
    } catch (e) {
      setError(e?.message || "Could not generate QR");
    } finally {
      setGenerating(false);
    }
  };

  const selectedFund = funds.find((fund) => fund.id === selectedFundId);

  const copyValue = async (label, value) => {
    if (!value) return;
    await Clipboard.setStringAsync(String(value));
    Alert.alert("Copied", `${label} copied.`);
  };

  const shareLink = async () => {
    const message = webLink || deepLink || qrValue;
    if (!message) return;
    await Share.share({ title: "Churpay QR", message });
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label="Church settings" variant="ghost" onPress={() => navigation.navigate("AdminChurch")} />
        </View>
      }
    >
      <BrandHeader />
      <TopHeroHeader
        tone="admin"
        badge="Admin Workspace"
        title="QR Codes"
        subtitle="Generate QR donation links in three quick steps."
        churchName={profile?.churchName}
        actions={[
          {
            label: unreadCount ? `Alerts (${unreadCount})` : "Alerts",
            highlight: !!unreadCount,
            onPress: () => navigation.navigate("Notifications"),
          },
          { label: "Funds", onPress: () => navigation.replace("AdminFunds") },
          { label: "Church settings", onPress: () => navigation.navigate("AdminChurch") },
        ]}
      />
      <AdminTabBar navigation={navigation} activeTab="qr" />

      {loading ? (
        <LoadingCards count={2} />
      ) : (
        <Card style={{ gap: spacing.md }}>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>1. Choose fund</Text>
          <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: spacing.xs }}>
            {funds.map((f) => (
              <Pressable
                key={f.id}
                onPress={() => setSelectedFundId(f.id)}
                style={{
                  paddingVertical: spacing.xs,
                  paddingHorizontal: spacing.md,
                  borderRadius: 999,
                  backgroundColor: selectedFundId === f.id ? palette.primary : palette.focus,
                }}
              >
                <Text style={{ color: selectedFundId === f.id ? "#fff" : palette.text, fontWeight: "600" }}>
                  {f.name}
                </Text>
              </Pressable>
            ))}
          </ScrollView>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>2. Optional amount</Text>
          <TextField label="Preset amount (optional)" value={amount} onChangeText={setAmount} keyboardType="decimal-pad" placeholder="10.00" />
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>3. Generate</Text>
          <PrimaryButton label={generating ? "Generating..." : "Generate QR"} onPress={onGenerate} disabled={generating || !selectedFundId} />
        </Card>
      )}

      {qrValue ? (
        <Card style={{ alignItems: "center", gap: spacing.md }}>
          <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700" }}>{profile?.churchName || "Church"}</Text>
          <Text style={{ color: palette.muted }}>{selectedFund?.name || "Selected fund"}</Text>
          {amount ? <StatusChip label={`Amount ${money(amount)}`} active /> : null}
          <View style={{ padding: spacing.md, backgroundColor: "#fff", borderRadius: 16 }}>
            <QRCode value={qrValue} size={180} />
          </View>
          <View style={{ width: "100%", gap: spacing.xs }}>
            <PrimaryButton label="Share QR link" variant="secondary" onPress={shareLink} />
            <PrimaryButton label="Copy payload" variant="ghost" onPress={() => copyValue("Payload", JSON.stringify(qrPayload || {}))} />
            <PrimaryButton label="Copy web link" variant="ghost" onPress={() => copyValue("Web link", webLink)} />
          </View>
          {deepLink ? (
            <Text style={{ color: palette.muted, fontSize: typography.small }} numberOfLines={2}>
              {deepLink}
            </Text>
          ) : null}
        </Card>
      ) : null}

      {funds.length === 0 && !loading ? (
        <EmptyStateCard icon="📱" title="No active funds" subtitle="Create and enable at least one fund before generating QR codes." />
      ) : null}
      <ErrorBanner message={error} />
    </Screen>
  );
}

function AdminTransactionsScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const [txns, setTxns] = useState([]);
  const [funds, setFunds] = useState([]);
  const [fundId, setFundId] = useState("");
  const [fromDate, setFromDate] = useState("");
  const [toDate, setToDate] = useState("");
  const [grandTotal, setGrandTotal] = useState("0.00");
  const [loading, setLoading] = useState(true);
  const [exporting, setExporting] = useState(false);
  const [error, setError] = useState("");
  const [unreadCount, setUnreadCount] = useState(0);
  const [refreshing, setRefreshing] = useState(false);
  const [selectedTxn, setSelectedTxn] = useState(null);
  const [cashActionLoading, setCashActionLoading] = useState(false);
  const [rejectCashOpen, setRejectCashOpen] = useState(false);
  const [rejectCashNote, setRejectCashNote] = useState("");

  const isAdmin = isAdminRole(profile?.role);

  const refreshUnreadCount = useCallback(async () => {
    try {
      const res = await getUnreadNotificationCount();
      setUnreadCount(Number(res?.count || 0));
    } catch (_err) {
      // non-fatal
    }
  }, []);

  const loadData = useCallback(async () => {
    if (!isAdmin) return;
    try {
      setLoading(true);
      setError("");
      const [fundsRes, totalsRes, recentRes] = await Promise.all([
        listFunds(true),
        getAdminDashboardTotals(),
        getAdminRecentTransactions({
          limit: 50,
          fundId: fundId || undefined,
          from: fromDate || undefined,
          to: toDate || undefined,
        }),
      ]);

      setFunds(fundsRes?.funds || []);
      setGrandTotal(totalsRes?.grandTotal || "0.00");
      setTxns(recentRes?.transactions || []);
    } catch (e) {
      setError(e?.message || "Could not load transactions");
    } finally {
      setLoading(false);
    }
  }, [fundId, fromDate, isAdmin, toDate]);

  const onRefresh = useCallback(async () => {
    setRefreshing(true);
    await loadData();
    setRefreshing(false);
  }, [loadData]);

  useEffect(() => {
    if (!profile) {
      navigation.replace("Welcome");
      return;
    }
    if (!isAdmin) {
      navigation.replace("Give");
      return;
    }
    loadData();
    refreshUnreadCount();
    const unsubscribe = navigation.addListener("focus", () => {
      loadData();
      refreshUnreadCount();
    });
    return unsubscribe;
  }, [isAdmin, loadData, navigation, profile, refreshUnreadCount]);

  const applyFilters = async () => {
    await loadData();
  };

  const applyQuickRange = (days) => {
    const today = new Date();
    const from = new Date();
    from.setDate(today.getDate() - (days - 1));
    setFromDate(formatDateInput(from));
    setToDate(formatDateInput(today));
  };

  const hasQuickRange = (days) => {
    const today = new Date();
    const from = new Date();
    from.setDate(today.getDate() - (days - 1));
    return fromDate === formatDateInput(from) && toDate === formatDateInput(today);
  };

  const onExportCsv = async () => {
    try {
      setExporting(true);
      setError("");
      const csv = await exportAdminTransactionsCsv({
        limit: 1000,
        fundId: fundId || undefined,
        from: fromDate || undefined,
        to: toDate || undefined,
      });

      await Share.share({
        title: "Churpay transactions export",
        message: csv,
      });
    } catch (e) {
      setError(e?.message || "Could not export CSV");
    } finally {
      setExporting(false);
    }
  };

  const copyReference = async (reference) => {
    if (!reference) return;
    await Clipboard.setStringAsync(reference);
    Alert.alert("Copied", "Transaction reference copied.");
  };

  const isCashPending = (txn) => {
    const provider = String(txn?.provider || "").toLowerCase();
    const status = String(txn?.status || "").toUpperCase();
    const verified = !!txn?.cashVerifiedByAdmin;
    return provider === "cash" && !verified && (status === "PREPARED" || status === "RECORDED");
  };

  const onConfirmCash = async () => {
    if (!selectedTxn) return;
    const paymentIntentId = selectedTxn.paymentIntentId || selectedTxn.payment_intent_id || null;
    if (!paymentIntentId) {
      Alert.alert("Missing record id", "This cash record is missing a payment intent id. Refresh and try again.");
      return;
    }
    try {
      setCashActionLoading(true);
      setError("");
      await confirmAdminCashGiving(paymentIntentId);
      Alert.alert("Confirmed", "Cash record confirmed.");
      setSelectedTxn(null);
      await loadData();
    } catch (e) {
      setError(e?.message || "Could not confirm cash record");
    } finally {
      setCashActionLoading(false);
    }
  };

  const onOpenRejectCash = () => {
    setRejectCashNote("");
    setRejectCashOpen(true);
  };

  const onRejectCash = async () => {
    if (!selectedTxn) return;
    const paymentIntentId = selectedTxn.paymentIntentId || selectedTxn.payment_intent_id || null;
    const note = String(rejectCashNote || "").trim();
    if (!paymentIntentId) {
      Alert.alert("Missing record id", "This cash record is missing a payment intent id. Refresh and try again.");
      return;
    }
    if (!note) {
      Alert.alert("Reason required", "Please enter a short reason for rejecting this cash record.");
      return;
    }
    try {
      setCashActionLoading(true);
      setError("");
      await rejectAdminCashGiving(paymentIntentId, note);
      Alert.alert("Rejected", "Cash record rejected.");
      setRejectCashOpen(false);
      setSelectedTxn(null);
      await loadData();
    } catch (e) {
      setError(e?.message || "Could not reject cash record");
    } finally {
      setCashActionLoading(false);
    }
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={exporting ? "Exporting..." : "Export CSV (share)"} variant="secondary" onPress={onExportCsv} disabled={exporting} />
          <PrimaryButton label="Church settings" variant="ghost" onPress={() => navigation.navigate("AdminChurch")} />
        </View>
      }
    >
      <BrandHeader />
      <TopHeroHeader
        tone="admin"
        badge="Admin Workspace"
        title="Transactions"
        subtitle="Review received donations and filter by period or fund."
        churchName={profile?.churchName}
        actions={[
          {
            label: unreadCount ? `Alerts (${unreadCount})` : "Alerts",
            highlight: !!unreadCount,
            onPress: () => navigation.navigate("Notifications"),
          },
          { label: "Church settings", onPress: () => navigation.navigate("AdminChurch") },
          { label: "Profile", onPress: () => navigation.navigate("Profile") },
        ]}
      />
      <AdminTabBar navigation={navigation} activeTab="transactions" />

      <Card style={{ gap: spacing.sm }}>
        <Text style={{ color: palette.muted, fontSize: typography.small }}>Total received</Text>
        <Text style={{ color: palette.text, fontSize: typography.h1, fontWeight: "700" }}>{money(grandTotal)}</Text>
        <Text style={{ color: palette.muted, fontSize: typography.small }}>
          {fromDate || toDate ? `Range: ${fromDate || "start"} to ${toDate || "today"}` : "Range: all time"}
        </Text>
      </Card>

      <Card style={{ gap: spacing.sm }}>
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Quick filters</Text>
        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: spacing.xs }}>
          <QuickAmountChip label="Today" active={hasQuickRange(1)} onPress={() => applyQuickRange(1)} />
          <QuickAmountChip label="7 days" active={hasQuickRange(7)} onPress={() => applyQuickRange(7)} />
          <QuickAmountChip label="30 days" active={hasQuickRange(30)} onPress={() => applyQuickRange(30)} />
        </ScrollView>
        <TextField label="From date (YYYY-MM-DD)" value={fromDate} onChangeText={setFromDate} placeholder="2026-02-01" />
        <TextField label="To date (YYYY-MM-DD)" value={toDate} onChangeText={setToDate} placeholder="2026-02-29" />
        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: spacing.xs }}>
          <Pressable
            onPress={() => setFundId("")}
            style={{
              paddingVertical: spacing.xs,
              paddingHorizontal: spacing.md,
              borderRadius: 999,
              backgroundColor: fundId ? palette.focus : palette.primary,
            }}
          >
            <Text style={{ color: fundId ? palette.text : "#fff", fontWeight: "600" }}>All funds</Text>
          </Pressable>
          {funds.map((f) => (
            <Pressable
              key={f.id}
              onPress={() => setFundId(f.id)}
              style={{
                paddingVertical: spacing.xs,
                paddingHorizontal: spacing.md,
                borderRadius: 999,
                backgroundColor: fundId === f.id ? palette.primary : palette.focus,
              }}
            >
              <Text style={{ color: fundId === f.id ? "#fff" : palette.text, fontWeight: "600" }}>{f.name}</Text>
            </Pressable>
          ))}
        </ScrollView>
        <PrimaryButton label="Apply filters" variant="secondary" onPress={applyFilters} />
      </Card>

      {loading ? (
        <LoadingCards count={4} />
      ) : (
        <ScrollView refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={palette.primary} />} contentContainerStyle={{ gap: spacing.sm, marginTop: spacing.md }}>
          {txns.map((t) => (
            <Pressable key={t.id} onPress={() => setSelectedTxn(t)}>
              <Card padding={spacing.md} style={{ gap: spacing.xs }}>
                <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
                  <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700" }}>{money(t.amount)}</Text>
                  <StatusChip label={String(t.status || "PAID").toUpperCase()} active={String(t.status || "PAID").toUpperCase() === "PAID"} />
                </View>
                <Text style={{ color: palette.text, fontWeight: "600" }}>{t.fundName || t.fundCode || "Fund"}</Text>
                {(() => {
                  const persona = resolveTransactionPersona(t);
                  if (persona.onBehalf) {
                    return (
                      <Text style={{ color: palette.muted }}>
                        Paid for {persona.beneficiaryLabel} • Payer: {persona.payerLabel}
                      </Text>
                    );
                  }
                  if (persona.visitor) {
                    return <Text style={{ color: palette.muted }}>Visitor: {persona.payerLabel}</Text>;
                  }
                  return <Text style={{ color: palette.muted }}>{persona.payerLabel}</Text>;
                })()}
                <Text style={{ color: palette.muted, fontSize: typography.small }}>
                  {String(t.provider || t.channel || "app").toUpperCase()} • {new Date(t.createdAt).toLocaleString()}
                </Text>
                <Text style={{ color: palette.muted, fontSize: typography.small }}>{t.reference}</Text>
              </Card>
            </Pressable>
          ))}
          {txns.length === 0 ? (
            <EmptyStateCard icon="📥" title="No transactions found" subtitle="Adjust filters or check again later." actionLabel="Clear fund filter" onAction={() => setFundId("")} />
          ) : null}
        </ScrollView>
      )}
      <ErrorBanner message={error} />

      <Modal transparent animationType="fade" visible={!!selectedTxn} onRequestClose={() => setSelectedTxn(null)}>
        <View style={styles.modalBackdrop}>
          <Card style={{ gap: spacing.md, width: "92%" }}>
            <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700" }}>
              {selectedTxn ? resolveTransactionPersona(selectedTxn).tag : "Transaction details"}
            </Text>
            <Body>Amount: {money(selectedTxn?.amount)}</Body>
            <Body>Fund: {selectedTxn?.fundName || selectedTxn?.fundCode}</Body>
            {selectedTxn && resolveTransactionPersona(selectedTxn).onBehalf ? (
              <>
                <Body>Paid for: {resolveTransactionPersona(selectedTxn).beneficiaryLabel}</Body>
                <Body>Payer: {resolveTransactionPersona(selectedTxn).payerLabel}</Body>
              </>
            ) : selectedTxn && resolveTransactionPersona(selectedTxn).visitor ? (
              <Body>Visitor: {resolveTransactionPersona(selectedTxn).payerLabel}</Body>
            ) : (
              <Body>Member: {resolveTransactionPersona(selectedTxn).payerLabel}</Body>
            )}
            <Body>Provider: {String(selectedTxn?.provider || selectedTxn?.channel || "app").toUpperCase()}</Body>
            <Body>Status: {String(selectedTxn?.status || "PAID").toUpperCase()}</Body>
            <Body>Created: {selectedTxn?.createdAt ? new Date(selectedTxn.createdAt).toLocaleString() : "-"}</Body>
            <Body>Reference: {selectedTxn?.reference || "-"}</Body>
            {selectedTxn && isCashPending(selectedTxn) ? (
              <Card style={{ gap: spacing.sm, borderColor: palette.border }}>
                <Body muted>Cash approval required</Body>
                <Body muted>Confirm after counting the cash, or reject with a note.</Body>
                <View style={{ flexDirection: "row", gap: spacing.xs }}>
                  <View style={{ flex: 1 }}>
                    <PrimaryButton
                      label={cashActionLoading ? "Confirming..." : "Confirm cash"}
                      variant="secondary"
                      onPress={onConfirmCash}
                      disabled={cashActionLoading}
                    />
                  </View>
                  <View style={{ flex: 1 }}>
                    <PrimaryButton
                      label="Reject"
                      variant="ghost"
                      onPress={onOpenRejectCash}
                      disabled={cashActionLoading}
                    />
                  </View>
                </View>
              </Card>
            ) : null}
            <View style={{ flexDirection: "row", gap: spacing.xs }}>
              <View style={{ flex: 1 }}>
                <PrimaryButton label="Copy reference" variant="secondary" onPress={() => copyReference(selectedTxn?.reference)} />
              </View>
              <View style={{ flex: 1 }}>
                <PrimaryButton label="Close" variant="ghost" onPress={() => setSelectedTxn(null)} />
              </View>
            </View>
          </Card>
        </View>
      </Modal>

      <Modal transparent animationType="fade" visible={rejectCashOpen} onRequestClose={() => setRejectCashOpen(false)}>
        <View style={styles.modalBackdrop}>
          <Card style={{ gap: spacing.md, width: "92%" }}>
            <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700" }}>Reject cash record</Text>
            <Body muted>Provide a short note so the member understands what to fix.</Body>
            <TextField
              label="Rejection note"
              value={rejectCashNote}
              onChangeText={setRejectCashNote}
              placeholder="e.g. Amount does not match counting sheet"
              multiline
            />
            <View style={{ flexDirection: "row", gap: spacing.xs }}>
              <View style={{ flex: 1 }}>
                <PrimaryButton
                  label={cashActionLoading ? "Rejecting..." : "Reject"}
                  variant="secondary"
                  onPress={onRejectCash}
                  disabled={cashActionLoading}
                />
              </View>
              <View style={{ flex: 1 }}>
                <PrimaryButton label="Cancel" variant="ghost" onPress={() => setRejectCashOpen(false)} disabled={cashActionLoading} />
              </View>
            </View>
          </Card>
        </View>
      </Modal>
    </Screen>
  );
}

const styles = StyleSheet.create({
  bodyText: {
    lineHeight: 22,
  },
  hero: {
    alignItems: "center",
    justifyContent: "center",
    paddingTop: 36,
    paddingBottom: 20,
  },
  heroLogo: {
    maxWidth: 500,
    maxHeight: 250,
  },
  heroTagline: {
    marginTop: 6,
    fontSize: 18,
    fontWeight: "600",
    textAlign: "center",
  },
  title: {
    fontWeight: "700",
  },
  subtitle: {
    fontWeight: "400",
  },
  modalBackdrop: {
    flex: 1,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(0,0,0,0.35)",
    padding: 16,
  },
});

function RootNavigator() {
  const { palette } = useTheme();
  const { token, profile, booting, logout } = useContext(AuthContext);
  const navigationRef = useRef(null);
  const pendingGiveLinkRef = useRef(null);
  const inactivityTimerRef = useRef(null);
  const inactivityWarningTimerRef = useRef(null);
  const lastActivityRef = useRef(Date.now());
  const backgroundAtRef = useRef(null);
  const autoLogoutInProgressRef = useRef(false);
  const appStateRef = useRef(AppState.currentState || "active");
  const inactivityWatchdogRef = useRef(null);
  const isPrivilegedSession = isAdminRole(profile?.role);
  const IDLE_TIMEOUT_MINUTES = isPrivilegedSession ? 15 : 10;
  const IDLE_TIMEOUT_MS = IDLE_TIMEOUT_MINUTES * 60 * 1000;
  const IDLE_WARNING_BEFORE_MS = 60 * 1000;
  const IDLE_WARNING_MS = Math.max(0, IDLE_TIMEOUT_MS - IDLE_WARNING_BEFORE_MS);

  const navTheme = useMemo(
    () => ({
      ...DefaultTheme,
      colors: {
        ...DefaultTheme.colors,
        background: palette.background,
        card: palette.card,
        text: palette.text,
        border: palette.border,
        primary: palette.primary,
      },
    }),
    [palette]
  );

  const initialRoute = token
    ? isAdminRole(profile?.role)
      ? profile?.churchId
        ? "AdminFunds"
        : "AdminChurch"
      : profile?.churchId
        ? "Give"
        : "JoinChurch"
    : "Welcome";

  const navKey = token
    ? isAdminRole(profile?.role)
      ? "admin"
      : profile?.churchId
        ? "give"
        : "join"
    : "welcome";

  const applyGiveLink = useCallback((payload) => {
    if (!payload || !navigationRef.current) return false;

    const joinCode = payload.joinCode ? String(payload.joinCode).trim().toUpperCase() : "";
    const fundCode = payload.fundCode ? String(payload.fundCode).trim() : "";
    const amount = payload.amount;

    if (!token) {
      pendingGiveLinkRef.current = { joinCode, fundCode: fundCode || null, amount: amount || null };
      navigationRef.current.navigate("Login", { mode: "member" });
      return true;
    }

    if (isAdminRole(profile?.role)) {
      pendingGiveLinkRef.current = null;
      navigationRef.current.navigate(profile?.churchId ? "AdminFunds" : "AdminChurch");
      return true;
    }

    if (joinCode && !profile?.churchId) {
      // Store so we can prefill after the member joins the church.
      pendingGiveLinkRef.current = { joinCode, fundCode: fundCode || null, amount: amount || null };
      navigationRef.current.navigate("JoinChurch", { joinCode, mode: "join" });
      return true;
    }

    pendingGiveLinkRef.current = null;
    navigationRef.current.navigate("Give", {
      fundCode: fundCode || undefined,
      amount: amount || undefined,
    });
    return true;
  }, [profile?.churchId, profile?.role, token]);

  const handleIncomingUrl = useCallback((url) => {
    if (!url || !navigationRef.current) return;
    let parsed;
    try {
      parsed = new URL(url);
    } catch (_err) {
      return;
    }

    const host = String(parsed.host || "").toLowerCase();
    const path = String(parsed.pathname || "").toLowerCase();
    const proto = String(parsed.protocol || "").toLowerCase();

    const isPayfastLink =
      host.includes("payfast") ||
      path.includes("/payfast/") ||
      path === "/return" ||
      path === "/cancel";

    if (isPayfastLink) {
      if (!token) return;

      const paymentIntentId = parsed.searchParams.get("pi");
      const mPaymentId = parsed.searchParams.get("mp");
      const isCancel = path.endsWith("/cancel") || path === "/cancel";

      if (isCancel) {
        navigationRef.current.navigate(isAdminRole(profile?.role) ? "AdminFunds" : "Give");
        return;
      }

      if (paymentIntentId) {
        navigationRef.current.navigate("Pending", {
          intent: {
            paymentIntentId,
            mPaymentId: mPaymentId || null,
          },
        });
      }
      return;
    }

    const isCustomGiveLink = proto === "churpaydemo:" && host === "give";
    const isUniversalGiveLink =
      proto === "https:" &&
      (host === "churpay.com" || host === "www.churpay.com") &&
      path.startsWith("/g/");

    if (!isCustomGiveLink && !isUniversalGiveLink) return;

    const joinCode = isUniversalGiveLink
      ? String(path.split("/")[2] || "")
      : String(parsed.searchParams.get("joinCode") || "");

    const fundCode = String(parsed.searchParams.get("fund") || parsed.searchParams.get("fundCode") || "");
    const amountRaw = parsed.searchParams.get("amount");
    const amount = amountRaw ? Number(amountRaw) : null;

    applyGiveLink({
      joinCode: joinCode ? decodeURIComponent(joinCode) : "",
      fundCode: fundCode || null,
      amount: Number.isFinite(amount) && amount > 0 ? amount : null,
    });
  }, [applyGiveLink, profile?.role, token]);

  useEffect(() => {
    const processIncomingUrl = (url) => {
      if (!url) return;
      setTimeout(() => handleIncomingUrl(url), 300);
    };

    Linking.getInitialURL()
      .then((url) => processIncomingUrl(url))
      .catch(() => {});

    const sub = Linking.addEventListener("url", ({ url }) => processIncomingUrl(url));
    return () => sub.remove();
  }, [handleIncomingUrl]);

  useEffect(() => {
    if (!token) return;
    if (!pendingGiveLinkRef.current) return;
    const t = setTimeout(() => {
      if (pendingGiveLinkRef.current) {
        applyGiveLink(pendingGiveLinkRef.current);
      }
    }, 350);
    return () => clearTimeout(t);
  }, [applyGiveLink, profile?.churchId, profile?.role, token]);

  const clearInactivityTimer = useCallback(() => {
    if (!inactivityTimerRef.current) return;
    clearTimeout(inactivityTimerRef.current);
    inactivityTimerRef.current = null;
  }, []);

  const clearInactivityWarningTimer = useCallback(() => {
    if (!inactivityWarningTimerRef.current) return;
    clearTimeout(inactivityWarningTimerRef.current);
    inactivityWarningTimerRef.current = null;
  }, []);

  const clearInactivityWatchdog = useCallback(() => {
    if (!inactivityWatchdogRef.current) return;
    clearInterval(inactivityWatchdogRef.current);
    inactivityWatchdogRef.current = null;
  }, []);

  const handleAutoLogout = useCallback(async () => {
    if (!token || autoLogoutInProgressRef.current) return;
    autoLogoutInProgressRef.current = true;
    clearInactivityTimer();
    clearInactivityWarningTimer();
    try {
      await logout();
      Alert.alert("Session expired", `You were logged out after ${IDLE_TIMEOUT_MINUTES} minutes of inactivity.`);
    } catch (_err) {
      // no-op
    } finally {
      autoLogoutInProgressRef.current = false;
    }
  }, [IDLE_TIMEOUT_MINUTES, clearInactivityTimer, clearInactivityWarningTimer, logout, token]);

  const scheduleInactivityTimer = useCallback(() => {
    clearInactivityTimer();
    clearInactivityWarningTimer();
    if (!token) return;

    if (IDLE_WARNING_MS > 0) {
      inactivityWarningTimerRef.current = setTimeout(() => {
        if (!token) return;
        if (appStateRef.current !== "active") return;
        if (autoLogoutInProgressRef.current) return;
        Alert.alert(
          "Still there?",
          "For your security, we’ll sign you out soon due to inactivity.",
          [
            {
              text: "Stay signed in",
              onPress: () => {
                lastActivityRef.current = Date.now();
                scheduleInactivityTimer();
              },
            },
            {
              text: "Log out",
              style: "destructive",
              onPress: async () => {
                try {
                  await logout();
                } finally {
                  // no-op
                }
              },
            },
          ]
        );
      }, IDLE_WARNING_MS);
    }

    inactivityTimerRef.current = setTimeout(() => {
      void handleAutoLogout();
    }, IDLE_TIMEOUT_MS);
  }, [IDLE_TIMEOUT_MS, IDLE_WARNING_MS, clearInactivityTimer, clearInactivityWarningTimer, handleAutoLogout, logout, token]);

  const recordActivity = useCallback(() => {
    if (!token) return;
    lastActivityRef.current = Date.now();
    scheduleInactivityTimer();
  }, [scheduleInactivityTimer, token]);

  useEffect(() => {
    if (!token) {
      backgroundAtRef.current = null;
      appStateRef.current = AppState.currentState || "active";
      clearInactivityTimer();
      clearInactivityWarningTimer();
      clearInactivityWatchdog();
      return undefined;
    }

    recordActivity();
    clearInactivityWatchdog();
    inactivityWatchdogRef.current = setInterval(() => {
      if (!token) return;
      if (appStateRef.current !== "active") return;
      if (Date.now() - lastActivityRef.current >= IDLE_TIMEOUT_MS) {
        void handleAutoLogout();
      }
    }, 5000);

    const subscription = AppState.addEventListener("change", (nextState) => {
      appStateRef.current = nextState;
      if (nextState === "active") {
        if (backgroundAtRef.current && Date.now() - backgroundAtRef.current >= IDLE_TIMEOUT_MS) {
          backgroundAtRef.current = null;
          void handleAutoLogout();
          return;
        }
        backgroundAtRef.current = null;
        recordActivity();
        return;
      }

      if (nextState === "background" || nextState === "inactive") {
        backgroundAtRef.current = Date.now();
        clearInactivityTimer();
        clearInactivityWarningTimer();
      }
    });

    return () => {
      subscription.remove();
      clearInactivityTimer();
      clearInactivityWarningTimer();
      clearInactivityWatchdog();
    };
  }, [clearInactivityTimer, clearInactivityWarningTimer, clearInactivityWatchdog, handleAutoLogout, recordActivity, token]);

  if (booting) {
    return <BootScreen />;
  }

  return (
    <View
      style={{ flex: 1 }}
      onStartShouldSetResponderCapture={() => {
        recordActivity();
        return false;
      }}
    >
      <NavigationContainer ref={navigationRef} key={navKey} theme={navTheme}>
        <Stack.Navigator initialRouteName={initialRoute} screenOptions={{ headerShown: false }}>
          <Stack.Screen name="Welcome" component={WelcomeScreen} />
          <Stack.Screen name="Login" component={LoginScreen} />
          <Stack.Screen name="PasswordReset" component={PasswordResetScreen} />
          <Stack.Screen name="Register" component={RegisterScreen} />
          <Stack.Screen name="VerifyEmail" component={VerifyEmailScreen} />
          <Stack.Screen name="JoinChurch" component={JoinChurchScreen} />
          <Stack.Screen name="Give" component={GiveScreen} />
          <Stack.Screen name="Confirm" component={ConfirmScreen} />
          <Stack.Screen name="GivingLink" component={GivingLinkScreen} />
          <Stack.Screen name="Pending" component={PendingScreen} />
          <Stack.Screen name="Success" component={SuccessScreen} />
          <Stack.Screen name="CashReceipt" component={CashReceiptScreen} />
          <Stack.Screen name="MemberTransactions" component={MemberTransactionsScreen} />
          <Stack.Screen name="Notifications" component={NotificationsScreen} />
          <Stack.Screen name="Profile" component={ProfileScreen} />
          <Stack.Screen name="AdminChurch" component={AdminChurchScreen} />
          <Stack.Screen name="AdminFunds" component={AdminFundsScreen} />
          <Stack.Screen name="AdminQr" component={AdminQrScreen} />
          <Stack.Screen name="AdminTransactions" component={AdminTransactionsScreen} />
        </Stack.Navigator>
      </NavigationContainer>
    </View>
  );
}

export default function App() {
  console.log("[boot] App render");

  const [showBoot, setShowBoot] = useState(true);

  useEffect(() => {
    const t = setTimeout(() => setShowBoot(false), 1800);
    return () => clearTimeout(t);
  }, []);

  if (showBoot) {
    return <BootScreen />;
  }

  return (
    <View style={{ flex: 1 }}>
      <SafeAreaProvider>
        <AuthProvider>
          <RootNavigator />
        </AuthProvider>
      </SafeAreaProvider>
    </View>
  );
}
