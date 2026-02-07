import React, { useCallback, useContext, useEffect, useMemo, useRef, useState } from "react";
import { View, Text, StyleSheet, ActivityIndicator, Linking, Image, Pressable, Alert, ScrollView, RefreshControl, Share, Modal, useWindowDimensions, AppState } from "react-native";
import { NavigationContainer, DefaultTheme } from "@react-navigation/native";
import { SafeAreaProvider } from "react-native-safe-area-context";
import QRCode from "react-native-qrcode-svg";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import * as Clipboard from "expo-clipboard";
import { Screen } from "./src/components/ui/Screen";
import { Card } from "./src/components/ui/Card";
import { PrimaryButton } from "./src/components/ui/PrimaryButton";
import { TextField } from "./src/components/ui/TextField";
import { BrandHeader } from "./src/components/ui/BrandHeader";
import { LinkButton } from "./src/components/ui/LinkButton";
import { useTheme } from "./src/components/ui/theme";
import { withTimeout, safe } from "./src/utils/boot";
import {
  loadSessionToken,
  setSessionToken,
  registerMember,
  loginMember,
  loginAdmin,
  joinChurch,
  getProfile,
  updateProfile,
  listFunds,
  createPaymentIntent,
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
} from "./src/api";

const Stack = createNativeStackNavigator();
const AuthContext = React.createContext(null);

const money = (n) => `R ${Number(n || 0).toFixed(2)}`;
const PLATFORM_FEE_FIXED = 2.5;
const PLATFORM_FEE_PCT = 0.0075;
const isAdminRole = (role) => role === "admin" || role === "super";
const formatDateInput = (date) => new Date(date.getTime() - date.getTimezoneOffset() * 60000).toISOString().slice(0, 10);
const roundCurrency = (value) => Number((Math.round(Number(value || 0) * 100) / 100).toFixed(2));
const estimateCheckoutPricing = (amount) => {
  const donationAmount = roundCurrency(amount);
  const churpayFee = roundCurrency(PLATFORM_FEE_FIXED + donationAmount * PLATFORM_FEE_PCT);
  const totalCharged = roundCurrency(donationAmount + churpayFee);
  return { donationAmount, churpayFee, totalCharged };
};

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
  const { palette, spacing, typography } = useTheme();
  return (
    <Card style={{ alignItems: "center", gap: spacing.sm }}>
      <Text style={{ fontSize: 28 }}>{icon}</Text>
      <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700", textAlign: "center" }}>{title}</Text>
      {subtitle ? <Text style={{ color: palette.muted, textAlign: "center", fontSize: typography.body }}>{subtitle}</Text> : null}
      {actionLabel && onAction ? <PrimaryButton label={actionLabel} variant="secondary" onPress={onAction} /> : null}
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
            height: 84,
            borderRadius: 16,
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
  const { palette, spacing, typography } = useTheme();
  return (
    <Pressable onPress={onPress} style={{ opacity: selected ? 1 : 0.94 }}>
      <Card
        padding={spacing.lg}
        style={{
          borderColor: selected ? palette.primary : palette.border,
          borderWidth: 1,
          backgroundColor: selected ? palette.focus : palette.card,
          gap: spacing.sm,
        }}
      >
        <View style={{ flexDirection: "row", alignItems: "center", gap: spacing.md }}>
          <View
            style={{
              width: 38,
              height: 38,
              borderRadius: 19,
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
            <Text style={{ color: palette.muted, marginTop: spacing.xs }}>{String(fund.code || "").toUpperCase()}</Text>
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
        paddingVertical: spacing.xs,
        paddingHorizontal: spacing.md,
        borderRadius: 999,
        borderWidth: 1,
        borderColor: active ? palette.primary : palette.border,
        backgroundColor: active ? palette.primary : palette.card,
      }}
    >
      <Text style={{ color: active ? palette.onPrimary : palette.text, fontWeight: "700" }}>{label}</Text>
    </Pressable>
  );
};

const AdminTabBar = ({ navigation, activeTab }) => {
  const { palette, spacing } = useTheme();
  const tabs = [
    { key: "funds", label: "Funds", screen: "AdminFunds" },
    { key: "qr", label: "QR", screen: "AdminQr" },
    { key: "transactions", label: "Transactions", screen: "AdminTransactions" },
    { key: "profile", label: "Profile", screen: "Profile" },
  ];

  return (
    <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: spacing.xs, paddingBottom: spacing.xs }}>
      {tabs.map((tab) => (
        <Pressable
          key={tab.key}
          onPress={() => {
            if (activeTab !== tab.key) navigation.replace(tab.screen);
          }}
          style={{
            paddingVertical: spacing.xs,
            paddingHorizontal: spacing.md,
            borderRadius: 999,
            backgroundColor: activeTab === tab.key ? palette.primary : palette.focus,
          }}
        >
          <Text style={{ color: activeTab === tab.key ? "#fff" : palette.text, fontWeight: "600" }}>
            {tab.label}
          </Text>
        </Pressable>
      ))}
    </ScrollView>
  );
};

function BootScreen() {
  const { palette } = useTheme();
  return (
    <View style={{ flex: 1, alignItems: "center", justifyContent: "center", gap: 16 }}>
      <Image source={require("./assets/churpay-logo-500x250.png")} style={{ width: 140, height: 70 }} resizeMode="contain" />
      <Text style={{ color: palette.muted, fontSize: 16, fontWeight: "600" }}>Giving made easy.</Text>
      <ActivityIndicator color={palette.primary} />
    </View>
  );
}

function AuthProvider({ children }) {
  const [token, setTokenState] = useState(null);
  const [profile, setProfile] = useState(null);
  const [booting, setBooting] = useState(true);

  useEffect(() => {
    const hardStop = setTimeout(() => setBooting(false), 7000);
    return () => clearTimeout(hardStop);
  }, []);

  const refreshProfile = useCallback(async () => {
    if (!token) return null;
    const res = await getProfile();
    if (res?.member) setProfile(res.member);
    else if (res) setProfile(res);
    return res?.member || res;
  }, [token]);

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
              const profileRes = await withTimeout(safe(getProfile()), 4000);
              if (profileRes?.member) setProfile(profileRes.member);
              else if (profileRes?.profile) setProfile(profileRes.profile);
              else if (profileRes) setProfile(profileRes);
              else {
                // Stored token may be stale/invalid; clear local auth so user can re-login.
                await setSessionToken(null);
                setTokenState(null);
                setProfile(null);
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

  const setSession = useCallback(
    async (data) => {
      if (data?.token) {
        await setSessionToken(data.token);
        setTokenState(data.token);
        if (data?.member) setProfile(data.member);
        else await refreshProfile();
      } else {
        await setSessionToken(null);
        setTokenState(null);
        setProfile(null);
      }
    },
    [refreshProfile]
  );

  const logout = useCallback(async () => {
    await apiLogout();
    await setSessionToken(null);
    setTokenState(null);
    setProfile(null);
  }, []);

  const value = useMemo(
    () => ({ token, profile, setSession, setProfile, refreshProfile, logout, booting }),
    [token, profile, setSession, refreshProfile, logout, booting]
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
        <Image source={require("./assets/churpay-logo-500x250.png")} style={[styles.heroLogo, { width: heroLogoWidth, height: heroLogoHeight }]} resizeMode="contain" />
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
  const isAdminAuth = authMode === "admin";

  useEffect(() => {
    const mode = route?.params?.mode;
    if (mode === "member" || mode === "admin") {
      setAuthMode(mode);
    }
  }, [route?.params?.mode]);

  const onSubmit = async () => {
    try {
      setLoading(true);
      setError("");
      const data = isAdminAuth
        ? await loginAdmin({ identifier, password })
        : await loginMember({ identifier, password });
      await setSession(data);
      if (isAdminRole(data?.member?.role)) {
        navigation.replace(data?.member?.churchId ? "AdminFunds" : "AdminChurch");
      } else {
        navigation.replace(data?.member?.churchId ? "Give" : "JoinChurch");
      }
    } catch (e) {
      setError(e?.message || "Login failed");
    } finally {
      setLoading(false);
    }
  };

  const onForgotPassword = () => {
    Alert.alert("Coming soon", "Password reset will be added in a future update.");
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
      {isAdminAuth ? (
        <Card style={{ gap: spacing.xs }}>
          <Text style={{ color: palette.text, fontWeight: "700" }}>Admin mode</Text>
          <Text style={{ color: palette.muted, fontSize: typography.small }}>You can switch back to member sign in at any time.</Text>
        </Card>
      ) : (
        <Card style={{ gap: spacing.xs }}>
          <Text style={{ color: palette.text, fontWeight: "700" }}>Member mode</Text>
          <Text style={{ color: palette.muted, fontSize: typography.small }}>Give quickly with your church and fund preferences.</Text>
        </Card>
      )}
      <ErrorBanner message={error} />
    </Screen>
  );
}

function RegisterScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { setSession } = useContext(AuthContext);
  const [fullName, setFullName] = useState("");
  const [phone, setPhone] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const onSubmit = async () => {
    try {
      setLoading(true);
      setError("");
      const data = await registerMember({ fullName, phone, email, password });
      await setSession(data);
      navigation.replace("JoinChurch");
    } catch (e) {
      setError(e?.message || "Registration failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Screen
      footer={
        <PrimaryButton label={loading ? "Creating..." : "Create account"} onPress={onSubmit} disabled={!fullName || !phone || !password || loading} />
      }
    >
      <BrandHeader />
      <SectionTitle title="Create account" subtitle="We’ll keep your details safe." />
      <Card style={{ gap: spacing.md }}>
        <TextField label="Full name" value={fullName} onChangeText={setFullName} placeholder="e.g. Thandi Dlamini" />
        <TextField label="Mobile number" value={phone} onChangeText={setPhone} placeholder="e.g. 0712345678" keyboardType="phone-pad" />
        <TextField label="Email (optional)" value={email} onChangeText={setEmail} placeholder="you@example.com" keyboardType="email-address" />
        <TextField label="Password" value={password} onChangeText={setPassword} placeholder="••••••" secureTextEntry />
      </Card>
      <ErrorBanner message={error} />
    </Screen>
  );
}

function JoinChurchScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile, refreshProfile, setProfile, token, logout } = useContext(AuthContext);
  const [joinCode, setJoinCode] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!token) navigation.replace("Welcome");
    if (isAdminRole(profile?.role)) navigation.replace(profile?.churchId ? "AdminFunds" : "AdminChurch");
    if (profile?.churchId) navigation.replace("Give");
  }, [profile?.churchId, profile?.role, navigation, token]);

  const onSubmit = async () => {
    try {
      setLoading(true);
      setError("");
      const res = await joinChurch(joinCode);
      if (res?.member) setProfile(res.member);
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

  return (
    <Screen footer={<PrimaryButton label={loading ? "Joining..." : "Join church"} onPress={onSubmit} disabled={!joinCode || loading} />}>
      <BrandHeader />
      <SectionTitle title="Join your church" subtitle="Enter the join code from your church admin." />
      <Card style={{ gap: spacing.md }}>
        <TextField label="Join code" value={joinCode} onChangeText={setJoinCode} placeholder="e.g. GCCOC-1234" autoCapitalize="characters" />
      </Card>
      <ErrorBanner message={error} />
    </Screen>
  );
}

function GiveScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile, token } = useContext(AuthContext);
  const [funds, setFunds] = useState([]);
  const [loading, setLoading] = useState(true);
  const [selected, setSelected] = useState(null);
  const [amount, setAmount] = useState("");
  const [error, setError] = useState("");

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
      navigation.replace("JoinChurch");
      return;
    }
    try {
      setLoading(true);
      const data = await listFunds();
      setFunds(data?.funds || []);
    } catch (e) {
      setError(e?.message || "Could not load funds");
    } finally {
      setLoading(false);
    }
  }, [navigation, profile?.churchId, profile?.role, token]);

  useEffect(() => {
    loadFunds();
  }, [loadFunds]);

  const quickAmounts = [50, 100, 200, 500];

  const onContinue = () => {
    const amt = Number(amount);
    if (!selected) return setError("Choose a fund");
    if (!amt || amt <= 0) return setError("Enter an amount");
    setError("");
    navigation.navigate("Confirm", { fund: selected, amount: amt });
  };

  return (
    <Screen footer={<PrimaryButton label="Continue" onPress={onContinue} disabled={loading || !selected || !Number(amount)} />}>
      <BrandHeader />
      <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
        <View style={{ flex: 1 }}>
          <SectionTitle title="Give" subtitle="Choose a fund and set your amount." churchName={profile?.churchName} />
        </View>
        <LinkButton label="Profile" onPress={() => navigation.navigate("Profile")} />
      </View>

      <Card style={{ gap: spacing.md }}>
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
          <EmptyStateCard
            icon="💒"
            title="No funds available yet"
            subtitle="Ask your church admin to create at least one fund."
            actionLabel="Refresh"
            onAction={loadFunds}
          />
        )}
      </Card>

      <Card style={{ gap: spacing.md }}>
        <View style={{ gap: spacing.xs }}>
          <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>Amount</Text>
          <Text style={{ color: palette.muted }}>Enter an amount or use a quick amount.</Text>
        </View>
        <TextField label={null} value={amount} onChangeText={setAmount} placeholder="R 200.00" keyboardType="decimal-pad" />
        <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: spacing.xs }}>
          {quickAmounts.map((value) => (
            <QuickAmountChip
              key={value}
              label={`R${value}`}
              active={Number(amount) === value}
              onPress={() => setAmount(String(value))}
            />
          ))}
        </ScrollView>
      </Card>

      <ErrorBanner message={error} />
    </Screen>
  );
}

function ConfirmScreen({ navigation, route }) {
  const { spacing, palette, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const fund = route.params?.fund;
  const amount = route.params?.amount;
  const pricing = useMemo(() => estimateCheckoutPricing(Number(amount || 0)), [amount]);

  const createIntent = async () => {
    try {
      setSubmitting(true);
      setError("");
      const res = await createPaymentIntent({ fundId: fund.id, amount: Number(amount) });
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

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.md }}>
          <PrimaryButton label={submitting ? "Opening PayFast..." : "Pay with PayFast"} onPress={createIntent} disabled={submitting} />
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
      </Card>
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
                Alert.alert("Coming soon", "Member transaction history will be available soon.");
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

function ProfileScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile, refreshProfile, setProfile, logout } = useContext(AuthContext);
  const [fullName, setFullName] = useState(profile?.fullName || "");
  const [phone, setPhone] = useState(profile?.phone || "");
  const [email, setEmail] = useState(profile?.email || "");
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
  }, [profile]);

  const onSave = async () => {
    try {
      setLoading(true);
      setError("");
      const res = await updateProfile({ fullName, phone, email });
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
      <SectionTitle title="Your profile" subtitle="Manage your details and church." churchName={profile?.churchName} />
      {isAdmin ? <AdminTabBar navigation={navigation} activeTab="profile" /> : null}
      <Card style={{ gap: spacing.md }}>
        <TextField label="Full name" value={fullName} onChangeText={setFullName} />
        <TextField label="Mobile number" value={phone} onChangeText={setPhone} keyboardType="phone-pad" />
        <TextField label="Email" value={email} onChangeText={setEmail} keyboardType="email-address" />
      </Card>
      <Card style={{ gap: spacing.sm, marginTop: spacing.md }}>
        <Body muted>Church</Body>
        <Body>{profile?.churchName || "Not joined"}</Body>
        {isAdmin ? (
          <PrimaryButton label="Church settings" variant="ghost" onPress={() => navigation.navigate("AdminChurch")} />
        ) : (
          <PrimaryButton label={profile?.churchId ? "Switch church" : "Join a church"} variant="ghost" onPress={() => navigation.navigate("JoinChurch")} />
        )}
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

  const isAdmin = isAdminRole(profile?.role);

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
  }, [isAdmin, loadChurch, navigation, profile]);

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
      <SectionTitle
        title="Church profile"
        subtitle="Set your church name. Join code is optional and auto-generated if blank."
        churchName={profile?.churchName}
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
  const [name, setName] = useState("");
  const [code, setCode] = useState("");
  const [refreshing, setRefreshing] = useState(false);
  const [creating, setCreating] = useState(false);
  const [summary, setSummary] = useState({ activeCount: 0, lastDonationAt: null });

  const isAdmin = isAdminRole(profile?.role);

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
  }, [isAdmin, loadFunds, navigation, profile]);

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
      <SectionTitle title="Funds" subtitle="Create, edit and control giving funds." churchName={profile?.churchName} />
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

  const isAdmin = isAdminRole(profile?.role);

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
  }, [isAdmin, loadFunds, navigation, profile]);

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
      <SectionTitle title="QR Codes" subtitle="Create donation QR links in three quick steps." churchName={profile?.churchName} />
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
  const [refreshing, setRefreshing] = useState(false);
  const [selectedTxn, setSelectedTxn] = useState(null);

  const isAdmin = isAdminRole(profile?.role);

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
  }, [isAdmin, loadData, navigation, profile]);

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
      <SectionTitle title="Transactions" subtitle="Review received donations and filter by period or fund." churchName={profile?.churchName} />
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
                <Text style={{ color: palette.muted }}>{t.memberName || t.memberPhone || "Anonymous donor"}</Text>
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
            <Text style={{ color: palette.text, fontSize: typography.h2, fontWeight: "700" }}>Transaction details</Text>
            <Body>Amount: {money(selectedTxn?.amount)}</Body>
            <Body>Fund: {selectedTxn?.fundName || selectedTxn?.fundCode}</Body>
            <Body>Member: {selectedTxn?.memberName || selectedTxn?.memberPhone || "Anonymous"}</Body>
            <Body>Provider: {String(selectedTxn?.provider || selectedTxn?.channel || "app").toUpperCase()}</Body>
            <Body>Status: {String(selectedTxn?.status || "PAID").toUpperCase()}</Body>
            <Body>Created: {selectedTxn?.createdAt ? new Date(selectedTxn.createdAt).toLocaleString() : "-"}</Body>
            <Body>Reference: {selectedTxn?.reference || "-"}</Body>
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
  const inactivityTimerRef = useRef(null);
  const lastActivityRef = useRef(Date.now());
  const backgroundAtRef = useRef(null);
  const autoLogoutInProgressRef = useRef(false);
  const appStateRef = useRef(AppState.currentState || "active");
  const inactivityWatchdogRef = useRef(null);
  const IDLE_TIMEOUT_MS = 60 * 1000;

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

  const handlePayfastDeepLink = useCallback((url) => {
    if (!url || !navigationRef.current) return;
    let parsed;
    try {
      parsed = new URL(url);
    } catch (_err) {
      return;
    }

    const host = String(parsed.host || "").toLowerCase();
    const path = String(parsed.pathname || "").toLowerCase();
    const isPayfastLink =
      host.includes("payfast") ||
      path.includes("/payfast/") ||
      path === "/return" ||
      path === "/cancel";

    if (!isPayfastLink) return;

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
  }, [profile?.role]);

  useEffect(() => {
    if (!token) return undefined;

    const processIncomingUrl = (url) => {
      if (!url) return;
      setTimeout(() => handlePayfastDeepLink(url), 300);
    };

    Linking.getInitialURL()
      .then((url) => processIncomingUrl(url))
      .catch(() => {});

    const sub = Linking.addEventListener("url", ({ url }) => processIncomingUrl(url));
    return () => sub.remove();
  }, [handlePayfastDeepLink, token]);

  const clearInactivityTimer = useCallback(() => {
    if (!inactivityTimerRef.current) return;
    clearTimeout(inactivityTimerRef.current);
    inactivityTimerRef.current = null;
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
    try {
      await logout();
      Alert.alert("Session expired", "You were logged out after 1 minute of inactivity.");
    } catch (_err) {
      // no-op
    } finally {
      autoLogoutInProgressRef.current = false;
    }
  }, [clearInactivityTimer, logout, token]);

  const scheduleInactivityTimer = useCallback(() => {
    clearInactivityTimer();
    if (!token) return;
    inactivityTimerRef.current = setTimeout(() => {
      void handleAutoLogout();
    }, IDLE_TIMEOUT_MS);
  }, [clearInactivityTimer, handleAutoLogout, token]);

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
      }
    });

    return () => {
      subscription.remove();
      clearInactivityTimer();
      clearInactivityWatchdog();
    };
  }, [clearInactivityTimer, clearInactivityWatchdog, handleAutoLogout, recordActivity, token]);

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
          <Stack.Screen name="Register" component={RegisterScreen} />
          <Stack.Screen name="JoinChurch" component={JoinChurchScreen} />
          <Stack.Screen name="Give" component={GiveScreen} />
          <Stack.Screen name="Confirm" component={ConfirmScreen} />
          <Stack.Screen name="Pending" component={PendingScreen} />
          <Stack.Screen name="Success" component={SuccessScreen} />
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
