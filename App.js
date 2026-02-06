import React, { useCallback, useContext, useEffect, useMemo, useState } from "react";
import { View, Text, StyleSheet, ActivityIndicator, Linking, Image, Pressable, Alert, ScrollView, RefreshControl, Share } from "react-native";
import { NavigationContainer, DefaultTheme } from "@react-navigation/native";
import { SafeAreaProvider } from "react-native-safe-area-context";
import QRCode from "react-native-qrcode-svg";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
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
  logout as apiLogout,
} from "./src/api";

const Stack = createNativeStackNavigator();
const AuthContext = React.createContext(null);

const money = (n) => `R ${Number(n || 0).toFixed(2)}`;

const Body = ({ children, muted }) => {
  const { palette, typography } = useTheme();
  return <Text style={[styles.bodyText, { color: muted ? palette.muted : palette.text, fontSize: typography.body }]}>{children}</Text>;
};

const FundCard = ({ fund, selected, onPress }) => {
  const { palette, spacing, typography } = useTheme();
  return (
    <Pressable onPress={onPress} style={{ opacity: selected ? 1 : 0.94 }}>
      <Card
        padding={spacing.lg}
        style={{
          borderColor: selected ? palette.primary : "transparent",
          borderWidth: selected ? 1 : 0,
          backgroundColor: selected ? palette.focus : palette.card,
        }}
      >
        <Text style={{ color: palette.text, fontWeight: "700", fontSize: typography.h2 }}>{fund.name}</Text>
        <Text style={{ color: palette.muted, marginTop: spacing.xs }}>{fund.code}</Text>
      </Card>
    </Pressable>
  );
};

function BootScreen() {
  const { palette } = useTheme();
  return (
    <View style={{ flex: 1, alignItems: "center", justifyContent: "center", gap: 16 }}>
      <Image source={require("./assets/churpay-logo.png")} style={{ width: 160, height: 160 }} resizeMode="contain" />
      <Text style={{ color: palette.muted, fontSize: 16, fontWeight: "600" }}>Giving made easy.</Text>
      <ActivityIndicator color={palette.primary} />
    </View>
  );
}

function AuthProvider({ children }) {
  const [token, setTokenState] = useState(null);
  const [profile, setProfile] = useState(null);
  const [booting, setBooting] = useState(true);

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
              await withTimeout(safe(refreshProfile()), 4000);
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
  }, [refreshProfile]);

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
    [token, profile, setSession, refreshProfile, logout]
  );

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

function WelcomeScreen({ navigation }) {
  const { spacing, palette } = useTheme();
  const { token, profile } = useContext(AuthContext);

  const continueFlow = () => {
    if (token && profile?.churchId) return navigation.replace("Give");
    if (token) return navigation.replace("JoinChurch");
    return navigation.navigate("Login");
  };

  return (
    <Screen
      disableScroll
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label="Continue" onPress={continueFlow} />
          <LinkButton label="I'm an admin" onPress={() => navigation.navigate("Login")} />
        </View>
      }
    >
      <View style={styles.hero}>
        <Image source={require("./assets/churpay-logo.png")} style={styles.heroLogo} resizeMode="contain" />
        <Text style={[styles.heroTagline, { color: palette.muted }]}>Giving made easy.</Text>
      </View>
    </Screen>
  );
}

function LoginScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { setSession } = useContext(AuthContext);
  const [identifier, setIdentifier] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const onSubmit = async () => {
    try {
      setLoading(true);
      setError("");
      const data = await loginMember({ identifier, password });
      await setSession(data);
      navigation.replace(data?.member?.churchId ? "Give" : "JoinChurch");
    } catch (e) {
      setError(e?.message || "Login failed");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={loading ? "Signing in..." : "Sign In"} onPress={onSubmit} disabled={!identifier || !password || loading} />
          <LinkButton label="Create an account" onPress={() => navigation.navigate("Register")} />
        </View>
      }
    >
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Welcome back</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Sign in with your phone or email.</Text>
      <Card style={{ gap: spacing.md }}>
        <TextField label="Phone or email" value={identifier} onChangeText={setIdentifier} placeholder="0712345678 or you@example.com" />
        <TextField label="Password" value={password} onChangeText={setPassword} placeholder="••••••" secureTextEntry />
        {error ? <Body muted>{error}</Body> : null}
      </Card>
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
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={loading ? "Creating..." : "Create account"} onPress={onSubmit} disabled={!fullName || !phone || !password || loading} />
          <LinkButton label="I already have an account" onPress={() => navigation.navigate("Login")} />
        </View>
      }
    >
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Create account</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>We’ll keep your details safe.</Text>
      <Card style={{ gap: spacing.md }}>
        <TextField label="Full name" value={fullName} onChangeText={setFullName} placeholder="e.g. Thandi Dlamini" />
        <TextField label="Mobile number" value={phone} onChangeText={setPhone} placeholder="e.g. 0712345678" keyboardType="phone-pad" />
        <TextField label="Email (optional)" value={email} onChangeText={setEmail} placeholder="you@example.com" keyboardType="email-address" />
        <TextField label="Password" value={password} onChangeText={setPassword} placeholder="••••••" secureTextEntry />
        {error ? <Body muted>{error}</Body> : null}
      </Card>
    </Screen>
  );
}

function JoinChurchScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile, refreshProfile, setProfile, token } = useContext(AuthContext);
  const [joinCode, setJoinCode] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    if (!token) navigation.replace("Welcome");
    if (profile?.churchId) navigation.replace("Give");
  }, [profile?.churchId, navigation, token]);

  const onSubmit = async () => {
    try {
      setLoading(true);
      setError("");
      const res = await joinChurch(joinCode);
      if (res?.member) setProfile(res.member);
      await refreshProfile();
      navigation.replace("Give");
    } catch (e) {
      setError(e?.message || "Could not join church");
    } finally {
      setLoading(false);
    }
  };

  return (
    <Screen footer={<PrimaryButton label={loading ? "Joining..." : "Join church"} onPress={onSubmit} disabled={!joinCode || loading} />}>
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Join your church</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Enter the join code from your church admin.</Text>
      <Card style={{ gap: spacing.md }}>
        <TextField label="Join code" value={joinCode} onChangeText={setJoinCode} placeholder="e.g. GCCOC-1234" autoCapitalize="characters" />
        {error ? <Body muted>{error}</Body> : null}
      </Card>
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
  }, [navigation, profile?.churchId, token]);

  useEffect(() => {
    loadFunds();
  }, [loadFunds]);

  const onContinue = () => {
    const amt = Number(amount);
    if (!selected) return setError("Choose a fund");
    if (!amt || amt <= 0) return setError("Enter an amount");
    setError("");
    navigation.navigate("Confirm", { fund: selected, amount: amt });
  };

  return (
    <Screen footer={<PrimaryButton label="Continue" onPress={onContinue} disabled={loading} />}>
      <BrandHeader />
      <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
        <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Enter amount</Text>
        <LinkButton label="Profile" onPress={() => navigation.navigate("Profile")} />
      </View>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>How much would you like to give?</Text>

      {loading ? (
        <ActivityIndicator color={palette.primary} />
      ) : (
        <View style={{ gap: spacing.md }}>
          {funds.map((f) => (
            <FundCard key={f.id} fund={f} selected={selected?.id === f.id} onPress={() => setSelected(f)} />
          ))}
          {funds.length === 0 && <Body muted>No funds available.</Body>}
        </View>
      )}

      <Card style={{ marginTop: spacing.xl, gap: spacing.md }}>
        <Text style={{ color: palette.muted, fontSize: typography.body }}>How much would you like to give?</Text>
        <TextField label={null} value={amount} onChangeText={setAmount} placeholder="R 200.00" keyboardType="decimal-pad" />
      </Card>

      {error ? <Body muted>{error}</Body> : null}
    </Screen>
  );
}

function ConfirmScreen({ navigation, route }) {
  const { spacing, palette, typography } = useTheme();
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState("");
  const fund = route.params?.fund;
  const amount = route.params?.amount;

  const createIntent = async () => {
    try {
      setSubmitting(true);
      setError("");
      const res = await createPaymentIntent({ fundId: fund.id, amount: Number(amount) });
      const checkoutUrl = res?.checkoutUrl || res?.paymentUrl;
      if (checkoutUrl) {
        navigation.navigate("Pending", { intent: res });
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
          <PrimaryButton label={submitting ? "Opening PayFast..." : "Confirm & Pay"} onPress={createIntent} disabled={submitting} />
          <PrimaryButton label="Back" variant="ghost" onPress={() => navigation.goBack()} />
        </View>
      }
    >
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Review</Text>
      <Card style={{ gap: spacing.sm }}>
        <Body>Fund: {fund?.name}</Body>
        <Body>Amount: {money(amount)}</Body>
        <Body muted>We will redirect you to PayFast to complete the payment.</Body>
      </Card>
      {error ? <Body muted>{error}</Body> : null}
    </Screen>
  );
}

function PendingScreen({ navigation, route }) {
  const { spacing, palette, radius, typography } = useTheme();
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
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Payment Pending</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Complete checkout in PayFast, then refresh status here.</Text>
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
          <Text style={{ color: palette.primary, fontSize: 48 }}>✓</Text>
        </View>
        <Body muted>Status: {status}</Body>
        {fallbackRef ? (
          <View
            style={{
              paddingHorizontal: spacing.lg,
              paddingVertical: spacing.sm,
              borderRadius: radius.pill,
              backgroundColor: palette.focus,
            }}
          >
            <Text style={{ color: palette.text }}>Ref: {fallbackRef}</Text>
          </View>
        ) : null}
      </Card>
      {error ? <Body muted>{error}</Body> : null}
    </Screen>
  );
}

function SuccessScreen({ navigation, route }) {
  const { spacing, palette, radius, typography } = useTheme();
  const intent = route.params?.intent || {};
  const isPaid = String(intent?.status || "").toUpperCase() === "PAID";
  const paymentRef = intent?.mPaymentId || intent?.m_payment_id || null;
  return (
    <Screen footer={<PrimaryButton label="Back home" onPress={() => navigation.popToTop()} />}>
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Thank you for giving</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Your generosity makes a difference.</Text>
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
          {profile?.role === "admin" || profile?.role === "super" ? (
            <View style={{ gap: spacing.xs }}>
              <PrimaryButton label="Church settings" variant="secondary" onPress={() => navigation.navigate("AdminChurch")} />
              <PrimaryButton label="Admin funds" variant="secondary" onPress={() => navigation.navigate("AdminFunds")} />
              <PrimaryButton label="Transactions" variant="secondary" onPress={() => navigation.navigate("AdminTransactions")} />
            </View>
          ) : null}
          <PrimaryButton label="Log out" variant="ghost" onPress={onLogout} />
        </View>
      }
    >
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Your profile</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Manage your details and church.</Text>
      <Card style={{ gap: spacing.md }}>
        <TextField label="Full name" value={fullName} onChangeText={setFullName} />
        <TextField label="Mobile number" value={phone} onChangeText={setPhone} keyboardType="phone-pad" />
        <TextField label="Email" value={email} onChangeText={setEmail} keyboardType="email-address" />
      </Card>
      <Card style={{ gap: spacing.sm, marginTop: spacing.md }}>
        <Body muted>Church</Body>
        <Body>{profile?.churchName || "Not joined"}</Body>
        <PrimaryButton label={profile?.churchId ? "Switch church" : "Join a church"} variant="ghost" onPress={() => navigation.navigate("JoinChurch")} />
      </Card>
      {error ? <Body muted>{error}</Body> : null}
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

  const isAdmin = profile?.role === "admin" || profile?.role === "super";

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
      const payload = { name: String(name || "").trim(), joinCode: String(joinCode || "").trim().toUpperCase() };
      if (!payload.name) throw new Error("Church name is required");
      if (!payload.joinCode) throw new Error("Join code is required");

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
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Church profile</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Set your church name and join code for members.</Text>
      {loading ? (
        <ActivityIndicator color={palette.primary} />
      ) : (
        <Card style={{ gap: spacing.md }}>
          <TextField label="Church name" value={name} onChangeText={setName} placeholder="Great Commission Church of Christ" />
          <TextField label="Join code" value={joinCode} onChangeText={setJoinCode} placeholder="GCCOC-1234" autoCapitalize="characters" />
          <Body muted>Members join with this code in the app.</Body>
        </Card>
      )}
      {error ? <Body muted>{error}</Body> : null}
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

  const isAdmin = profile?.role === "admin" || profile?.role === "super";

  const loadFunds = useCallback(async () => {
    if (!isAdmin) return;
    try {
      setLoading(true);
      const res = await listFunds(true);
      setFunds(res?.funds || []);
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
      await createFund({ name, code });
      setName("");
      setCode("");
      await loadFunds();
    } catch (e) {
      setError(e?.message || "Could not create fund");
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

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label="Back" variant="ghost" onPress={() => navigation.goBack()} />
        </View>
      }
    >
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Manage funds</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Create or disable funds for your church.</Text>

      <Card style={{ gap: spacing.md }}>
        <TextField label="Fund name" value={name} onChangeText={setName} placeholder="Building Project" />
        <TextField label="Code" value={code} onChangeText={setCode} placeholder="BLDG" autoCapitalize="characters" />
        <PrimaryButton label="Create fund" onPress={onCreate} disabled={!name || !code} />
      </Card>

      {loading ? (
        <ActivityIndicator color={palette.primary} />
      ) : (
        <ScrollView refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={palette.primary} />} contentContainerStyle={{ gap: spacing.sm, marginTop: spacing.md }}>
          {funds.map((f) => (
            <Card key={f.id} padding={spacing.md} style={{ gap: spacing.sm }}>
              <View style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
                <View>
                  <Text style={{ color: palette.text, fontSize: typography.h3 }}>{f.name}</Text>
                  <Text style={{ color: palette.muted }}>{f.code}</Text>
                  <Text style={{ color: palette.muted }}>{f.active ? "Active" : "Inactive"}</Text>
                </View>
                <PrimaryButton label={f.active ? "Disable" : "Enable"} variant="ghost" onPress={() => toggleActive(f)} />
              </View>
              <View style={{ alignItems: "center" }}>
                <Body muted>Scan to give to this fund</Body>
                <QRCode value={JSON.stringify({ fundId: f.id, churchId: profile?.churchId, fundCode: f.code })} size={120} />
              </View>
            </Card>
          ))}
          {funds.length === 0 && <Body muted>No funds yet.</Body>}
        </ScrollView>
      )}
      {error ? <Body muted>{error}</Body> : null}
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

  const isAdmin = profile?.role === "admin" || profile?.role === "super";

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

  return (
    <Screen
      footer={
        <View style={{ gap: spacing.sm }}>
          <PrimaryButton label={exporting ? "Exporting..." : "Export CSV"} variant="secondary" onPress={onExportCsv} disabled={exporting} />
          <PrimaryButton label="Back" variant="ghost" onPress={() => navigation.goBack()} />
        </View>
      }
    >
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Transactions</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>Filter by date/fund and review church totals.</Text>

      <Card style={{ gap: spacing.sm }}>
        <Body>Total received: {money(grandTotal)}</Body>
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
        <ActivityIndicator color={palette.primary} />
      ) : (
        <ScrollView refreshControl={<RefreshControl refreshing={refreshing} onRefresh={onRefresh} tintColor={palette.primary} />} contentContainerStyle={{ gap: spacing.sm, marginTop: spacing.md }}>
          {txns.map((t) => (
            <Card key={t.id} padding={spacing.md} style={{ gap: spacing.xs }}>
              <Text style={{ color: palette.text, fontSize: typography.h3 }}>{t.fundName || t.fundCode}</Text>
              <Body>{money(t.amount)}</Body>
              <Body muted>{t.reference}</Body>
              <Body muted>{new Date(t.createdAt).toLocaleString()}</Body>
              <Body muted>{t.memberName || t.memberPhone || "Anonymous"}</Body>
            </Card>
          ))}
          {txns.length === 0 && <Body muted>No transactions yet.</Body>}
        </ScrollView>
      )}
      {error ? <Body muted>{error}</Body> : null}
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
    paddingTop: 80,
    paddingBottom: 40,
  },
  heroLogo: {
    width: 500,
    height: 500,
  },
  heroTagline: {
    marginTop: 10,
    fontSize: 18,
    fontWeight: "600",
  },
  title: {
    fontWeight: "700",
  },
  subtitle: {
    fontWeight: "400",
  },
});

function RootNavigator() {
  const { palette } = useTheme();
  const { token, profile, booting } = useContext(AuthContext);

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

  const initialRoute = token ? (profile?.churchId ? "Give" : "JoinChurch") : "Welcome";

  const navKey = token ? (profile?.churchId ? "give" : "join") : "welcome";

  return (
    <NavigationContainer key={navKey} theme={navTheme}>
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
        <Stack.Screen name="AdminTransactions" component={AdminTransactionsScreen} />
      </Stack.Navigator>
    </NavigationContainer>
  );
}

export default function App() {
  console.log("[boot] App render");

  const [showBoot, setShowBoot] = useState(true);

  useEffect(() => {
    const t = setTimeout(() => setShowBoot(false), 800);
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
