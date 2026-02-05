import React, { useCallback, useContext, useEffect, useMemo, useState } from "react";
import { View, Text, StyleSheet, ActivityIndicator, Linking, Image, Pressable, Alert } from "react-native";
import { NavigationContainer, DefaultTheme } from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { Screen } from "./src/components/ui/Screen";
import { Card } from "./src/components/ui/Card";
import { PrimaryButton } from "./src/components/ui/PrimaryButton";
import { TextField } from "./src/components/ui/TextField";
import { BrandHeader } from "./src/components/ui/BrandHeader";
import { LinkButton } from "./src/components/ui/LinkButton";
import { useTheme } from "./src/components/ui/theme";
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
  createFund,
  updateFund as apiUpdateFund,
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
    (async () => {
      console.log("BOOT start");
      console.log("BOOT loading token...");
      const stored = await loadSessionToken();
      console.log("BOOT token loaded", !!stored);
      if (stored) {
        setTokenState(stored);
        try {
          console.log("BOOT fetching profile...");
          await refreshProfile();
        } catch (_) {
          await setSessionToken(null);
          setTokenState(null);
        }
      }
      console.log("BOOT done");
      setBooting(false);
    })();
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
      if (res?.checkoutUrl) {
        navigation.navigate("Pending", { intent: res });
        await Linking.openURL(res.checkoutUrl);
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
  const intent = route.params?.intent;
  return (
    <Screen footer={<PrimaryButton label="Back to start" onPress={() => navigation.popToTop()} />}>
      <BrandHeader />
      <Text style={[styles.title, { color: palette.text, fontSize: typography.h1 }]}>Payment Pending</Text>
      <Text style={[styles.subtitle, { color: palette.muted, fontSize: typography.body }]}>We are processing your donation.</Text>
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
        <Body muted>Payment Pending</Body>
        {intent?.m_payment_id ? (
          <View
            style={{
              paddingHorizontal: spacing.lg,
              paddingVertical: spacing.sm,
              borderRadius: radius.pill,
              backgroundColor: palette.focus,
            }}
          >
            <Text style={{ color: palette.text }}>Ref: {intent.m_payment_id}</Text>
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
            <PrimaryButton label="Admin funds" variant="secondary" onPress={() => navigation.navigate("AdminFunds")} />
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

function AdminFundsScreen({ navigation }) {
  const { spacing, palette, typography } = useTheme();
  const { profile } = useContext(AuthContext);
  const [funds, setFunds] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [name, setName] = useState("");
  const [code, setCode] = useState("");

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
        <View style={{ gap: spacing.sm, marginTop: spacing.md }}>
          {funds.map((f) => (
            <Card key={f.id} padding={spacing.md} style={{ flexDirection: "row", justifyContent: "space-between", alignItems: "center" }}>
              <View>
                <Text style={{ color: palette.text, fontSize: typography.h3 }}>{f.name}</Text>
                <Text style={{ color: palette.muted }}>{f.code}</Text>
                <Text style={{ color: palette.muted }}>{f.active ? "Active" : "Inactive"}</Text>
              </View>
              <PrimaryButton label={f.active ? "Disable" : "Enable"} variant="ghost" onPress={() => toggleActive(f)} />
            </Card>
          ))}
          {funds.length === 0 && <Body muted>No funds yet.</Body>}
        </View>
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

  if (booting) {
    return (
      <Screen>
        <ActivityIndicator color={palette.primary} />
      </Screen>
    );
  }

  return (
    <NavigationContainer theme={navTheme}>
      <Stack.Navigator initialRouteName={initialRoute} screenOptions={{ headerShown: false }}>
        <Stack.Screen name="Welcome" component={WelcomeScreen} />
        <Stack.Screen name="Login" component={LoginScreen} />
        <Stack.Screen name="Register" component={RegisterScreen} />
        <Stack.Screen name="JoinChurch" component={JoinChurchScreen} />
        <Stack.Screen name="Give" component={GiveScreen} />
        <Stack.Screen name="Confirm" component={ConfirmScreen} />
        <Stack.Screen name="Pending" component={PendingScreen} />
        <Stack.Screen name="Profile" component={ProfileScreen} />
        <Stack.Screen name="AdminFunds" component={AdminFundsScreen} />
      </Stack.Navigator>
    </NavigationContainer>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <RootNavigator />
    </AuthProvider>
  );
}
