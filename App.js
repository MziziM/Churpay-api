import React, { useMemo, useState, useEffect, useCallback } from "react";
import {
  View,
  Text,
  Pressable,
  TextInput,
  SafeAreaView,
  ScrollView,
  Platform,
  ActivityIndicator,
  Alert,
  Image,
  Share,
} from "react-native";
import * as Linking from "expo-linking";
import * as Clipboard from "expo-clipboard";
import * as Print from "expo-print";
import * as Sharing from "expo-sharing";
import * as FileSystem from "expo-file-system";
import { Asset } from "expo-asset";
import QRCode from "react-native-qrcode-svg";
import AsyncStorage from "@react-native-async-storage/async-storage";
import Svg, { Path } from "react-native-svg";
import { Ionicons } from "@expo/vector-icons";
import { NavigationContainer, DefaultTheme } from "@react-navigation/native";
import { createNativeStackNavigator } from "@react-navigation/native-stack";
import { apiGetTransactions } from "./src/api";
import { Brand, Logo } from "./brand";
import { API_BASE_URL, PILOT_CHURCH_ID } from "./config/api";

/**
 * ===========================
 * CHURPAY APP VARIANT
 * ===========================
 * This is intentionally simple so you can build two separate apps from the same codebase.
 * - For the Member app:  set APP_VARIANT = "member"
 * - For the Admin app:   set APP_VARIANT = "admin"
 */
const APP_VARIANT = "dual"; // "dual" for demo, or set to "member" / "admin" for separate builds
const isDualApp = APP_VARIANT === "dual";
const isAdminOnly = APP_VARIANT === "admin";
const isMemberOnly = APP_VARIANT === "member";


const Stack = createNativeStackNavigator();

const uid = () => Math.random().toString(36).slice(2, 10).toUpperCase();
const money = (n) => `R ${Number(n || 0).toFixed(2)}`;

const fmtDateTime = (iso) => {
  try {
    return new Date(iso).toLocaleString();
  } catch {
    return String(iso || "");
  }
};

const maskPhone = (phone) => {
  const s = String(phone || "").trim();
  if (!s) return "";
  const digits = s.replace(/\D/g, "");
  if (digits.length < 6) return s;
  const prefix = digits.slice(0, Math.min(4, digits.length - 2));
  const suffix = digits.slice(-2);
  return `${prefix}•••${suffix}`;
};

// ====== CHURPAY BRAND TOKENS (single source from /brand) ======
const BRAND = Brand;

const theme = {
  ...DefaultTheme,
  colors: {
    ...DefaultTheme.colors,
    background: BRAND.colors.bg,
    card: BRAND.colors.card,
    text: BRAND.colors.text,
    border: BRAND.colors.line,
    primary: BRAND.colors.primary,
  },
};

// ====== STORAGE KEYS ======
const STORAGE = {
  demo: "churpay.demo",
  church: "churpay.church",
  adminPin: "churpay.adminPin",
  funds: "churpay.funds",
  tx: "churpay.tx",
  memberProfile: "churpay.memberProfile",
};

// ====== OFFICIAL LOGO IMAGE (sourced via /brand) ======
const LOGO_IMAGE = Logo.source;

// ====== PDF LOGO (base64) ======
let _logoDataUri = null;
async function getLogoDataUri() {
  try {
    if (_logoDataUri) return _logoDataUri;
    const asset = Asset.fromModule(LOGO_IMAGE);
    await asset.downloadAsync();
    const uri = asset.localUri || asset.uri;
    if (!uri) return null;

    const b64 = await FileSystem.readAsStringAsync(uri, {
      encoding: FileSystem.EncodingType.Base64,
    });

    _logoDataUri = `data:image/png;base64,${b64}`;
    return _logoDataUri;
  } catch {
    return null;
  }
}

function ChurpayLogo({ height = 44 }) {
  return (
    <View style={{ alignItems: "center", justifyContent: "center" }}>
      <Image
        source={LOGO_IMAGE}
        style={{ height, width: Math.round(height * 3.2) }}
        resizeMode="contain"
      />
    </View>
  );
}
// ====== API CONFIG (PRODUCTION-LOCKED) ======
const DEFAULT_API_BASE_URL = API_BASE_URL;
const DEFAULT_CHURCH_ID = PILOT_CHURCH_ID;
const PILOT_MODE = true; // Phase 1: lock church + backend for pilot

async function apiJSON(baseUrl, path, opts = {}) {
  const url = `${String(baseUrl).replace(/\/$/, "")}${path}`;
  const res = await fetch(url, {
    headers: {
      "Content-Type": "application/json",
      ...(opts.headers || {}),
    },
    ...opts,
  });

  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {
    json = { raw: text };
  }

  if (!res.ok) {
    const msg = json?.error || json?.message || `HTTP ${res.status}`;
    throw new Error(msg);
  }

  return json;
}
// ====== OPTIONAL: fallback mark (kept for future icon use) ======
function ChurpayMark({ size = 40 }) {
  return (
    <View
      style={{
        width: size,
        height: size,
        borderRadius: Math.round(size * 0.35),
        backgroundColor: BRAND.colors.card,
        borderWidth: 1,
        borderColor: BRAND.colors.line,
        alignItems: "center",
        justifyContent: "center",
      }}
    >
      <Svg
        width={Math.round(size * 0.72)}
        height={Math.round(size * 0.72)}
        viewBox="0 0 64 64"
      >
        <Path
          d="M41.5 14.5c-4-3-8.3-4.5-13-4.5C15.7 10 5.5 20.2 5.5 33S15.7 56 28.5 56c4.8 0 9.1-1.5 13-4.5"
          fill="none"
          stroke={BRAND.colors.text}
          strokeWidth={6}
          strokeLinecap="round"
        />
        <Path
          d="M33 27c7.5-6.5 16-6.5 23.5 0"
          fill="none"
          stroke={BRAND.colors.teal}
          strokeWidth={6}
          strokeLinecap="round"
        />
        <Path
          d="M33 38c5.2-4.5 11-4.5 16.2 0"
          fill="none"
          stroke={BRAND.colors.teal}
          strokeWidth={6}
          strokeLinecap="round"
          opacity={0.9}
        />
      </Svg>
    </View>
  );
}

function startOfDayISO(d = new Date()) {
  const x = new Date(d);
  x.setHours(0, 0, 0, 0);
  return x.toISOString();
}

function daysAgo(n) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  return d;
}

function toCSV(rows) {
  const esc = (v) => {
    const s = String(v ?? "");
    if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
    return s;
  };
  const header = [
    "createdAt",
    "amount",
    "fund",
    "reference",
    "channel",
    "memberName",
    "memberPhone",
  ];
  const lines = [header.join(",")];
  for (const r of rows) {
    lines.push(
      [
        esc(r.createdAt),
        esc(r.amount),
        esc(r.fundName),
        esc(r.reference),
        esc(r.channel),
        esc(r.memberName),
        esc(r.memberPhone),
      ].join(",")
    );
  }
  return lines.join("\n");
}

export default function App() {
  // ====== Persisted app state ======
  const [booting, setBooting] = useState(true);
  const [demoMode, setDemoMode] = useState(true);
  const apiBaseUrl = DEFAULT_API_BASE_URL;
  const churchId = DEFAULT_CHURCH_ID;
  const [serverTotals, setServerTotals] = useState({ totals: [], grandTotal: "0.00" });
  const [churchName, setChurchName] = useState("The Great Commission Church");
  const [adminPin, setAdminPin] = useState("1234");

  const [memberProfile, setMemberProfile] = useState({
    name: "",
    phone: "",
  });

  const [funds, setFunds] = useState([]);

  /**
   * tx: { id, createdAt, fundId, fundName, amount, reference, channel, memberName, memberPhone }
   */
  const [tx, setTx] = useState([]);
  const [serverTx, setServerTx] = useState([]);

  const activeFunds = useMemo(() => funds.filter((f) => f.active), [funds]);

  const totals = useMemo(() => {
    const total = tx.reduce((s, i) => s + i.amount, 0);
    const byFund = Object.fromEntries(funds.map((f) => [f.id, 0]));
    tx.forEach((i) => (byFund[i.fundId] = (byFund[i.fundId] || 0) + i.amount));

    const todayStart = new Date(startOfDayISO());
    const weekStart = new Date(startOfDayISO(daysAgo(6)));
    const monthStart = new Date();
    monthStart.setDate(1);
    monthStart.setHours(0, 0, 0, 0);

    const today = tx.filter((t) => new Date(t.createdAt) >= todayStart).reduce((s, t) => s + t.amount, 0);
    const week = tx.filter((t) => new Date(t.createdAt) >= weekStart).reduce((s, t) => s + t.amount, 0);
    const month = tx.filter((t) => new Date(t.createdAt) >= monthStart).reduce((s, t) => s + t.amount, 0);

    return { total, byFund, count: tx.length, today, week, month };
  }, [tx, funds]);

  const saveAll = useCallback(
  async (next = {}) => {
    try {
      const payload = {
        demo: next.demoMode ?? demoMode,
        church: next.churchName ?? churchName,
        adminPin: next.adminPin ?? adminPin,
        funds: next.funds ?? funds,
        tx: next.tx ?? tx,
        memberProfile: next.memberProfile ?? memberProfile,
      };

      await Promise.all([
        AsyncStorage.setItem(STORAGE.demo, JSON.stringify(payload.demo)),
        AsyncStorage.setItem(STORAGE.church, payload.church),
        AsyncStorage.setItem(STORAGE.adminPin, payload.adminPin),
        AsyncStorage.setItem(STORAGE.funds, JSON.stringify(payload.funds)),
        AsyncStorage.setItem(STORAGE.tx, JSON.stringify(payload.tx)),
        AsyncStorage.setItem(STORAGE.memberProfile, JSON.stringify(payload.memberProfile)),
      ]);
    } catch (e) {
      // silent in demo
    }
  },
  [demoMode, churchName, adminPin, funds, tx, memberProfile, apiBaseUrl, churchId]
);

  useEffect(() => {
    (async () => {
      try {
        
const [d, c, p, f, t, mp] = await Promise.all([
  AsyncStorage.getItem(STORAGE.demo),
  AsyncStorage.getItem(STORAGE.church),
  AsyncStorage.getItem(STORAGE.adminPin),
  AsyncStorage.getItem(STORAGE.funds),
  AsyncStorage.getItem(STORAGE.tx),
  AsyncStorage.getItem(STORAGE.memberProfile),
]);
        if (d) setDemoMode(Boolean(JSON.parse(d)));
        if (c) setChurchName(c);
        if (p) setAdminPin(p);
        if (f) setFunds(JSON.parse(f));
        if (t) setTx(JSON.parse(t));
        if (mp) setMemberProfile(JSON.parse(mp));
      } catch (e) {
        // ignore boot errors
      } finally {
        setBooting(false);
      }
    })();
  }, []);

  // Save local app state
useEffect(() => {
  if (!booting) saveAll();
}, [
  booting,
  demoMode,
  churchName,
  adminPin,
  funds,
  tx,
  memberProfile,
  saveAll,
]);

// Load funds + totals from server (demo DB)
const loadFunds = useCallback(async () => {
  if (!churchId) return;
  try {
    const data = await apiJSON(apiBaseUrl, `/churches/${churchId}/funds`);
    if (Array.isArray(data?.funds)) {
      const mapped = data.funds.map((x) => ({
        id: x.id,
        name: x.name,
        active: !!x.active,
        code: x.code,
      }));
      setFunds(mapped);
      saveAll({ funds: mapped });
    }
  } catch (e) {
    console.warn("[app] loadFunds failed", e?.message || e);
  }
}, [apiBaseUrl, churchId, saveAll]);

const loadTotals = useCallback(async () => {
  if (!churchId) return;
  try {
    const data = await apiJSON(apiBaseUrl, `/churches/${churchId}/totals`);
    if (data?.totals && data?.grandTotal !== undefined) {
      setServerTotals({ totals: data.totals, grandTotal: data.grandTotal });
    }
  } catch (e) {
    console.warn("[app] loadTotals failed", e?.message || e);
  }
}, [apiBaseUrl, churchId]);

const loadTransactions = useCallback(
  async ({ limit = 200, offset = 0, fundId = null, channel = null, from = null, to = null } = {}) => {
    if (!churchId) return;
    try {
      const qs = new URLSearchParams();
      qs.set("limit", String(limit));
      qs.set("offset", String(offset));
      if (fundId) qs.set("fundId", String(fundId));
      if (channel) qs.set("channel", String(channel));
      if (from) qs.set("from", String(from));
      if (to) qs.set("to", String(to));

      const data = await apiJSON(apiBaseUrl, `/churches/${churchId}/transactions?${qs.toString()}`);
      if (Array.isArray(data?.transactions)) {
        // normalize to match UI expectations
        const mapped = data.transactions.map((t) => ({
          id: t.id,
          createdAt: t.createdAt,
          fundId: t.fundId,
          fundName: t.fundName,
          amount: Number(t.amount || 0),
          reference: t.reference,
          channel: t.channel,
          memberName: t.memberName || "",
          memberPhone: t.memberPhone || "",
        }));
        setServerTx(mapped);
      }
    } catch (e) {
      console.warn("[app] loadTransactions failed", e?.message || e);
    }
  },
  [apiBaseUrl, churchId]
);

useEffect(() => {
  if (booting) return;
  loadFunds();
  loadTotals();
  loadTransactions();
}, [booting, loadFunds, loadTotals, loadTransactions]);
  // ====== Deep linking (Member app mainly) ======
  const linking = {
    prefixes: [Linking.createURL("/"), "churpaydemo://", "https://churpay.com/"],
    config: {
      screens: {
        Landing: "",
        MemberGate: "login",
        MemberHome: "member",
        Contribute: "give",
        RequestHelp: "request",
        Receipt: "receipt",
        MemberHistory: "history",
        MemberProfile: "profile",
        AdminGate: "admin",
        AdminHome: "admin/home",
        AdminFunds: "admin/funds",
        AdminTx: "admin/transactions",
        AdminTxDetail: "admin/tx",
        POS: "admin/pos",
        AdminReports: "admin/reports",
        AdminSettings: "admin/settings",
      },
    },
  };


  if (booting) {
    return (
      <SafeAreaView style={[s.safe, { alignItems: "center", justifyContent: "center" }]}>
        <ActivityIndicator color={BRAND.colors.teal} />
        <Text style={[s.muted, { marginTop: 10 }]}>Loading churpay…</Text>
      </SafeAreaView>
    );
  }

  return (
    <NavigationContainer theme={theme} linking={linking}>
      <Stack.Navigator
        initialRouteName={isDualApp ? "Landing" : isAdminOnly ? "AdminGate" : "MemberGate"}
        screenOptions={{
          headerStyle: { backgroundColor: BRAND.colors.card },
          headerTintColor: BRAND.colors.text,
          contentStyle: { backgroundColor: BRAND.colors.bg },
          headerShadowVisible: false,
        }}
      >
        {/* ---------- LANDING (DUAL ONLY) ---------- */}
        {isDualApp && (
          <Stack.Screen name="Landing" options={{ headerShown: false }}>
            {(navProps) => (
              <LandingScreen
                {...navProps}
                churchName={churchName}
                serverTotals={serverTotals}
                onMember={() => navProps.navigation.navigate("MemberGate")}
                onAdmin={() => navProps.navigation.navigate("AdminGate")}
              />
            )}
          </Stack.Screen>
        )}

        {/* ---------- MEMBER GATE ---------- */}
        {(isDualApp || isMemberOnly) && (
          <Stack.Screen name="MemberGate" options={{ title: "Sign in" }}>
            {(navProps) => (
              <MemberGate
                {...navProps}
                churchName={churchName}
                profile={memberProfile}
                setProfile={(p) => {
                  setMemberProfile(p);
                  saveAll({ memberProfile: p });
                }}
                onSuccess={() => navProps.navigation.replace("MemberHome")}
              />
            )}
          </Stack.Screen>
        )}

        {/* ---------- MEMBER SCREENS ---------- */}
        {(isDualApp || isMemberOnly) && (
          <>
            <Stack.Screen name="MemberHome" options={{ title: "churpay" }}>
              {(navProps) => (
                <MemberHome
                  {...navProps}
                  churchName={churchName}
                  serverTotals={serverTotals}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="RequestHelp" options={{ title: "Request Help" }}>
              {(navProps) => (
                <RequestHelpScreen
                  {...navProps}
                  churchName={churchName}
                  activeFunds={activeFunds}
                  allFunds={funds}
                  churchId={churchId}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="Contribute" options={{ title: "Contribute" }}>
              {(navProps) => (
                <ContributeScreen
                  {...navProps}
                  demoMode={demoMode}
                  activeFunds={activeFunds}
                  allFunds={funds}
                  memberProfile={memberProfile}
                  apiBaseUrl={apiBaseUrl}
                  churchId={churchId}
                  onAfterPayment={loadTotals}
                  onCreateTx={(t) => {
                    setTx((prev) => [t, ...prev]);
                    navProps.navigation.replace("Receipt", { id: t.id });
                  }}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="Receipt" options={{ title: "Receipt" }}>
              {(navProps) => (
                <ReceiptScreen
                  {...navProps}
                  churchName={churchName}
                  demoMode={demoMode}
                  tx={tx}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="MemberHistory" options={{ title: "History" }}>
              {(navProps) => <MemberHistory {...navProps} tx={tx} />}
            </Stack.Screen>

            <Stack.Screen name="MemberProfile" options={{ title: "Profile" }}>
              {(navProps) => (
                <MemberProfile
                  {...navProps}
                  profile={memberProfile}
                  setProfile={(p) => {
                    setMemberProfile(p);
                    saveAll({ memberProfile: p });
                  }}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="QR" options={{ title: "Share QR" }}>
              {(navProps) => (
                <QRScreen
                  {...navProps}
                  demoMode={demoMode}
                  activeFunds={activeFunds}
                  churchName={churchName}
                />
              )}
            </Stack.Screen>
          </>
        )}

        {/* ---------- ADMIN GATE ---------- */}
        {(isDualApp || isAdminOnly) && (
          <Stack.Screen name="AdminGate" options={{ title: "Admin" }}>
            {(navProps) => (
              <AdminGate
                {...navProps}
                churchName={churchName}
                adminPin={adminPin}
                onSuccess={() => navProps.navigation.replace("AdminHome")}
              />
            )}
          </Stack.Screen>
        )}

        {/* ---------- ADMIN SCREENS ---------- */}
        {(isDualApp || isAdminOnly) && (
          <>
            <Stack.Screen name="AdminHome" options={{ title: "Admin Dashboard" }}>
              {(navProps) => (
                <AdminHome
                  {...navProps}
                  churchName={churchName}
                  serverTotals={serverTotals}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="AdminFunds" options={{ title: "Funds" }}>
              {(navProps) => (
                <AdminFunds
                  {...navProps}
                  funds={funds}
                  setFunds={setFunds}
                  apiBaseUrl={apiBaseUrl}
                  churchId={churchId}
                  onChanged={() => {
                    loadFunds();
                    loadTotals();
                  }}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="POS" options={{ title: "POS Mode" }}>
              {(navProps) => (
                <POSScreen
                  {...navProps}
                  demoMode={demoMode}
                  activeFunds={activeFunds}
                  allFunds={funds}
                  onCreateTx={(t) => setTx((prev) => [t, ...prev])}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="AdminTx" options={{ title: "Transactions" }}>
              {(navProps) => (
                <AdminTransactions
                  {...navProps}
                  tx={serverTx}
                  onRefresh={() => loadTransactions()}
                  onOpen={(id) => navProps.navigation.navigate("AdminTxDetail", { id })}
                  onClear={() => Alert.alert("Not supported", "Server transactions cannot be cleared from the app.")}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="AdminTxDetail" options={{ title: "Transaction" }}>
              {(navProps) => <AdminTxDetail {...navProps} tx={serverTx} />}
            </Stack.Screen>

            <Stack.Screen name="AdminReports" options={{ title: "Reports" }}>
              {(navProps) => (
                <AdminReports
                  {...navProps}
                  churchName={churchName}
                  tx={serverTx}
                  totals={totals}
                />
              )}
            </Stack.Screen>

            <Stack.Screen name="AdminSettings" options={{ title: "Settings" }}>
              {(navProps) => (
                <AdminSettings
                  {...navProps}
                  demoMode={demoMode}
                  adminPin={adminPin}
                  setAdminPin={(pin) => {
                    setAdminPin(pin);
                    saveAll({ adminPin: pin });
                  }}
                  churchName={churchName}
                  setChurchName={(name) => {
                    setChurchName(name);
                    saveAll({ churchName: name });
                  }}
                  onResetAll={async () => {
                    if (PILOT_MODE) {
                      Alert.alert("Disabled", "Reset is disabled during pilot to prevent data loss.");
                      return;
                    }
                    Alert.alert("Reset", "Clear all local data?", [
                      { text: "Cancel", style: "cancel" },
                      {
                        text: "Reset",
                        style: "destructive",
                        onPress: async () => {
                          setTx([]);
                          setFunds([]);
                          setMemberProfile({ name: "", phone: "" });
                          await AsyncStorage.multiRemove(Object.values(STORAGE));
                          navProps.navigation.replace(isDualApp ? "Landing" : "AdminGate");
                        },
                      },
                    ]);
                  }}
                />
              )}
            </Stack.Screen>
          </>
        )}
      </Stack.Navigator>
    </NavigationContainer>
  );
}

function LandingScreen({ churchName, serverTotals, onMember, onAdmin }) {
  const grand = Number(serverTotals?.grandTotal || 0);
  const tiles = [
    { k: "Total Raised", v: money(grand), sub: "All funds (server)" },
    { k: "Top Fund", v: serverTotals?.totals?.[0]?.name || "—", sub: "By name" },
    { k: "Fast Giving", v: "QR + POS", sub: "Built for services" },
  ];

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 14 }}>
        {/* HERO */}
        <View style={[s.hero, s.card]}> 
          <View style={{ flexDirection: "row", alignItems: "center", gap: 12 }}>
            <ChurpayMark size={46} />
            <View style={{ flex: 1 }}>
              <Text style={s.heroTitle}>churpay</Text>
              <Text style={s.heroSub}>{churchName}</Text>
            </View>
          </View>

          <Text style={[s.p, { marginTop: 12 }]}>
            Fast, trackable giving with receipts, reports, and POS readiness.
          </Text>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 14, flexWrap: "wrap" }}>
            {tiles.map((t) => (
              <View key={t.k} style={s.statTile}>
                <Text style={s.statK}>{t.k}</Text>
                <Text style={s.statV}>{t.v}</Text>
                <Text style={s.statSub}>{t.sub}</Text>
              </View>
            ))}
          </View>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 16 }}>
            <Pressable onPress={onMember} style={[s.btn, { flex: 1 }]}>
              <Text style={s.btnText}>Member</Text>
            </Pressable>
            <Pressable onPress={onAdmin} style={[s.btnSecondary, { flex: 1 }] }>
              <Text style={s.btnText}>Admin</Text>
            </Pressable>
          </View>
        </View>

        {/* FEATURE CARDS */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>What you get</Text>
          <View style={{ marginTop: 10, gap: 10 }}>
            <FeatureRow title="Instant receipts" body="Shareable receipts for reconciliation and proof." />
            <FeatureRow title="Fund tracking" body="Separate purposes: building, missions, events, etc." />
            <FeatureRow title="Admin reporting" body="Totals + exports for finance and leadership." />
            <FeatureRow title="POS mode" body="Fast in-service collection simulation." />
          </View>
        </View>

        {/* FOOTER */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Tip</Text>
          <Text style={s.p}>If you don't see any funds, Admin must enable them first.</Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function FeatureRow({ title, body }) {
  return (
    <View style={s.featureRow}>
      <View style={s.featureDot} />
      <View style={{ flex: 1 }}>
        <Text style={s.featureTitle}>{title}</Text>
        <Text style={s.featureBody}>{body}</Text>
      </View>
    </View>
  );
}

function MemberGate({ churchName, profile, setProfile, onSuccess }) {
  const [name, setName] = useState(profile?.name || "");
  const [phone, setPhone] = useState(profile?.phone || "");

  const canContinue = name.trim().length >= 2 && phone.trim().length >= 8;

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={[s.card, { alignItems: "center" }]}>
          <ChurpayLogo height={40} />
          <Text style={[s.sub, { marginTop: 6, textAlign: "center" }]}>{churchName}</Text>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Member sign in</Text>
          <Text style={s.p}>
            Pilot sign-in (stored on this device). Production will use secure authentication.
          </Text>

          <Text style={s.label}>Full name</Text>
          <TextInput
            value={name}
            onChangeText={setName}
            placeholder="e.g. Mzwakhe Mzizi"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />

          <Text style={s.label}>Phone</Text>
          <TextInput
            value={phone}
            onChangeText={(t) => setPhone(t.replace(/[^0-9+\s]/g, ""))}
            placeholder="e.g. +27 81 000 0000"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
            keyboardType={Platform.select({ ios: "phone-pad", android: "phone-pad" })}
          />

          <Pressable
            onPress={() => {
              if (!canContinue) {
                Alert.alert("Missing details", "Please enter your name and phone.");
                return;
              }
              const next = { name: name.trim(), phone: phone.trim() };
              setProfile(next);
              onSuccess();
            }}
            disabled={!canContinue}
            style={[s.btn, !canContinue && s.btnDisabled]}
          >
            <Text style={s.btnText}>Continue</Text>
          </Pressable>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

// ===========================
// MEMBER APP SCREENS
// ===========================

function MemberHome({ navigation, churchName, serverTotals }) {
  const grand = Number(serverTotals?.grandTotal || 0);
  const top = (serverTotals?.totals || []).slice().sort((a, b) => Number(b.total) - Number(a.total))[0];

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ paddingVertical: 16, gap: 12 }}>
        <View style={[s.hero, s.card]}>
          <View style={{ flexDirection: "row", alignItems: "center", gap: 12 }}>
            <ChurpayMark size={44} />
            <View style={{ flex: 1 }}>
              <Text style={s.heroTitle}>{churchName}</Text>
              <Text style={s.heroSub}>Secure giving • receipts • transparency</Text>
            </View>
          </View>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 14, flexWrap: "wrap" }}>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Grand Total (server)</Text>
              <Text style={s.statV}>{money(grand)}</Text>
              <Text style={s.statSub}>All funds combined</Text>
            </View>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Top Fund</Text>
              <Text style={s.statV}>{top?.name || "—"}</Text>
              <Text style={s.statSub}>{top ? money(top.total) : "No contributions yet"}</Text>
            </View>
          </View>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 14 }}>
            <Pressable style={[s.btn, { flex: 1 }]} onPress={() => navigation.navigate("Contribute")}>
              <Text style={s.btnText}>Give Now</Text>
            </Pressable>
            <Pressable style={[s.btnSecondary, { flex: 1 }]} onPress={() => navigation.navigate("RequestHelp")}>
              <Text style={s.btnText}>Request Help</Text>
            </Pressable>
          </View>

          <Pressable style={[s.btnSecondary, { marginTop: 10 }]} onPress={() => navigation.navigate("MemberHistory")}>
            <Text style={s.btnText}>My Giving</Text>
          </Pressable>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Why we give</Text>
          <Text style={s.p}>• Faithfulness • stewardship • mission</Text>
          <Text style={s.p}>• Trackable giving with receipts</Text>
          <Text style={s.p}>• Clear fund purposes for transparency</Text>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Quick actions</Text>
          <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap", marginTop: 10 }}>
            <Pressable onPress={() => navigation.navigate("MemberProfile")} style={s.actionCard}>
              <Text style={s.actionTitle}>Profile</Text>
              <Text style={s.actionBody}>Add name & phone for receipts</Text>
            </Pressable>
            <Pressable onPress={() => navigation.navigate("Contribute")} style={s.actionCard}>
              <Text style={s.actionTitle}>Give</Text>
              <Text style={s.actionBody}>Purpose → amount → confirm</Text>
            </Pressable>
          </View>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}
// ===========================
// REQUEST HELP SCREEN
// ===========================
function RequestHelpScreen({ navigation, churchName, activeFunds, allFunds, churchId }) {
  const [fundId, setFundId] = useState(activeFunds[0]?.id || "");
  const [amount, setAmount] = useState("200");
  const [behalfName, setBehalfName] = useState("");
  const [behalfPhone, setBehalfPhone] = useState("");
  const [note, setNote] = useState("");

  useEffect(() => {
    if (!fundId && activeFunds?.length) setFundId(activeFunds[0].id);
  }, [activeFunds, fundId]);

  const selectedFund = allFunds.find((f) => f.id === fundId);

  const url = useMemo(() => {
    const base = "https://churpay.com/give";
    const qs = new URLSearchParams();
    qs.set("churchId", String(churchId || DEFAULT_CHURCH_ID));
    qs.set("fund", String(fundId || ""));
    qs.set("amount", String(amount || ""));
    if (behalfName.trim()) qs.set("behalfName", behalfName.trim());
    if (behalfPhone.trim()) qs.set("behalfPhone", behalfPhone.trim());
    if (note.trim()) qs.set("note", note.trim());
    return `${base}?${qs.toString()}`;
  }, [churchId, fundId, amount, behalfName, behalfPhone, note]);

  const copy = async () => {
    await Clipboard.setStringAsync(url);
    Alert.alert("Copied", "Request link copied.");
  };

  const share = async () => {
    try {
      await Share.share({ message: url });
    } catch (e) {}
  };

  const openInApp = () => {
    navigation.navigate("Contribute", {
      fund: fundId,
      amount,
      behalfName: behalfName.trim(),
      behalfPhone: behalfPhone.trim(),
      note: note.trim(),
    });
  };

  const validAmount = Number(amount) > 0;

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={[s.card, { alignItems: "center" }]}>
          <ChurpayLogo height={32} />
          <Text style={[s.sub, { marginTop: 6, textAlign: "center" }]}>{churchName}</Text>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Request help to give</Text>
          <Text style={s.p}>
            Create a link you can share with someone who wants to sponsor your contribution. If they have the app, it will open
            directly. If not, it opens on the web.
          </Text>

          <Text style={[s.label, { marginTop: 12 }]}>Purpose</Text>
          <View style={{ marginTop: 10, gap: 10 }}>
            {activeFunds.map((f) => {
              const active = fundId === f.id;
              return (
                <Pressable key={f.id} onPress={() => setFundId(f.id)} style={[s.fundCard, active && s.fundCardActive]}>
                  <Text style={s.fundCardTitle}>{f.name}</Text>
                  <Text style={s.fundCardSub}>{active ? "Selected" : "Tap to select"}</Text>
                </Pressable>
              );
            })}
          </View>

          <Text style={[s.label, { marginTop: 12 }]}>Suggested amount</Text>
          <TextInput
            value={amount}
            onChangeText={(t) => setAmount(t.replace(/[^\d.]/g, ""))}
            keyboardType={Platform.select({ ios: "decimal-pad", android: "numeric" })}
            placeholder="e.g. 200"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.amountInput}
          />
          {!validAmount && <Text style={s.warn}>Enter a valid amount.</Text>}

          <Text style={[s.label, { marginTop: 12 }]}>Who are you requesting for?</Text>
          <TextInput
            value={behalfName}
            onChangeText={setBehalfName}
            placeholder="Your name (optional)"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />
          <TextInput
            value={behalfPhone}
            onChangeText={(t) => setBehalfPhone(t.replace(/[^0-9+\s]/g, ""))}
            placeholder="Your phone (optional)"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
            keyboardType={Platform.select({ ios: "phone-pad", android: "phone-pad" })}
          />

          <Text style={s.label}>Message (optional)</Text>
          <TextInput
            value={note}
            onChangeText={setNote}
            placeholder="e.g. Please help me give today"
            placeholderTextColor={BRAND.colors.textMuted}
            style={[s.input, { minHeight: 80 }]}
            multiline
          />

          <View style={{ marginTop: 12 }}>
            <Text style={s.label}>Preview link</Text>
            <Text style={s.code}>{url}</Text>
          </View>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 14, flexWrap: "wrap" }}>
            <Pressable onPress={copy} style={[s.btnSecondary, { flex: 1, minWidth: 140 }]}>
              <Text style={s.btnText}>Copy link</Text>
            </Pressable>
            <Pressable onPress={share} style={[s.btn, { flex: 1, minWidth: 140 }]}>
              <Text style={s.btnText}>Share link</Text>
            </Pressable>
          </View>

          <Pressable onPress={openInApp} style={[s.btnSecondary, { marginTop: 10 }]}>
            <Text style={s.btnText}>Open in my app</Text>
          </Pressable>

          <Text style={[s.muted, { marginTop: 10 }]}>Tip: Print QR codes from Admin for in-service giving.</Text>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>How it works</Text>
          <Text style={s.p}>• You share the link</Text>
          <Text style={s.p}>• Sponsor taps it → app opens (if installed) or web opens</Text>
          <Text style={s.p}>• Sponsor confirms payment → receipt is generated</Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function MemberProfile({ profile, setProfile }) {
  const [name, setName] = useState(profile?.name || "");
  const [phone, setPhone] = useState(profile?.phone || "");

  const save = () => {
    setProfile({
      name: name.trim(),
      phone: phone.trim(),
    });
    Alert.alert("Saved", "Profile saved on this device.");
  };

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={s.card}>
          <Text style={s.sectionTitle}>Your details</Text>
          <Text style={s.p}>Optional. Used on receipts and admin exports.</Text>

          <Text style={s.label}>Full name</Text>
          <TextInput
            value={name}
            onChangeText={setName}
            placeholder="e.g. Mzwakhe Mzizi"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />

          <Text style={s.label}>Phone (optional)</Text>
          <TextInput
            value={phone}
            onChangeText={(t) => setPhone(t.replace(/[^0-9+\s]/g, ""))}
            placeholder="e.g. +27 81 000 0000"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />

          <Pressable onPress={save} style={s.btnSecondary}>
            <Text style={s.btnText}>Save</Text>
          </Pressable>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function ContributeScreen({
  route,
  demoMode,
  activeFunds,
  allFunds,
  memberProfile,
  onCreateTx,
  apiBaseUrl,
  churchId,
  onAfterPayment,
}) {
  const preFund = route?.params?.fund;
  const preAmount = route?.params?.amount;
  const source = route?.params?.source;
  const behalfName = route?.params?.behalfName || "";
  const behalfPhone = route?.params?.behalfPhone || "";
  const behalfNote = route?.params?.note || "";

  const isQrSource = source === "qr" || !!preFund;

  // Step flow: purpose -> amount -> confirm
  const [step, setStep] = useState("purpose"); // purpose | amount | confirm

  const initialFund =
    preFund && allFunds.some((f) => f.id === preFund) ? preFund : activeFunds[0]?.id;

  const [fundId, setFundId] = useState(initialFund || "");
  const [amount, setAmount] = useState(isQrSource ? "" : preAmount ? String(preAmount) : "100");
  const [reference, setReference] = useState("");
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    if (preFund && allFunds.some((f) => f.id === preFund)) setFundId(preFund);
    if (!isQrSource && preAmount) setAmount(String(preAmount));
    if (isQrSource && !preAmount) setAmount("");
  }, [preFund, preAmount, allFunds, isQrSource]);

  // If funds load after mount and no fund is selected yet, default to first active fund.
  useEffect(() => {
    if (!fundId && activeFunds?.length) {
      setFundId(activeFunds[0].id);
    }
  }, [activeFunds, fundId]);

  const selectedFund = allFunds.find((f) => f.id === fundId);
  const amountNum = Number(amount);
  const validAmount = Number.isFinite(amountNum) && amountNum > 0 && amountNum <= 1000000;
  const canContinuePurpose = !!selectedFund;
  const canContinueAmount = !!selectedFund && validAmount;

  const channel = behalfName || behalfPhone || behalfNote ? "request" : isQrSource ? "qr" : "member";

  const setQuickAmount = (n) => setAmount(String(n));

  const goNext = () => {
    if (step === "purpose") {
      if (!canContinuePurpose) {
        Alert.alert("Select purpose", "Please select what you are giving for.");
        return;
      }
      setStep("amount");
      return;
    }

    if (step === "amount") {
      if (!canContinueAmount) {
        Alert.alert("Amount", "Please enter a valid amount.");
        return;
      }
      setStep("confirm");
      return;
    }
  };

  const goBackStep = () => {
    if (step === "confirm") return setStep("amount");
    if (step === "amount") return setStep("purpose");
  };

  const submit = async () => {
    if (!canContinueAmount) return;

    const apiReady = !!apiBaseUrl && !!churchId;
    const clientRef = reference.trim() || `CHUR-${uid()}`;

    try {
      setSubmitting(true);

      // DEMO + SERVER: write to Postgres via /simulate-payment
      if (demoMode && apiReady) {
        const payload = {
          churchId,
          fundId: selectedFund.id,
          amount: Number(amountNum.toFixed(2)),
          memberName: memberProfile?.name || "",
          memberPhone: memberProfile?.phone || "",
          channel,
        };

        const data = await apiJSON(apiBaseUrl, `/simulate-payment`, {
          method: "POST",
          body: JSON.stringify(payload),
        });

        const r = data?.receipt;

        const t = {
          id: String(data?.transactionId || uid()),
          createdAt: String(r?.createdAt || new Date().toISOString()),
          fundId: selectedFund.id,
          fundName: selectedFund.name,
          amount: payload.amount,
          reference: String(r?.reference || clientRef),
          channel: payload.channel,
          memberName: payload.memberName,
          memberPhone: payload.memberPhone,
          demo: 1,
          behalfName: behalfName || "",
          behalfPhone: behalfPhone || "",
          behalfNote: behalfNote || "",
        };

        onCreateTx(t);
        if (typeof onAfterPayment === "function") onAfterPayment();
        return;
      }

      // LOCAL fallback
      const t = {
        id: uid(),
        createdAt: new Date().toISOString(),
        fundId: selectedFund.id,
        fundName: selectedFund.name,
        amount: Number(amountNum.toFixed(2)),
        reference: clientRef,
        channel,
        memberName: memberProfile?.name || "",
        memberPhone: memberProfile?.phone || "",
        demo: demoMode ? 1 : 0,
        behalfName: behalfName || "",
        behalfPhone: behalfPhone || "",
        behalfNote: behalfNote || "",
      };

      onCreateTx(t);
    } catch (e) {
      Alert.alert("Payment failed", e?.message || "Could not record payment.");
    } finally {
      setSubmitting(false);
    }
  };

  const StepPill = ({ label, active }) => (
    <View style={[s.stepPill, active && s.stepPillActive]}>
      <Text style={[s.stepPillText, active && { opacity: 1 }]}>{label}</Text>
    </View>
  );

  const AmountChip = ({ value }) => {
    const isActive = String(amount) === String(value);
    return (
      <Pressable 
        onPress={() => setQuickAmount(value)} 
        style={({ pressed }) => [
          s.amountChip, 
          isActive && s.amountChipActive,
          pressed && { opacity: 0.7 }
        ]}
      >
        <Text style={[s.amountChipText, isActive && s.amountChipActiveText]}>
          {money(value)}
        </Text>
      </Pressable>
    );
  };

  // Empty state when admin hasn’t enabled any funds yet
  if (activeFunds.length === 0) {
    return (
      <SafeAreaView style={s.safe}>
        <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
          <View style={s.card}>
            <Text style={s.sectionTitle}>Giving is not ready</Text>
            <Text style={s.p}>
              No giving purposes are active yet. Please ask an admin to add or enable funds.
            </Text>
          </View>
        </ScrollView>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        {/* HEADER */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Contribute</Text>
          <Text style={s.p}>Select purpose, enter amount, then confirm.</Text>

          <View style={{ flexDirection: "row", gap: 8, marginTop: 12 }}>
            <StepPill label="1. Purpose" active={step === "purpose"} />
            <StepPill label="2. Amount" active={step === "amount"} />
            <StepPill label="3. Confirm" active={step === "confirm"} />
          </View>
        </View>

        {/* STEP 1: PURPOSE */}
        {step === "purpose" && (
          <View style={s.card}>
            <Text style={s.sectionTitle}>What are you giving for?</Text>
            <Text style={s.p}>Choose a purpose below.</Text>

            <View style={{ marginTop: 12, gap: 10 }}>
              {activeFunds.map((f) => {
                const active = fundId === f.id;
                return (
                  <Pressable
                    key={f.id}
                    onPress={() => setFundId(f.id)}
                    style={[s.fundCard, active && s.fundCardActive]}
                  >
                    <Text style={s.fundCardTitle}>{f.name}</Text>
                    <Text style={s.fundCardSub}>{active ? "Selected" : "Tap to select"}</Text>
                  </Pressable>
                );
              })}
            </View>

            <Pressable onPress={goNext} style={[s.btn, { marginTop: 16 }]}>
              <Text style={s.btnText}>Continue</Text>
            </Pressable>
          </View>
        )}

        {/* STEP 2: AMOUNT */}
        {step === "amount" && (
          <View style={s.card}>
            <Text style={s.sectionTitle}>Enter amount</Text>
            <Text style={s.p}>Purpose: <Text style={{ fontWeight: "900", color: BRAND.colors.text }}>{selectedFund?.name}</Text></Text>

            <Text style={[s.label, { marginTop: 14 }]}>Amount (ZAR)</Text>
            <TextInput
              value={amount}
              onChangeText={(t) => setAmount(t.replace(/[^\d.]/g, ""))}
              keyboardType={Platform.select({ ios: "decimal-pad", android: "numeric" })}
              placeholder="e.g. 200"
              placeholderTextColor={BRAND.colors.textMuted}
              style={s.amountInput}
            />
            {!validAmount && <Text style={s.warn}>Enter a valid amount.</Text>}

            <Text style={[s.label, { marginTop: 10 }]}>Quick amounts</Text>
            <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 10, marginTop: 8 }}>
              <AmountChip value={50} />
              <AmountChip value={100} />
              <AmountChip value={200} />
              <AmountChip value={500} />
              <AmountChip value={1000} />
            </View>

            <Text style={s.label}>Reference (optional)</Text>
            <TextInput
              value={reference}
              onChangeText={setReference}
              placeholder="e.g. Sunday service"
              placeholderTextColor={BRAND.colors.textMuted}
              style={s.input}
            />

            <View style={{ flexDirection: "row", gap: 10, marginTop: 14 }}>
              <Pressable onPress={goBackStep} style={[s.btnSecondary, { flex: 1 }] }>
                <Text style={s.btnText}>Back</Text>
              </Pressable>
              <Pressable
                onPress={goNext}
                disabled={!canContinueAmount}
                style={[s.btn, { flex: 1 }, !canContinueAmount && s.btnDisabled]}
              >
                <Text style={s.btnText}>Continue</Text>
              </Pressable>
            </View>
          </View>
        )}

        {/* STEP 3: CONFIRM */}
        {step === "confirm" && (
          <View style={s.card}>
            <Text style={s.sectionTitle}>Confirm contribution</Text>
            <Text style={s.p}>Please review your details before confirming.</Text>

            <View style={{ marginTop: 12, gap: 8 }}>
              <RowKV k="Purpose" v={selectedFund?.name || ""} strong />
              <RowKV k="Amount" v={money(amountNum)} strong />
              <RowKV k="Channel" v={channel.toUpperCase()} />
              <RowKV k="Reference" v={reference.trim() || "Auto-generated"} />
              {(behalfName || behalfPhone || behalfNote) ? (
                <>
                  {!!behalfName && <RowKV k="For" v={behalfName} />}
                  {!!behalfPhone && <RowKV k="For phone" v={behalfPhone} />}
                  {!!behalfNote && <RowKV k="Note" v={behalfNote} />}
                </>
              ) : null}
              {!!(memberProfile?.name) && <RowKV k="Name" v={memberProfile.name} />}
              {!!(memberProfile?.phone) && <RowKV k="Phone" v={memberProfile.phone} />}
            </View>

            <View style={{ flexDirection: "row", gap: 10, marginTop: 14 }}>
              <Pressable onPress={goBackStep} disabled={submitting} style={[s.btnSecondary, { flex: 1 }] }>
                <Text style={s.btnText}>Back</Text>
              </Pressable>
              <Pressable
                onPress={submit}
                disabled={!canContinueAmount || submitting}
                style={[s.btn, { flex: 1 }, (!canContinueAmount || submitting) && s.btnDisabled]}
              >
                {submitting ? (
                  <ActivityIndicator color={BRAND.colors.text} />
                ) : (
                  <Text style={s.btnText}>Confirm</Text>
                )}
              </Pressable>
            </View>

            <Text style={[s.muted, { marginTop: 10 }]}>You will receive a receipt after confirming.</Text>
          </View>
        )}

        {/* RECEIPT NOTE */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Receipt</Text>
          <Text style={s.p}>After confirming, you’ll get a receipt you can share.</Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}
function ReceiptScreen({ route, navigation, churchName, demoMode, tx }) {
  const id = route?.params?.id;
  const item = tx.find((t) => t.id === id);

  const shareReceipt = async () => {
    if (!item) return;
    const text =
      `churpay receipt\n` +
      `${churchName}\n` +
      `Amount: ${money(item.amount)}\n` +
      `Purpose: ${item.fundName}\n` +
      `Reference: ${item.reference}\n` +
      `Channel: ${item.channel}\n` +
      (item.behalfName ? `For: ${item.behalfName}\n` : "") +
      (item.behalfPhone ? `For phone: ${item.behalfPhone}\n` : "") +
      (item.behalfNote ? `Note: ${item.behalfNote}\n` : "") +
      `Date: ${new Date(item.createdAt).toLocaleString()}\n` +
      (item.memberName ? `Name: ${item.memberName}\n` : "") +
      (item.memberPhone ? `Phone: ${item.memberPhone}\n` : "");

    try {
      await Share.share({ message: text });
    } catch (e) {
      // ignore
    }
  };

  const copy = async () => {
    if (!item) return;
    const txt = `${money(item.amount)} • ${item.fundName} • Ref ${item.reference}`;
    await Clipboard.setStringAsync(txt);
    Alert.alert("Copied", "Receipt summary copied.");
  };

  if (!item) {
    return (
      <SafeAreaView style={s.safe}>
        <View style={[s.card, { marginTop: 16 }]}> 
          <Text style={s.sectionTitle}>Receipt not found</Text>
          <Text style={s.p}>Go back and create a contribution.</Text>
          <Pressable onPress={() => navigation.navigate("Contribute")} style={s.btnSecondary}>
            <Text style={s.btnText}>Contribute</Text>
          </Pressable>
        </View>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={[s.card, { alignItems: "center" }]}>
          <ChurpayLogo height={28} />
          <Text style={[s.sub, { marginTop: 6, textAlign: "center" }]}>{churchName}</Text>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Receipt</Text>
          <View style={{ marginTop: 10, gap: 6 }}>
            <RowKV k="Amount" v={money(item.amount)} strong />
            <RowKV k="Purpose" v={item.fundName} />
            <RowKV k="Reference" v={item.reference} />
            <RowKV k="Channel" v={item.channel} />
            {!!item.behalfName && <RowKV k="For" v={item.behalfName} />}
            {!!item.behalfPhone && <RowKV k="For phone" v={item.behalfPhone} />}
            {!!item.behalfNote && <RowKV k="Note" v={item.behalfNote} />}
            <RowKV k="Date" v={new Date(item.createdAt).toLocaleString()} />
          </View>

          <Pressable onPress={shareReceipt} style={s.btn}>
            <Text style={s.btnText}>Share receipt</Text>
          </Pressable>
          <Pressable onPress={copy} style={s.btnSecondary}>
            <Text style={s.btnText}>Copy summary</Text>
          </Pressable>
        </View>

        <View style={s.row}>
          <NavBtn title="Home" onPress={() => navigation.navigate("MemberHome")} />
          <NavBtn title="History" onPress={() => navigation.navigate("MemberHistory")} />
          <NavBtn title="New" onPress={() => navigation.replace("Contribute")} />
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function MemberHistory({ tx }) {
  const [q, setQ] = useState("");

  const filtered = useMemo(() => {
    const query = q.trim().toLowerCase();
    if (!query) return tx;
    return tx.filter((t) => {
      const hay = `${t.fundName} ${t.reference} ${t.channel} ${t.memberName} ${t.memberPhone}`.toLowerCase();
      return hay.includes(query);
    });
  }, [q, tx]);

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={s.card}>
          <Text style={s.sectionTitle}>History</Text>
          <Text style={s.p}>Search by purpose, reference, or channel.</Text>
          <TextInput
            value={q}
            onChangeText={setQ}
            placeholder="Search…"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />

          {filtered.length === 0 ? (
            <Text style={[s.muted, { marginTop: 10 }]}>No records.</Text>
          ) : (
            filtered.slice(0, 50).map((t) => (
              <View key={t.id} style={s.intent}>
                <View style={{ flex: 1 }}>
                  <Text style={{ color: BRAND.colors.text, fontWeight: "900" }}>{money(t.amount)}</Text>
                  <Text style={s.muted}>{t.fundName}</Text>
                </View>
                <View style={{ alignItems: "flex-end" }}>
                  <Text style={s.muted}>{new Date(t.createdAt).toLocaleString()}</Text>
                  <Text style={s.muted}>Ref: {t.reference}</Text>
                  <Text style={s.muted}>Channel: {t.channel}</Text>
                </View>
              </View>
            ))
          )}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function QRScreen({ demoMode, activeFunds, churchName }) {
  const [fundId, setFundId] = useState(activeFunds[0]?.id || "");

  // If funds load after mount and no fund is selected yet, default to first active fund.
  useEffect(() => {
    if (!fundId && activeFunds?.length) setFundId(activeFunds[0].id);
  }, [activeFunds, fundId]);

  const url = useMemo(() => {
    const base = "https://churpay.com/give";
    const f = encodeURIComponent(fundId || "");
    return `${base}?churchId=${DEFAULT_CHURCH_ID}&fund=${f}`;
  }, [fundId]);

  const copy = async () => {
    await Clipboard.setStringAsync(url);
    Alert.alert("Copied", "Link copied.");
  };

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12, alignItems: "center" }}>
        <View style={[s.card, { width: "100%", alignItems: "center" }]}>
          <Text style={s.sectionTitle}>Share QR</Text>
          <Text style={[s.p, { textAlign: "center" }]}>Use this in-service. People scan and confirm contributions.</Text>

          <View style={[s.qrHeader, { width: "100%" }]}>
            <ChurpayLogo height={24} />
            <View style={{ flex: 1 }}>
              <Text style={{ color: BRAND.colors.text, fontWeight: "900" }}>{churchName}</Text>
              <Text style={s.muted}>Powered by churpay</Text>
            </View>
          </View>

          <View style={{ flexDirection: "row", flexWrap: "wrap", justifyContent: "center", gap: 10, marginTop: 10 }}>
            {activeFunds.map((f) => {
              const active = fundId === f.id;
              return (
                <Pressable key={f.id} onPress={() => setFundId(f.id)} style={[s.pill, active && s.pillActive]}>
                  <Text style={{ color: BRAND.colors.text, fontWeight: "900" }}>{f.name}</Text>
                </Pressable>
              );
            })}
          </View>

          <View style={{ marginTop: 20, width: "100%", alignItems: "center", gap: 8 }}>
            <Text style={[s.label, { alignSelf: "center" }]}>QR Label (for printing)</Text>
            <View style={{ backgroundColor: "rgba(248,250,252,0.04)", padding: 12, borderRadius: 12, width: "100%" }}>
              <Text style={[s.sectionTitle, { textAlign: "center", fontSize: 16 }]}>
                {activeFunds.find(f => f.id === fundId)?.name || "Select a fund"}
              </Text>
              <Text style={[s.muted, { textAlign: "center", marginTop: 8 }]}>Scan to give (enter amount manually)</Text>
            </View>
          </View>

          <View
            style={{
              marginTop: 18,
              padding: 14,
              borderRadius: 16,
              backgroundColor: "rgba(248,250,252,0.06)",
              borderWidth: 1,
              borderColor: BRAND.colors.line,
            }}
          >
            <QRCode value={url} size={220} />
          </View>

          <View style={{ marginTop: 12, width: "100%" }}>
            <Text style={s.label}>Link</Text>
            <Text style={s.code}>{url}</Text>
            <Pressable onPress={copy} style={s.btnSecondary}>
              <Text style={s.btnText}>Copy link</Text>
            </Pressable>
          </View>
        </View>

        <View style={[s.card, { width: "100%" }]}>
          <Text style={s.sectionTitle}>Notes</Text>
          <Text style={s.p}>• Shows adoption + reporting + POS readiness</Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function RowKV({ k, v, strong }) {
  return (
    <View style={{ flexDirection: "row", justifyContent: "space-between", gap: 12 }}>
      <Text style={s.muted}>{k}</Text>
      <Text style={{ color: BRAND.colors.text, fontWeight: strong ? "900" : "700" }}>{v}</Text>
    </View>
  );
}

// ===========================
// ADMIN APP SCREENS
// ===========================

function AdminGate({ navigation, churchName, adminPin, onSuccess }) {
  const [pin, setPin] = useState("");
  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={[s.card, { alignItems: "center" }]}>
          <ChurpayLogo height={40} />
          <Text style={[s.sub, { marginTop: 6 }]}>{churchName}</Text>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Admin access</Text>
          <Text style={s.p}>Enter PIN to access the admin dashboard.</Text>
          <Text style={s.label}>PIN</Text>
          <TextInput
            value={pin}
            onChangeText={(t) => setPin(t.replace(/[^0-9]/g, "").slice(0, 6))}
            keyboardType={Platform.select({ ios: "number-pad", android: "numeric" })}
            placeholder="Enter PIN"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
            secureTextEntry
          />
          <Pressable
            onPress={() => {
              const entered = pin || "";
              if (entered !== adminPin) {
                Alert.alert("Access denied", "Incorrect admin PIN.");
                return;
              }
              onSuccess();
            }}
            style={s.btn}
          >
            <Text style={s.btnText}>Enter</Text>
          </Pressable>
          <Text style={[s.muted, { marginTop: 10 }]}>Admin PIN: {adminPin}</Text>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Security note</Text>
          <Text style={s.p}>This is a pilot local PIN. Production will use secure authentication + roles.</Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function AdminHome({ navigation, churchName, serverTotals }) {
  const rows = serverTotals?.totals || [];
  const grand = Number(serverTotals?.grandTotal || 0);

  const top3 = rows
    .slice()
    .sort((a, b) => Number(b.total) - Number(a.total))
    .slice(0, 3);

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ paddingVertical: 16, gap: 12 }}>
        <View style={[s.hero, s.card]}>
          <View style={{ flexDirection: "row", alignItems: "center", gap: 12 }}>
            <ChurpayMark size={44} />
            <View style={{ flex: 1 }}>
              <Text style={s.heroTitle}>Admin Dashboard</Text>
              <Text style={s.heroSub}>{churchName}</Text>
            </View>
          </View>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 14, flexWrap: "wrap" }}>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Grand Total (server)</Text>
              <Text style={s.statV}>{money(grand)}</Text>
              <Text style={s.statSub}>All funds combined</Text>
            </View>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Active Funds</Text>
              <Text style={s.statV}>{String(rows.length || 0)}</Text>
              <Text style={s.statSub}>Configured purposes</Text>
            </View>
          </View>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
            <Pressable style={[s.btn, { flex: 1, minWidth: 120 }]} onPress={() => navigation.navigate("AdminTx")}>
              <Text style={s.btnText}>Transactions</Text>
            </Pressable>
            <Pressable style={[s.btnSecondary, { flex: 1, minWidth: 120 }]} onPress={() => navigation.navigate("AdminFunds")}>
              <Text style={s.btnText}>Funds</Text>
            </Pressable>
            <Pressable style={[s.btnSecondary, { flex: 1, minWidth: 120 }]} onPress={() => navigation.navigate("QR")}>
              <Text style={s.btnText}>QR Codes</Text>
            </Pressable>
          </View>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Top funds</Text>
          <Text style={s.p}>Highest totals (server).</Text>
          <View style={{ marginTop: 10, gap: 10 }}>
            {top3.length === 0 ? (
              <Text style={s.muted}>No contributions yet.</Text>
            ) : (
              top3.map((r) => (
                <View key={r.code} style={s.rankRow}>
                  <View style={{ flex: 1 }}>
                    <Text style={s.rankName}>{r.name}</Text>
                    <Text style={s.muted}>{r.code}</Text>
                  </View>
                  <Text style={s.rankValue}>{money(r.total)}</Text>
                </View>
              ))
            )}
          </View>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Operations</Text>
          <View style={{ flexDirection: "row", gap: 10, flexWrap: "wrap", marginTop: 10 }}>
            <Pressable onPress={() => navigation.navigate("POS")} style={s.actionCard}>
              <Text style={s.actionTitle}>POS Mode</Text>
              <Text style={s.actionBody}>Fast service collection simulation</Text>
            </Pressable>
            <Pressable onPress={() => navigation.navigate("AdminReports")} style={s.actionCard}>
              <Text style={s.actionTitle}>Reports</Text>
              <Text style={s.actionBody}>Share totals & exports</Text>
            </Pressable>
            <Pressable onPress={() => navigation.navigate("AdminSettings")} style={s.actionCard}>
              <Text style={s.actionTitle}>Settings</Text>
              <Text style={s.actionBody}>Church & admin PIN</Text>
            </Pressable>
          </View>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function AdminFunds({ funds, setFunds, apiBaseUrl, churchId, onChanged }) {
  const [newFund, setNewFund] = useState("");

  const toggle = async (f) => {
    try {
      const data = await apiJSON(apiBaseUrl, `/funds/${f.id}`, {
        method: "PATCH",
        body: JSON.stringify({ churchId, active: !f.active }),
      });
      const updated = data?.fund;
      if (updated) {
        setFunds((prev) => prev.map((x) => (x.id === f.id ? { ...x, active: !!updated.active, name: updated.name, code: updated.code } : x)));
      }
      if (typeof onChanged === "function") onChanged();
    } catch (e) {
      Alert.alert("Update failed", e?.message || "Could not update fund.");
    }
  };

  const rename = async (f, name) => {
    try {
      const data = await apiJSON(apiBaseUrl, `/funds/${f.id}`, {
        method: "PATCH",
        body: JSON.stringify({ churchId, name }),
      });
      const updated = data?.fund;
      if (updated) {
        setFunds((prev) => prev.map((x) => (x.id === f.id ? { ...x, name: updated.name, active: !!updated.active, code: updated.code } : x)));
      }
      if (typeof onChanged === "function") onChanged();
    } catch (e) {
      Alert.alert("Rename failed", e?.message || "Could not rename fund.");
    }
  };

  const remove = (id) => {
    Alert.alert("Remove fund", "Remove this fund from the list?", [
      { text: "Cancel", style: "cancel" },
      {
        text: "Remove",
        style: "destructive",
        onPress: () => setFunds((prev) => prev.filter((f) => f.id !== id)),
      },
    ]);
  };

  const addFund = async () => {
    const name = newFund.trim();
    if (!name) return;
    try {
      const data = await apiJSON(apiBaseUrl, `/funds`, {
        method: "POST",
        body: JSON.stringify({ churchId, name, active: true }),
      });
      const created = data?.fund;
      if (created) {
        setFunds((prev) => [{ id: created.id, name: created.name, active: !!created.active, code: created.code }, ...prev]);
        setNewFund("");
      }
      if (typeof onChanged === "function") onChanged();
    } catch (e) {
      Alert.alert("Create failed", e?.message || "Could not create fund.");
    }
  };

  const activeCount = funds.filter((f) => f.active).length;

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        {/* HEADER */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Funds</Text>
          <Text style={s.p}>Enable/disable purposes shown to members and POS.</Text>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Total funds</Text>
              <Text style={s.statV}>{String(funds.length)}</Text>
              <Text style={s.statSub}>Configured purposes</Text>
            </View>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Active funds</Text>
              <Text style={s.statV}>{String(activeCount)}</Text>
              <Text style={s.statSub}>Visible to members</Text>
            </View>
          </View>
        </View>

        {/* ADD NEW */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Add new fund</Text>
          <Text style={s.p}>Example: "Youth", "Thanksgiving", "Outreach".</Text>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 10 }}>
            <TextInput
              value={newFund}
              onChangeText={setNewFund}
              placeholder="Fund name"
              placeholderTextColor={BRAND.colors.textMuted}
              style={[s.input, { flex: 1, marginTop: 0 }]}
            />
            <Pressable onPress={addFund} style={[s.btn, { paddingHorizontal: 16, marginTop: 0 }]}>
              <Text style={s.btnText}>Add</Text>
            </Pressable>
          </View>
        </View>

        {/* FUNDS LIST */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Manage funds</Text>
          <Text style={s.p}>Tap a fund to edit. Use Enable/Disable to control visibility.</Text>

          {funds.length === 0 ? (
            <View style={{ marginTop: 12 }}>
              <Text style={s.muted}>No funds yet. Add one above.</Text>
            </View>
          ) : (
            <View style={{ marginTop: 12, gap: 12 }}>
              {funds
                .slice()
                .sort((a, b) => String(a.name || "").localeCompare(String(b.name || "")))
                .map((f) => (
                  <FundCard
                    key={f.id}
                    fund={f}
                    onToggle={() => toggle(f)}
                    onRename={(name) => rename(f, name)}
                    onRemove={() => remove(f.id)}
                  />
                ))}
            </View>
          )}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function FundCard({ fund, onToggle, onRename, onRemove }) {
  const [editing, setEditing] = useState(false);
  const [name, setName] = useState(fund.name);

  const save = () => {
    const next = name.trim();
    if (!next) return;
    onRename(next);
    setEditing(false);
  };

  return (
    <Pressable
      onPress={() => setEditing(true)}
      style={[s.fundManageCard, fund.active && s.fundManageCardActive]}
    >
      <View style={{ flexDirection: "row", alignItems: "center", gap: 10 }}>
        <View style={[s.iconBadge, fund.active ? s.iconBadgeOn : s.iconBadgeOff]}>
          <Ionicons name={fund.active ? "checkmark" : "pause"} size={16} color={BRAND.colors.text} />
        </View>

        <View style={{ flex: 1 }}>
          {editing ? (
            <TextInput
              value={name}
              onChangeText={setName}
              placeholder="Fund name"
              placeholderTextColor={BRAND.colors.textMuted}
              style={[s.input, { marginTop: 0 }]}
              autoFocus
            />
          ) : (
            <>
              <Text style={s.fundManageTitle}>{fund.name}</Text>
              <Text style={s.fundManageSub}>{fund.active ? "Active (visible)" : "Inactive (hidden)"}</Text>
            </>
          )}
        </View>
      </View>

      <View style={{ flexDirection: "row", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
        {editing ? (
          <>
            <Pressable onPress={save} style={[s.tinyBtn, s.tinyBtnPrimary]}>
              <Ionicons name="save" size={16} color={BRAND.colors.text} />
              <Text style={s.tinyBtnText}>Save</Text>
            </Pressable>
            <Pressable onPress={() => setEditing(false)} style={s.tinyBtn}>
              <Ionicons name="close" size={16} color={BRAND.colors.text} />
              <Text style={s.tinyBtnText}>Cancel</Text>
            </Pressable>
          </>
        ) : (
          <Pressable onPress={onToggle} style={[s.tinyBtn, fund.active && s.tinyBtnPrimary]}>
            <Ionicons name={fund.active ? "eye-off" : "eye"} size={16} color={BRAND.colors.text} />
            <Text style={s.tinyBtnText}>{fund.active ? "Disable" : "Enable"}</Text>
          </Pressable>
        )}

        <Pressable onPress={onRemove} style={[s.tinyBtn, s.tinyBtnDanger]}>
          <Ionicons name="trash" size={16} color={BRAND.colors.text} />
          <Text style={s.tinyBtnText}>Remove</Text>
        </Pressable>
      </View>

      {!editing && <Text style={[s.muted, { marginTop: 10 }]}>Tap card to edit name.</Text>}
    </Pressable>
  );
}

function AdminTransactions({ tx, onOpen, onClear, onRefresh }) {
  const [q, setQ] = useState("");
  const [filterChannel, setFilterChannel] = useState("all");
  const [loading, setLoading] = useState(true);
  const [remoteTx, setRemoteTx] = useState([]);
  const [err, setErr] = useState("");

  const refresh = async () => {
    try {
      setLoading(true);
      setErr("");
      const data = await apiGetTransactions({ limit: 80 });
      setRemoteTx(data.transactions || []);
    } catch (e) {
      setErr(e.message || "Failed to load");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { refresh(); }, []);

  const channels = useMemo(() => ["all", "member", "qr", "pos", "request"], []);

  const filtered = useMemo(() => {
    const query = q.trim().toLowerCase();
    const base = remoteTx.filter((t) => {
      const hay = `${t.fundName} ${t.reference} ${t.channel} ${t.memberName} ${t.memberPhone}`.toLowerCase();
      const matchesQ = !query || hay.includes(query);
      const ch = String(t.channel || "").toLowerCase();
      const matchesCh = filterChannel === "all" || ch === filterChannel;
      return matchesQ && matchesCh;
    });

    return base.slice().sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt));
  }, [q, remoteTx, filterChannel]);

  const totalsX = useMemo(() => {
    const count = filtered.length;
    const amount = filtered.reduce((s, t) => s + Number(t.amount || 0), 0);
    return { count, amount };
  }, [filtered]);

  const ChannelChip = ({ value }) => {
    const active = filterChannel === value;
    return (
      <Pressable onPress={() => setFilterChannel(value)} style={[s.chip, active && s.chipActive]}>
        <Text style={[s.chipText, active && { opacity: 1 }]}>{value.toUpperCase()}</Text>
      </Pressable>
    );
  };

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={s.card}>
          <Text style={s.sectionTitle}>Transactions</Text>
          <Text style={s.p}>Search, filter, then tap a row to view details.</Text>
          
          {err ? (
            <View style={[s.card, { backgroundColor: "#fee", marginTop: 10, borderColor: "#c00", borderWidth: 1 }]}>
              <Text style={{ color: "#c00", fontWeight: "500" }}>Error: {err}</Text>
            </View>
          ) : null}

          {loading ? (
            <Text style={[s.muted, { marginTop: 10 }]}>Loading transactions…</Text>
          ) : (
            <Pressable
              onPress={refresh}
              style={[s.btnSecondary, { marginTop: 10 }]}
            >
              <Text style={s.btnText}>Refresh from server</Text>
            </Pressable>
          )}

          <View style={{ flexDirection: "row", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Records</Text>
              <Text style={s.statV}>{String(totalsX.count)}</Text>
              <Text style={s.statSub}>Filtered</Text>
            </View>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Total</Text>
              <Text style={s.statV}>{money(totalsX.amount)}</Text>
              <Text style={s.statSub}>Filtered amount</Text>
            </View>
          </View>

          <TextInput
            value={q}
            onChangeText={setQ}
            placeholder="Search by purpose, reference, name, phone…"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />

          <Text style={[s.label, { marginTop: 10 }]}>Channel</Text>
          <ScrollView horizontal showsHorizontalScrollIndicator={false} contentContainerStyle={{ gap: 10 }}>
            {channels.map((c) => (
              <ChannelChip key={c} value={c} />
            ))}
          </ScrollView>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>List</Text>

          {filtered.length === 0 ? (
            <Text style={[s.muted, { marginTop: 10 }]}>No records.</Text>
          ) : (
            <View style={{ marginTop: 12, gap: 10 }}>
              {filtered.slice(0, 120).map((t) => (
                <Pressable key={t.id} onPress={() => onOpen(t.id)} style={s.txCard}>
                  <View style={{ flexDirection: "row", alignItems: "center", gap: 12 }}>
                    <View style={s.txIcon}>
                      <Ionicons
                        name={
                          String(t.channel || "").toLowerCase() === "pos"
                            ? "card"
                            : String(t.channel || "").toLowerCase() === "qr"
                            ? "qr-code"
                            : String(t.channel || "").toLowerCase() === "request"
                            ? "gift"
                            : "wallet"
                        }
                        size={18}
                        color={BRAND.colors.text}
                      />
                    </View>

                    <View style={{ flex: 1 }}>
                      <Text style={s.txAmount}>{money(t.amount)}</Text>
                      <Text style={s.txMeta}>{t.fundName} • Ref {t.reference}</Text>

                      {!!(t.memberName || t.memberPhone) && (
                        <Text style={[s.muted, { marginTop: 6 }]}>
                          {t.memberName ? t.memberName : ""}
                          {t.memberName && t.memberPhone ? " • " : ""}
                          {t.memberPhone ? maskPhone(t.memberPhone) : ""}
                        </Text>
                      )}
                    </View>

                    <View style={{ alignItems: "flex-end", gap: 8 }}>
                      <Text style={s.txDate}>{fmtDateTime(t.createdAt)}</Text>
                      <View style={[s.chipMini, s.chipMiniOn]}>
                        <Text style={s.chipMiniText}>{String(t.channel || "").toUpperCase()}</Text>
                      </View>
                    </View>
                  </View>

                  {!!(t.memberName || t.memberPhone) && (
                    <View style={{ marginTop: 10 }}>
                      <Text style={s.muted}>
                        {t.memberName ? t.memberName : ""}
                        {t.memberName && t.memberPhone ? " • " : ""}
                        {t.memberPhone ? t.memberPhone : ""}
                      </Text>
                    </View>
                  )}
                </Pressable>
              ))}
            </View>
          )}

          {PILOT_MODE ? (
            <View style={[s.btnDanger, { marginTop: 12, opacity: 0.5 }]}>
              <Text style={s.btnText}>Clear local data (disabled in pilot)</Text>
            </View>
          ) : (
            <Pressable
              onPress={() =>
                Alert.alert("Clear data", "Clear all local transactions?", [
                  { text: "Cancel", style: "cancel" },
                  { text: "Clear", style: "destructive", onPress: onClear },
                ])
              }
              style={[s.btnDanger, { marginTop: 12 }]}
            >
              <Text style={s.btnText}>Clear local data</Text>
            </Pressable>
          )}
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function AdminTxDetail({ route, tx }) {
  const id = route?.params?.id;
  const item = tx.find((t) => t.id === id);

  const shareRow = async () => {
    if (!item) return;
    const text =
      `Transaction\n` +
      `Amount: ${money(item.amount)}\n` +
      `Purpose: ${item.fundName}\n` +
      `Reference: ${item.reference}\n` +
      `Channel: ${item.channel}\n` +
      `Date: ${new Date(item.createdAt).toLocaleString()}\n` +
      (item.memberName ? `Name: ${item.memberName}\n` : "") +
      (item.memberPhone ? `Phone: ${item.memberPhone}\n` : "");
    await Share.share({ message: text });
  };

  if (!item) {
    return (
      <SafeAreaView style={s.safe}>
        <View style={[s.card, { marginTop: 16 }]}>
          <Text style={s.sectionTitle}>Not found</Text>
          <Text style={s.p}>Transaction not found on this device.</Text>
        </View>
      </SafeAreaView>
    );
  }

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={s.card}>
          <Text style={s.sectionTitle}>Transaction</Text>
          <View style={{ marginTop: 10, gap: 6 }}>
            <RowKV k="Amount" v={money(item.amount)} strong />
            <RowKV k="Purpose" v={item.fundName} />
            <RowKV k="Reference" v={item.reference} />
            <RowKV k="Channel" v={item.channel} />
            <RowKV k="Date" v={new Date(item.createdAt).toLocaleString()} />
            {!!item.memberName && <RowKV k="Name" v={item.memberName} />}
            {!!item.memberPhone && <RowKV k="Phone" v={item.memberPhone} />}
          </View>

          <Pressable onPress={shareRow} style={s.btnSecondary}>
            <Text style={s.btnText}>Share</Text>
          </Pressable>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function AdminReports({ churchName, tx, totals }) {
  // ====== Date range picker (Last 7 days / This month / Custom) ======
  const [rangePreset, setRangePreset] = useState("thisMonth"); // last7 | thisMonth | custom
  const [customStart, setCustomStart] = useState(""); // YYYY-MM-DD
  const [customEnd, setCustomEnd] = useState(""); // YYYY-MM-DD

  const parseYMD = (s) => {
    const m = String(s || "").trim().match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (!m) return null;
    const d = new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
    if (Number.isNaN(d.getTime())) return null;
    d.setHours(0, 0, 0, 0);
    return d;
  };

  const range = useMemo(() => {
    const now = new Date();
    const end = new Date(now);
    end.setHours(23, 59, 59, 999);

    if (rangePreset === "last7") {
      const start = new Date(now);
      start.setDate(start.getDate() - 6);
      start.setHours(0, 0, 0, 0);
      return { start, end, label: "Last 7 days" };
    }

    if (rangePreset === "thisMonth") {
      const start = new Date(now.getFullYear(), now.getMonth(), 1);
      start.setHours(0, 0, 0, 0);
      return { start, end, label: "This month" };
    }

    // custom
    const start = parseYMD(customStart) || new Date(now.getFullYear(), now.getMonth(), 1);
    const endCustom = parseYMD(customEnd);
    const end2 = endCustom
      ? new Date(endCustom.getFullYear(), endCustom.getMonth(), endCustom.getDate(), 23, 59, 59, 999)
      : end;

    return { start, end: end2, label: "Custom" };
  }, [rangePreset, customStart, customEnd]);

  const txInRange = useMemo(() => {
    const s = range.start.getTime();
    const e = range.end.getTime();
    return (tx || []).filter((t) => {
      const d = new Date(t.createdAt);
      const ts = d.getTime();
      return !Number.isNaN(ts) && ts >= s && ts <= e;
    });
  }, [tx, range]);

  const totalsInRange = useMemo(() => {
    const total = txInRange.reduce((sum, t) => sum + Number(t.amount || 0), 0);
    const count = txInRange.length;
    return { total, count };
  }, [txInRange]);

  const exportCSV = async () => {
    const csv = toCSV(txInRange);
    await Clipboard.setStringAsync(csv);
    try {
      await Share.share({ message: csv });
    } catch (e) {}
    Alert.alert("Exported", "CSV copied to clipboard and opened for sharing.");
  };

  const shareSummary = async () => {
    const summaryText =
      `churpay report\n` +
      `${churchName}\n` +
      `Range: ${range.label} (${range.start.toLocaleDateString()} - ${range.end.toLocaleDateString()})\n` +
      `Total: ${money(totalsInRange.total)}\n` +
      `Records: ${totalsInRange.count}\n`;

    await Share.share({ message: summaryText });
  };

  const [exportingPdf, setExportingPdf] = useState(false);

  const escapeHtml = (s) =>
    String(s ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");

  const formatDate = (d) => {
    try {
      return new Date(d).toLocaleDateString(undefined, {
        year: "numeric",
        month: "short",
        day: "2-digit",
      });
    } catch {
      return String(d);
    }
  };

  // --- Monthly trend (last 6 months) ---
  const monthlyTrend = useMemo(() => {
    const now = new Date();
    const months = [];
    for (let i = 5; i >= 0; i--) {
      const d = new Date(now.getFullYear(), now.getMonth() - i, 1);
      months.push({
        key: `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}`,
        date: d,
        total: 0,
      });
    }

    const index = new Map(months.map((m) => [m.key, m]));

    for (const t of txInRange) {
      const created = new Date(t.createdAt);
      if (Number.isNaN(created.getTime())) continue;
      const k = `${created.getFullYear()}-${String(created.getMonth() + 1).padStart(2, "0")}`;
      const bucket = index.get(k);
      if (bucket) bucket.total += Number(t.amount || 0);
    }

    return months;
  }, [txInRange]);

  // --- Top donors (by name) ---
  const topDonors = useMemo(() => {
    const map = new Map();
    for (const t of txInRange) {
      const name = String(t.memberName || "").trim();
      if (!name) continue;
      map.set(name, (map.get(name) || 0) + Number(t.amount || 0));
    }
    return Array.from(map.entries())
      .map(([name, total]) => ({ name, total }))
      .sort((a, b) => b.total - a.total)
      .slice(0, 5);
  }, [txInRange]);

  // --- Top funds (by total) ---
  const byFundRows = useMemo(() => {
    const map = new Map();
    for (const t of txInRange) {
      const key = t.fundName || "Unknown";
      map.set(key, (map.get(key) || 0) + Number(t.amount || 0));
    }
    return Array.from(map.entries())
      .map(([name, total]) => ({ name, total }))
      .sort((a, b) => b.total - a.total);
  }, [txInRange]);

  const exportLeadershipPackPDF = async () => {
    try {
      setExportingPdf(true);

      const logoDataUri = await getLogoDataUri();

      const rangeStart = range.start;
      const rangeEnd = range.end;

      const topFundsHtml = (byFundRows || [])
        .slice(0, 8)
        .map(
          (r, idx) =>
            `<tr>
              <td class="rank">${idx + 1}</td>
              <td>${escapeHtml(r.name)}</td>
              <td class="num">${escapeHtml(money(r.total))}</td>
            </tr>`
        )
        .join("");

      const topDonorsHtml = (topDonors || [])
        .slice(0, 5)
        .map(
          (d, idx) =>
            `<tr>
              <td class="rank">${idx + 1}</td>
              <td>${escapeHtml(d.name)}</td>
              <td class="num">${escapeHtml(money(d.total))}</td>
            </tr>`
        )
        .join("");

      const trendHtml = (monthlyTrend || [])
        .map((m) => {
          const label = m?.date
            ? m.date.toLocaleString(undefined, { month: "short" })
            : escapeHtml(m?.key);
          return `<tr>
            <td>${escapeHtml(label)}</td>
            <td class="num">${escapeHtml(money(m.total))}</td>
          </tr>`;
        })
        .join("");

      const html = `
<!doctype html>
<html>
<head>
<meta charset="utf-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Leadership Pack</title>
<style>
  * { box-sizing: border-box; }
  body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
    margin: 0;
    padding: 22px;
    background: #070A12;
    color: #F8FAFC;
  }
  .wrap { max-width: 900px; margin: 0 auto; }
  .card {
    background: rgba(248,250,252,0.06);
    border: 1px solid rgba(248,250,252,0.10);
    border-radius: 18px;
    padding: 16px;
    margin-bottom: 12px;
  }
  .brandRow { display:flex; align-items:center; gap:12px; }
  .logo { height: 38px; width:auto; object-fit:contain; }
  .title { margin:0; font-size:18px; font-weight:900; }
  .sub { margin-top:4px; color: rgba(248,250,252,0.72); font-size:12px; }
  .row { display:flex; gap:12px; flex-wrap:wrap; }
  .tile { width:48%; min-width:240px; }
  .k { font-size:11px; color: rgba(248,250,252,0.72); font-weight:800; }
  .v { font-size:18px; font-weight:900; margin-top:6px; }
  .note { font-size:11px; color: rgba(248,250,252,0.60); margin-top:6px; }
  h2 { font-size:13px; margin:0 0 10px; font-weight:900; }
  table { width:100%; border-collapse:collapse; }
  th, td { border-bottom: 1px solid rgba(248,250,252,0.10); padding:10px 8px; font-size:12px; }
  th { text-align:left; font-size:11px; color: rgba(248,250,252,0.70); }
  .num { text-align:right; font-weight:800; }
  .rank { width:28px; text-align:center; font-weight:900; color: rgba(248,250,252,0.85); }
  .twoCol { display:flex; gap:12px; flex-wrap:wrap; }
  .col { flex:1; min-width:300px; }
  .sigWrap { display:flex; gap:20px; margin-top:18px; }
  .sig { flex:1; }
  .sigLine { height:1px; background: rgba(248,250,252,0.35); margin-top:34px; }
  .sigLabel { font-size:11px; color: rgba(248,250,252,0.70); margin-top:6px; }
  .footer { margin-top:12px; font-size:10px; color: rgba(248,250,252,0.55); }
  .pill {
    display:inline-block;
    padding:6px 10px;
    border-radius:999px;
    background: rgba(34,211,238,0.18);
    border: 1px solid rgba(34,211,238,0.45);
    font-size:11px;
    font-weight:900;
  }
</style>
</head>
<body>
  <div class="wrap">

    <div class="card">
      <div class="brandRow">
        ${logoDataUri ? `<img class="logo" src="${logoDataUri}" />` : `<div class="pill">CHURPAY</div>`}
        <div style="flex:1">
          <div class="title">${escapeHtml(churchName)} — Leadership Pack</div>
          <div class="sub">Range: ${escapeHtml(range.label)} • ${escapeHtml(formatDate(rangeStart))} – ${escapeHtml(formatDate(rangeEnd))}</div>
          <div class="sub">Generated: ${escapeHtml(formatDate(new Date()))}</div>
        </div>
        <div class="pill">Premium Report</div>
      </div>
    </div>

    <div class="card">
      <div class="row">
        <div class="tile">
          <div class="k">Total (selected range)</div>
          <div class="v">${escapeHtml(money(totalsInRange.total))}</div>
          <div class="note">Records: ${escapeHtml(String(totalsInRange.count))}</div>
        </div>
        <div class="tile">
          <div class="k">All time (device)</div>
          <div class="v">${escapeHtml(money(totals.total))}</div>
          <div class="note">Transactions: ${escapeHtml(String(totals.count))}</div>
        </div>
      </div>
    </div>

    <div class="card">
      <h2>Monthly trend (last 6 months)</h2>
      <table>
        <thead>
          <tr><th>Month</th><th class="num">Total</th></tr>
        </thead>
        <tbody>
          ${trendHtml || `<tr><td colspan="2">No data</td></tr>`}
        </tbody>
      </table>
    </div>

    <div class="twoCol">
      <div class="card col">
        <h2>Top funds</h2>
        <table>
          <thead><tr><th class="rank">#</th><th>Fund</th><th class="num">Total</th></tr></thead>
          <tbody>
            ${topFundsHtml || `<tr><td colspan="3">No data</td></tr>`}
          </tbody>
        </table>
      </div>

      <div class="card col">
        <h2>Top donors</h2>
        <div class="note">Only includes donors where a name was captured.</div>
        <table>
          <thead><tr><th class="rank">#</th><th>Name</th><th class="num">Total</th></tr></thead>
          <tbody>
            ${topDonorsHtml || `<tr><td colspan="3">No donor names captured</td></tr>`}
          </tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <div class="sigWrap">
        <div class="sig">
          <div class="sigLine"></div>
          <div class="sigLabel">Prepared by (Name & Signature)</div>
        </div>
        <div class="sig">
          <div class="sigLine"></div>
          <div class="sigLabel">Approved by (Name & Signature)</div>
        </div>
      </div>

      <div class="footer">
        Note: This leadership pack is generated from device-recorded transactions in this demo build. Production should use secure server totals.
      </div>
    </div>

  </div>
</body>
</html>`;

      const file = await Print.printToFileAsync({ html, base64: false });

      if (await Sharing.isAvailableAsync()) {
        await Sharing.shareAsync(file.uri, {
          mimeType: "application/pdf",
          dialogTitle: "Share Leadership Pack",
          UTI: "com.adobe.pdf",
        });
      } else {
        await Share.share({ message: `Leadership Pack generated: ${file.uri}` });
      }

      Alert.alert("Done", "Leadership Pack PDF generated.");
    } catch (e) {
      Alert.alert("PDF export failed", e?.message || "Could not generate PDF.");
    } finally {
      setExportingPdf(false);
    }
  };

  const trendMax = Math.max(1, ...monthlyTrend.map((m) => m.total));
  const max = Math.max(1, ...byFundRows.map((r) => r.total));

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={[s.card, { alignItems: "center" }]}>
          <ChurpayLogo height={28} />
          <Text style={[s.sub, { marginTop: 6 }]}>{churchName}</Text>
        </View>

        {/* DATE RANGE */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Date range</Text>
          <Text style={s.p}>Choose a range for reports, exports, and the Leadership Pack.</Text>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
            <Pressable onPress={() => setRangePreset("last7")} style={[s.chip, rangePreset === "last7" && s.chipActive]}>
              <Text style={[s.chipText, rangePreset === "last7" && { opacity: 1 }]}>LAST 7 DAYS</Text>
            </Pressable>

            <Pressable onPress={() => setRangePreset("thisMonth")} style={[s.chip, rangePreset === "thisMonth" && s.chipActive]}>
              <Text style={[s.chipText, rangePreset === "thisMonth" && { opacity: 1 }]}>THIS MONTH</Text>
            </Pressable>

            <Pressable onPress={() => setRangePreset("custom")} style={[s.chip, rangePreset === "custom" && s.chipActive]}>
              <Text style={[s.chipText, rangePreset === "custom" && { opacity: 1 }]}>CUSTOM</Text>
            </Pressable>
          </View>

          {rangePreset === "custom" && (
            <View style={{ marginTop: 12 }}>
              <Text style={s.label}>Start (YYYY-MM-DD)</Text>
              <TextInput
                value={customStart}
                onChangeText={setCustomStart}
                placeholder="2026-02-01"
                placeholderTextColor={BRAND.colors.textMuted}
                style={[s.input, { marginTop: 0 }]}
              />
              <Text style={s.label}>End (YYYY-MM-DD)</Text>
              <TextInput
                value={customEnd}
                onChangeText={setCustomEnd}
                placeholder="2026-02-28"
                placeholderTextColor={BRAND.colors.textMuted}
                style={[s.input, { marginTop: 0 }]}
              />
              <Text style={[s.muted, { marginTop: 8 }]}>Tip: If end date is blank, it uses today.</Text>
            </View>
          )}

          <View style={{ marginTop: 12 }}>
            <Text style={s.muted}>
              Active range: <Text style={{ fontWeight: "900", color: BRAND.colors.text }}>{range.label}</Text> •{" "}
              {range.start.toLocaleDateString()} – {range.end.toLocaleDateString()}
            </Text>
          </View>
        </View>

        {/* SUMMARY */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Summary</Text>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Today</Text>
              <Text style={s.statV}>{money(totals.today)}</Text>
              <Text style={s.statSub}>Device totals</Text>
            </View>
            <View style={s.statTileWide}>
              <Text style={s.statK}>This month</Text>
              <Text style={s.statV}>{money(totals.month)}</Text>
              <Text style={s.statSub}>Device totals</Text>
            </View>
            <View style={s.statTileWide}>
              <Text style={s.statK}>All time</Text>
              <Text style={s.statV}>{money(totals.total)}</Text>
              <Text style={s.statSub}>Device totals</Text>
            </View>
            <View style={s.statTileWide}>
              <Text style={s.statK}>Records</Text>
              <Text style={s.statV}>{String(totals.count)}</Text>
              <Text style={s.statSub}>Transactions</Text>
            </View>
          </View>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 12 }}>
            <Pressable onPress={shareSummary} style={[s.btnSecondary, { flex: 1 }]}>
              <Text style={s.btnText}>Share summary</Text>
            </Pressable>
            <Pressable onPress={exportCSV} style={[s.btn, { flex: 1 }]}>
              <Text style={s.btnText}>Export CSV</Text>
            </Pressable>
          </View>
        </View>

        {/* MONTHLY TREND */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Monthly trend</Text>
          <Text style={s.p}>Last 6 months (device data).</Text>

          <View style={{ flexDirection: "row", gap: 10, marginTop: 12, flexWrap: "wrap" }}>
            {monthlyTrend.map((m) => {
              const pct = Math.max(0.06, m.total / trendMax);
              const label = m.date.toLocaleString(undefined, { month: "short" });
              const year = String(m.date.getFullYear());

              return (
                <View key={m.key} style={s.trendTile}>
                  <Text style={s.trendMonth}>{label} {year}</Text>
                  <Text style={s.trendValue}>{money(m.total)}</Text>
                  <View style={s.trendTrack}>
                    <View style={[s.trendFill, { width: `${Math.round(pct * 100)}%` }]} />
                  </View>
                </View>
              );
            })}
          </View>
        </View>

        {/* TOP DONORS */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Top donors</Text>
          <Text style={s.p}>Based on names captured on receipts (device data).</Text>

          {topDonors.length === 0 ? (
            <Text style={[s.muted, { marginTop: 10 }]}>No donor names captured yet.</Text>
          ) : (
            <View style={{ marginTop: 12, gap: 10 }}>
              {topDonors.map((d, idx) => (
                <View key={d.name} style={s.donorRow}>
                  <View style={s.donorRank}>
                    <Text style={s.donorRankText}>{idx + 1}</Text>
                  </View>

                  <View style={{ flex: 1 }}>
                    <Text style={s.donorName} numberOfLines={1}>{d.name}</Text>
                    <Text style={s.muted}>Total</Text>
                  </View>

                  <Text style={s.donorAmount}>{money(d.total)}</Text>
                </View>
              ))}
            </View>
          )}

          <Text style={[s.muted, { marginTop: 10 }]}>
            Tip: ask members to add their name in Profile so it appears here.
          </Text>
        </View>

        {/* BREAKDOWN */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Breakdown by fund</Text>
          <Text style={s.p}>Simple visual (device data).</Text>

          {byFundRows.length === 0 ? (
            <Text style={[s.muted, { marginTop: 10 }]}>No records yet.</Text>
          ) : (
            <View style={{ marginTop: 12, gap: 10 }}>
              {byFundRows.slice(0, 8).map((r) => {
                const pct = Math.max(0.06, r.total / max);
                return (
                  <View key={r.name} style={s.barRow}>
                    <View style={{ flexDirection: "row", alignItems: "center", gap: 10 }}>
                      <Ionicons name="layers" size={16} color={BRAND.colors.text} />
                      <Text style={s.barName} numberOfLines={1}>{r.name}</Text>
                      <Text style={s.barValue}>{money(r.total)}</Text>
                    </View>
                    <View style={s.barTrack}>
                      <View style={[s.barFill, { width: `${Math.round(pct * 100)}%` }]} />
                    </View>
                  </View>
                );
              })}
            </View>
          )}
        </View>

        {/* LEADERSHIP PACK (PDF) */}
        <View style={s.card}>
          <Text style={s.sectionTitle}>Leadership Pack</Text>
          <Text style={s.p}>
            Branded PDF for leadership: date range, monthly trend, top funds, top donors, and signatures.
          </Text>
          <Pressable
            onPress={exportLeadershipPackPDF}
            disabled={exportingPdf}
            style={[s.btnSecondary, exportingPdf && s.btnDisabled]}
          >
            {exportingPdf ? (
              <ActivityIndicator color={BRAND.colors.text} />
            ) : (
              <Text style={s.btnText}>Export Leadership Pack (PDF)</Text>
            )}
          </Pressable>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function POSScreen({ navigation, demoMode, activeFunds, allFunds, onCreateTx }) {
  const [fundId, setFundId] = useState(activeFunds[0]?.id || "");
  const [amount, setAmount] = useState("200");
  const [step, setStep] = useState("idle");

  // If funds load after mount and no fund is selected yet, default to first active fund.
  useEffect(() => {
    if (!fundId && activeFunds?.length) setFundId(activeFunds[0].id);
  }, [activeFunds, fundId]);

  const selectedFund = allFunds.find((f) => f.id === fundId);
  const amountNum = Number(amount);
  const valid = Number.isFinite(amountNum) && amountNum > 0 && amountNum <= 1000000;

  const start = () => {
    if (!valid || !selectedFund) return;
    setStep("processing");
    setTimeout(() => {
      const ref = `POS-${uid()}`;
      const t = {
        id: uid(),
        createdAt: new Date().toISOString(),
        fundId: selectedFund.id,
        fundName: selectedFund.name,
        amount: Number(amountNum.toFixed(2)),
        reference: ref,
        channel: "pos",
        memberName: "",
        memberPhone: "",
        demo: demoMode ? 1 : 0,
      };
      onCreateTx(t);
      setStep("done");
    }, 900);
  };

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={s.card}>
          <Text style={s.sectionTitle}>POS mode</Text>
          <Text style={s.p}>{demoMode ? "Simulates speed point flow (no charges)." : "Terminal mode."}</Text>

          <Text style={s.label}>Purpose</Text>
          <View style={{ flexDirection: "row", flexWrap: "wrap", gap: 10, marginTop: 8 }}>
            {activeFunds.map((f) => {
              const active = fundId === f.id;
              return (
                <Pressable key={f.id} onPress={() => setFundId(f.id)} style={[s.pill, active && s.pillActive]}>
                  <Text style={{ color: BRAND.colors.text, fontWeight: "900" }}>{f.name}</Text>
                </Pressable>
              );
            })}
          </View>

          <Text style={s.label}>Amount (ZAR)</Text>
          <TextInput
            value={amount}
            onChangeText={(t) => setAmount(t.replace(/[^\d.]/g, ""))}
            keyboardType={Platform.select({ ios: "decimal-pad", android: "numeric" })}
            placeholder="e.g. 200"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />
          {!valid && <Text style={s.warn}>Enter a valid amount.</Text>}

          <Pressable onPress={start} disabled={!valid || !selectedFund || step === "processing"} style={[s.btn, (!valid || !selectedFund || step === "processing") && s.btnDisabled]}>
            <Text style={s.btnText}>{step === "processing" ? "Processing…" : step === "done" ? "Done" : "Charge"}</Text>
          </Pressable>

          {step === "done" && (
            <View style={{ marginTop: 12 }}>
              <Text style={s.muted}>
                Demo receipt created for <Text style={{ fontWeight: "900" }}>{money(amountNum)}</Text> →
                <Text style={{ fontWeight: "900" }}> {selectedFund?.name}</Text>
              </Text>
              <Pressable onPress={() => navigation.navigate("AdminTx")} style={s.btnSecondary}>
                <Text style={s.btnText}>View transactions</Text>
              </Pressable>
            </View>
          )}
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Speed points later</Text>
          <Text style={s.p}>• Churpay selects purpose + amount</Text>
          <Text style={s.p}>• Terminal charges card / tap</Text>
          <Text style={s.p}>• Churpay stores reference for reconciliation</Text>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function AdminSettings({ demoMode, adminPin, setAdminPin, churchName, setChurchName, onResetAll }) {
  const [pin, setPin] = useState(adminPin);

  return (
    <SafeAreaView style={s.safe}>
      <ScrollView contentContainerStyle={{ padding: 16, gap: 12 }}>
        <View style={s.card}>
          <Text style={s.sectionTitle}>Church</Text>
          <TextInput
            value={churchName}
            onChangeText={setChurchName}
            placeholder="Church name"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
          />
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Admin PIN</Text>
          <Text style={s.p}>Set a PIN for admin access on this device.</Text>
          <TextInput
            value={pin}
            onChangeText={(t) => setPin(t.replace(/[^0-9]/g, "").slice(0, 6))}
            keyboardType={Platform.select({ ios: "number-pad", android: "numeric" })}
            placeholder="New PIN"
            placeholderTextColor={BRAND.colors.textMuted}
            style={s.input}
            secureTextEntry
          />
          <Pressable
            onPress={() => {
              if (pin.length < 4) {
                Alert.alert("PIN", "Use at least 4 digits.");
                return;
              }
              setAdminPin(pin);
              Alert.alert("Saved", "Admin PIN updated.");
            }}
            style={s.btnSecondary}
          >
            <Text style={s.btnText}>Save PIN</Text>
          </Pressable>
        </View>

        <View style={s.card}>
          <Text style={s.sectionTitle}>Reset</Text>
          <Text style={s.p}>Clear all local data (funds + transactions + member profile).</Text>
          <Pressable onPress={onResetAll} style={s.btnDanger}>
            <Text style={s.btnText}>Reset app data</Text>
          </Pressable>
        </View>
      </ScrollView>
    </SafeAreaView>
  );
}

function NavBtn({ title, onPress }) {
  return (
    <Pressable onPress={onPress} style={s.navBtn}>
      <Text style={{ color: BRAND.colors.text, fontWeight: "900" }}>{title}</Text>
    </Pressable>
  );
}

const s = {
  safe: { flex: 1, backgroundColor: BRAND.colors.bg },

  headerBlock: {
    flexDirection: "row",
    gap: 12,
    padding: 16,
    alignItems: "center",
  },

  brand: { color: BRAND.colors.text, fontSize: 18, fontWeight: "900" },
  sub: { color: BRAND.colors.textMuted, marginTop: 2 },

  card: {
    marginHorizontal: 16,
    padding: 16,
    borderRadius: 18,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: BRAND.colors.line,
  },

  hero: {
    backgroundColor: "rgba(248,250,252,0.07)",
  },

  heroTitle: { color: BRAND.colors.text, fontSize: 18, fontWeight: "900" },
  heroSub: { color: BRAND.colors.textMuted, marginTop: 2 },

  statTile: {
    flexGrow: 1,
    minWidth: "30%",
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(0,0,0,0.22)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
  },
  statTileWide: {
    flexGrow: 1,
    minWidth: "48%",
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(0,0,0,0.22)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
  },
  statK: { color: BRAND.colors.textMuted, fontSize: 12, fontWeight: "800" },
  statV: { color: BRAND.colors.text, fontSize: 16, fontWeight: "900", marginTop: 6 },
  statSub: { color: BRAND.colors.textMuted, fontSize: 12, marginTop: 6 },

  featureRow: {
    flexDirection: "row",
    gap: 10,
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
    alignItems: "flex-start",
  },
  featureDot: {
    width: 10,
    height: 10,
    borderRadius: 999,
    marginTop: 4,
    backgroundColor: "rgba(34,211,238,0.95)",
  },
  featureTitle: { color: BRAND.colors.text, fontWeight: "900" },
  featureBody: { color: BRAND.colors.textMuted, marginTop: 4, lineHeight: 18 },

  actionCard: {
    flexGrow: 1,
    minWidth: "48%",
    padding: 14,
    borderRadius: 18,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
  },
  actionTitle: { color: BRAND.colors.text, fontWeight: "900" },
  actionBody: { color: BRAND.colors.textMuted, marginTop: 6, lineHeight: 18 },

  rankRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
  },
  rankName: { color: BRAND.colors.text, fontWeight: "900" },
  rankValue: { color: BRAND.colors.text, fontWeight: "900" },

  row: { flexDirection: "row", gap: 10, paddingHorizontal: 16, flexWrap: "wrap" },
  navBtn: {
    minWidth: "48%",
    flexGrow: 1,
    paddingVertical: 12,
    borderRadius: 16,
    backgroundColor: "rgba(248,250,252,0.08)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
    alignItems: "center",
    marginBottom: 10,
  },

  sectionTitle: { color: BRAND.colors.text, fontSize: 16, fontWeight: "900" },
  p: { color: BRAND.colors.textMuted, marginTop: 6, lineHeight: 20 },
  muted: { color: BRAND.colors.textMuted, fontSize: 12 },

  label: { color: BRAND.colors.textMuted, marginBottom: 6, marginTop: 10, fontSize: 12 },

  input: {
    backgroundColor: "rgba(248,250,252,0.07)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
    color: BRAND.colors.text,
    paddingHorizontal: 12,
    paddingVertical: 12,
    borderRadius: 14,
    marginTop: 10,
  },

  // ====== Contribute flow styles ======
  stepPill: {
    flex: 1,
    paddingVertical: 10,
    borderRadius: 14,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
    alignItems: "center",
  },
  stepPillActive: {
    backgroundColor: BRAND.colors.tealSoft,
    borderColor: "rgba(34,211,238,0.55)",
  },
  stepPillText: {
    color: BRAND.colors.text,
    fontWeight: "900",
    fontSize: 12,
    opacity: 0.75,
  },

  fundCard: {
    padding: 14,
    borderRadius: 18,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
  },
  fundCardActive: {
    backgroundColor: BRAND.colors.tealSoft,
    borderColor: "rgba(34,211,238,0.55)",
  },
  fundCardTitle: {
    color: BRAND.colors.text,
    fontWeight: "900",
    fontSize: 15,
  },
  fundCardSub: {
    color: BRAND.colors.textMuted,
    marginTop: 6,
    fontSize: 12,
  },

  amountInput: {
    backgroundColor: "rgba(248,250,252,0.08)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.16)",
    color: BRAND.colors.text,
    paddingHorizontal: 14,
    paddingVertical: 14,
    borderRadius: 18,
    marginTop: 10,
    fontSize: 22,
    fontWeight: "900",
  },

  amountChip: {
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 999,
    backgroundColor: "rgba(248,250,252,0.08)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.14)",
  },
  amountChipActive: {
    backgroundColor: BRAND.colors.tealSoft,
    borderColor: "rgba(34,211,238,0.55)",
  },
  amountChipText: {
    color: BRAND.colors.text,
    fontWeight: "900",
  },
  amountChipActiveText: {
    color: BRAND.colors.bg,
  },

  warn: { color: BRAND.colors.warn, marginTop: 8, fontWeight: "800" },

  pill: {
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 999,
    backgroundColor: "rgba(248,250,252,0.07)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
  },
  pillActive: {
    backgroundColor: BRAND.colors.tealSoft,
    borderColor: "rgba(34,211,238,0.55)",
  },

  btn: {
    marginTop: 14,
    paddingVertical: 13,
    borderRadius: 16,
    alignItems: "center",
    backgroundColor: "rgba(34,211,238,0.95)",
  },
  btnDisabled: { opacity: 0.5 },
  btnSecondary: {
    marginTop: 10,
    paddingVertical: 12,
    borderRadius: 16,
    alignItems: "center",
    backgroundColor: "rgba(248,250,252,0.10)",
    borderWidth: 1,
    borderColor: BRAND.colors.lineStrong,
  },
  btnDanger: {
    paddingVertical: 12,
    borderRadius: 16,
    alignItems: "center",
    backgroundColor: "rgba(248,113,113,0.22)",
    borderWidth: 1,
    borderColor: "rgba(248,113,113,0.35)",
  },
  btnText: { color: BRAND.colors.text, fontWeight: "900" },

  kpiLabel: { color: BRAND.colors.textMuted, fontWeight: "800" },
  kpiValue: { color: BRAND.colors.text, fontSize: 26, fontWeight: "900", marginTop: 6 },
  kpiFoot: { color: BRAND.colors.textMuted, marginTop: 6 },

  listRow: {
    flexDirection: "row",
    gap: 10,
    paddingVertical: 12,
    borderBottomWidth: 1,
    borderBottomColor: "rgba(248,250,252,0.08)",
    alignItems: "center",
    flexWrap: "wrap",
  },
  tinyBtn: {
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 14,
    backgroundColor: "rgba(248,250,252,0.10)",
    borderWidth: 1,
    borderColor: BRAND.colors.lineStrong,
  },

  code: {
    color: "rgba(248,250,252,0.85)",
    fontSize: 12,
    backgroundColor: "rgba(0,0,0,0.25)",
    padding: 10,
    borderRadius: 12,
    marginTop: 6,
  },

  intent: {
    flexDirection: "row",
    gap: 12,
    paddingVertical: 12,
    borderTopWidth: 1,
    borderTopColor: "rgba(248,250,252,0.08)",
  },


  qrHeader: {
    flexDirection: "row",
    gap: 10,
    alignItems: "center",
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: BRAND.colors.line,
    marginTop: 12,
  },

  // Chips (filters)
  chip: {
    paddingVertical: 10,
    paddingHorizontal: 12,
    borderRadius: 999,
    backgroundColor: "rgba(248,250,252,0.08)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.14)",
  },
  chipActive: {
    backgroundColor: BRAND.colors.tealSoft,
    borderColor: "rgba(34,211,238,0.55)",
  },
  chipText: {
    color: BRAND.colors.text,
    fontWeight: "900",
    fontSize: 12,
    opacity: 0.8,
    letterSpacing: 0.5,
  },
  chipMini: {
    paddingVertical: 6,
    paddingHorizontal: 10,
    borderRadius: 999,
    backgroundColor: "rgba(248,250,252,0.07)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
  },
  chipMiniOn: {
    backgroundColor: BRAND.colors.tealSoft,
    borderColor: "rgba(34,211,238,0.55)",
  },
  chipMiniText: {
    color: BRAND.colors.text,
    fontWeight: "900",
    fontSize: 11,
  },

  // Fund management cards
  fundManageCard: {
    padding: 14,
    borderRadius: 18,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
  },
  fundManageCardActive: {
    backgroundColor: "rgba(34,211,238,0.10)",
    borderColor: "rgba(34,211,238,0.35)",
  },
  fundManageTitle: {
    color: BRAND.colors.text,
    fontWeight: "900",
    fontSize: 15,
  },
  fundManageSub: {
    color: BRAND.colors.textMuted,
    marginTop: 6,
    fontSize: 12,
  },
  iconBadge: {
    width: 28,
    height: 28,
    borderRadius: 10,
    alignItems: "center",
    justifyContent: "center",
    borderWidth: 1,
  },
  iconBadgeOn: {
    backgroundColor: "rgba(52,211,153,0.18)",
    borderColor: "rgba(52,211,153,0.35)",
  },
  iconBadgeOff: {
    backgroundColor: "rgba(248,250,252,0.08)",
    borderColor: "rgba(248,250,252,0.14)",
  },
  tinyBtnPrimary: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    backgroundColor: BRAND.colors.tealSoft,
    borderColor: "rgba(34,211,238,0.45)",
  },
  tinyBtnDanger: {
    flexDirection: "row",
    alignItems: "center",
    gap: 8,
    borderColor: "rgba(248,113,113,0.35)",
    backgroundColor: "rgba(248,113,113,0.18)",
  },
  tinyBtnText: {
    color: BRAND.colors.text,
    fontWeight: "900",
  },

  // Transaction cards
  txCard: {
    padding: 14,
    borderRadius: 18,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
  },
  txIcon: {
    width: 38,
    height: 38,
    borderRadius: 14,
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "rgba(248,250,252,0.08)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.12)",
  },
  txAmount: {
    color: BRAND.colors.text,
    fontWeight: "900",
    fontSize: 16,
  },
  txMeta: {
    color: BRAND.colors.textMuted,
    marginTop: 2,
    fontSize: 12,
  },
  txDate: {
    color: BRAND.colors.textMuted,
    fontSize: 11,
  },

  // Reports bar "graph look"
  barRow: {
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
  },
  barName: {
    color: BRAND.colors.text,
    fontWeight: "900",
    flex: 1,
  },
  barValue: {
    color: BRAND.colors.text,
    fontWeight: "900",
  },
  barTrack: {
    height: 10,
    borderRadius: 999,
    backgroundColor: "rgba(0,0,0,0.25)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
    overflow: "hidden",
    marginTop: 10,
  },
  barFill: {
    height: "100%",
    borderRadius: 999,
    backgroundColor: "rgba(34,211,238,0.85)",
  },
  // Monthly trend tiles
  trendTile: {
    flexGrow: 1,
    minWidth: "48%",
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
  },
  trendMonth: {
    color: BRAND.colors.textMuted,
    fontSize: 12,
    fontWeight: "800",
  },
  trendValue: {
    color: BRAND.colors.text,
    fontSize: 16,
    fontWeight: "900",
    marginTop: 6,
  },
  trendTrack: {
    height: 10,
    borderRadius: 999,
    backgroundColor: "rgba(0,0,0,0.25)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
    overflow: "hidden",
    marginTop: 10,
  },
  trendFill: {
    height: "100%",
    borderRadius: 999,
    backgroundColor: "rgba(34,211,238,0.75)",
  },
  // Top donors
  donorRow: {
    flexDirection: "row",
    alignItems: "center",
    gap: 12,
    padding: 12,
    borderRadius: 16,
    backgroundColor: "rgba(248,250,252,0.06)",
    borderWidth: 1,
    borderColor: "rgba(248,250,252,0.10)",
  },
  donorRank: {
    width: 28,
    height: 28,
    borderRadius: 10,
    backgroundColor: "rgba(34,211,238,0.14)",
    borderWidth: 1,
    borderColor: "rgba(34,211,238,0.35)",
    alignItems: "center",
    justifyContent: "center",
  },
  donorRankText: {
    color: BRAND.colors.text,
    fontWeight: "900",
  },
  donorName: {
    color: BRAND.colors.text,
    fontWeight: "900",
  },
  donorAmount: {
    color: BRAND.colors.text,
    fontWeight: "900",
  },
};