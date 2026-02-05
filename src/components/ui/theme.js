import { useColorScheme } from "react-native";
import { Brand } from "../../../churpay-api/brand/brand";

export const useTheme = () => {
  const scheme = useColorScheme();
  const palette = scheme === "dark" ? Brand.colors.dark : Brand.colors.light;
  return {
    palette,
    spacing: Brand.spacing,
    radius: Brand.radius,
    typography: Brand.typography,
    scheme,
  };
};
