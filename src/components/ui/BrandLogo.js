import React from "react";
import { SvgXml } from "react-native-svg";

import { Logo } from "../../../brand";

export function BrandLogo({ width = 220, height, style, accessibilityLabel = "Churpay" }) {
  const w = Math.max(1, Math.round(Number(width) || 220));
  const ratio = Number(Logo?.aspectRatio || 2) || 2;
  const h = Math.max(1, Math.round(typeof height === "number" ? height : w / ratio));

  return <SvgXml xml={Logo.svgXml} width={w} height={h} style={style} accessibilityLabel={accessibilityLabel} />;
}

