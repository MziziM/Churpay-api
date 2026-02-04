# Churpay Brand Tokens

Single source of truth for design system used across web and mobile app.

## Structure

```
brand/
  ├── tokens.js   # JS/TS tokens for React Native
  ├── tokens.css  # CSS variables for web
  └── index.js    # Convenience export
```

## Usage in React Native App

```javascript
import { Brand } from "./brand";

const styles = StyleSheet.create({
  screen: {
    flex: 1,
    backgroundColor: Brand.colors.bg,
  },
  card: {
    backgroundColor: Brand.colors.card,
    borderColor: Brand.colors.cardBorder,
    borderWidth: 1,
    borderRadius: Brand.radius.lg,
    padding: Brand.spacing.lg,
  },
  title: {
    color: Brand.colors.text,
    fontSize: Brand.type.h1,
    fontWeight: Brand.type.weightBold,
  },
  primaryBtn: {
    backgroundColor: Brand.colors.primary,
    borderRadius: Brand.radius.pill,
    paddingVertical: Brand.spacing.md,
  },
});
```

## Usage in Web HTML

Include the CSS file:

```html
<link rel="stylesheet" href="/brand/tokens.css">
```

Use CSS variables in your styles:

```css
body {
  background: var(--cp-hero);
  color: var(--cp-text);
}

.card {
  background: var(--cp-card);
  border: 1px solid var(--cp-card-border);
  border-radius: var(--cp-r-lg);
  box-shadow: var(--cp-shadow);
  padding: var(--cp-s-xl);
}

.btn-primary {
  background: var(--cp-primary);
  color: var(--cp-text);
  border-radius: var(--cp-r-pill);
}
```

## Token Categories

### Colors
- `bg`, `surface`, `card`, `cardBorder`
- `text`, `textMuted`, `textFaint`
- `primary`, `primary2`, `danger`, `success`

### Spacing
- `xs`, `sm`, `md`, `lg`, `xl`

### Radius
- `sm`, `md`, `lg`, `pill`

### Typography
- Sizes: `h1`, `h2`, `body`, `small`
- Weights: `weightBold`, `weightSemi`, `weightRegular`

## Keeping in Sync

Currently using Option A: same folder copied in both repos.

**Important**: When updating tokens, update in BOTH locations:
- `/churpay-api/brand/` (and copy to `/churpay-api/public/brand/tokens.css`)
- `/churpay-app/brand/`

Future: Move to shared package or auto-generate CSS from JS.
