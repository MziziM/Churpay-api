# Churpay Public Website (`churpay-web`)

## Local run

```bash
cd /Users/mzizimzwakhe/Documents/Churpay-demo/churpay-web
npm ci
npm run dev
```

## Build

```bash
npm run build
npm run preview
```

## Environment variables

```bash
VITE_API_BASE_URL=https://api.churpay.com
VITE_APP_LINK=https://expo.dev
```

## Notes
- Contact form posts to `POST /api/public/contact` on API.
- Status page checks `GET /health` on API base.
