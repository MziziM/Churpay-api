const fs = require("fs");

const ENV_FILE = "/etc/churpay/churpay-api.env";

function readEnvFile(filePath) {
  try {
    const content = fs.readFileSync(filePath, "utf8");
    const out = {};
    for (const rawLine of content.split(/\r?\n/)) {
      const line = rawLine.trim();
      if (!line || line.startsWith("#")) continue;
      const idx = line.indexOf("=");
      if (idx <= 0) continue;
      const key = line.slice(0, idx).trim();
      let value = line.slice(idx + 1).trim();
      if (
        (value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))
      ) {
        value = value.slice(1, -1);
      }
      out[key] = value;
    }
    return out;
  } catch (_err) {
    return {};
  }
}

const fileEnv = readEnvFile(ENV_FILE);

module.exports = {
  apps: [
    {
      name: "churpay-api",
      cwd: "/var/www/churpay/repo/churpay-api",
      script: "src/index.js",
      interpreter: "node",
      env_file: "/etc/churpay/churpay-api.env",
      instances: 1,
      exec_mode: "fork",
      autorestart: true,
      max_memory_restart: "512M",
      watch: false,
      env: {
        ...fileEnv,
        NODE_ENV: fileEnv.NODE_ENV || "production",
        PORT: fileEnv.PORT || "8080",
      },
    },
  ],
};
