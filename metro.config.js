const { getDefaultConfig } = require("expo/metro-config");
const path = require("path");

const config = getDefaultConfig(__dirname);

// Block the backend folder from Metro
config.resolver.blockList = [
  new RegExp(path.resolve(__dirname, "../churpay-api").replace(/[/\\]/g, "[/\\\\]") + ".*"),
];

module.exports = config;