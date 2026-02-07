export function withTimeout(promise, ms = 4000) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error("BOOT_TIMEOUT")), ms)),
  ]);
}

export async function safe(promise) {
  try {
    return await promise;
  } catch (_) {
    return null;
  }
}
