const TOKEN_MODE_STRICT = 'strict';
const TOKEN_MODE_DUAL_LEGACY = 'dual-legacy-8h';
const FORCE_MODE_OBSERVE = 'observe';
const FORCE_MODE_GUIDE = 'guide';
const FORCE_MODE_ENFORCE = 'enforce';
const MAX_LEGACY_WINDOW_MS = 8 * 60 * 60 * 1000;

function integrationMetadata(options = {}) {
  return {
    tokenMode: options.tokenMode || TOKEN_MODE_STRICT,
    forcePasswordChangeMode: options.forcePasswordChangeMode || FORCE_MODE_ENFORCE,
  };
}

function createProductionAuthIntegration(env, now = Date.now()) {
  if (env.AUTH_RUNTIME_PROFILE !== 'production-integration-candidate') {
    throw new Error('Production auth integration profile is not explicitly enabled.');
  }
  const tokenMode = env.AUTH_TOKEN_MODE;
  if (!tokenMode) throw new Error('AUTH_TOKEN_MODE must be set explicitly.');
  if (![TOKEN_MODE_STRICT, TOKEN_MODE_DUAL_LEGACY].includes(tokenMode)) throw new Error('Invalid AUTH_TOKEN_MODE.');
  const forcePasswordChangeMode = env.AUTH_FORCE_PASSWORD_CHANGE_MODE;
  if (!forcePasswordChangeMode) throw new Error('AUTH_FORCE_PASSWORD_CHANGE_MODE must be set explicitly.');
  if (![FORCE_MODE_OBSERVE, FORCE_MODE_GUIDE, FORCE_MODE_ENFORCE].includes(forcePasswordChangeMode)) {
    throw new Error('Invalid AUTH_FORCE_PASSWORD_CHANGE_MODE.');
  }
  let legacyAcceptUntil = null;
  if (tokenMode === TOKEN_MODE_DUAL_LEGACY) {
    legacyAcceptUntil = Date.parse(env.AUTH_LEGACY_ACCEPT_UNTIL || '');
    if (!Number.isFinite(legacyAcceptUntil) || legacyAcceptUntil <= now || legacyAcceptUntil > now + MAX_LEGACY_WINDOW_MS) {
      throw new Error('Legacy token cutoff must be in the future and at most eight hours away.');
    }
  }
  let trustProxy = false;
  if (env.AUTH_TRUST_PROXY_VERIFIED === 'true') {
    const hops = Number(env.AUTH_TRUST_PROXY_HOPS);
    if (!Number.isInteger(hops) || hops < 1 || hops > 4) throw new Error('Verified trust proxy hops must be between one and four.');
    trustProxy = hops;
  } else if (env.AUTH_TRUST_PROXY_HOPS) {
    throw new Error('Proxy hops require explicit verification.');
  }
  return { tokenMode, forcePasswordChangeMode, legacyAcceptUntil, trustProxy };
}

module.exports = {
  FORCE_MODE_ENFORCE, FORCE_MODE_GUIDE, FORCE_MODE_OBSERVE, MAX_LEGACY_WINDOW_MS,
  TOKEN_MODE_DUAL_LEGACY, TOKEN_MODE_STRICT, createProductionAuthIntegration, integrationMetadata,
};
