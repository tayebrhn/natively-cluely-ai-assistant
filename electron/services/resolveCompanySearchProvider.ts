// electron/services/resolveCompanySearchProvider.ts
// Single source of truth for the company-research search provider cascade:
//   Tavily (user key) → Natively API proxy (Natively key / trial token) → null (LLM-only).
// Used by both the manual profile:research-company IPC handler and the automatic
// AOT pipeline (injected via KnowledgeOrchestrator.setSearchProviderResolver),
// so the two paths cannot drift. Resolve per invocation — never cache the
// result — because keys can be added, changed, or removed mid-session.

import { TRIAL_SENTINEL_KEY } from '../config/constants';
import { CredentialsManager } from './CredentialsManager';
// `SearchProvider` is implemented in the premium/ tree, which is absent from
// the source-available build. Describe the structural surface this resolver and
// its consumers depend on locally, so `tsc` never has to resolve the premium
// module. A type-only `import('../../premium/...').SearchProvider` does NOT
// avoid resolution — tsc still loads that module to check the reference and
// fails with TS2307 when premium is not checked out. The concrete Tavily /
// Natively providers are loaded via runtime require() below (see the try/catch
// cascade) and satisfy this shape structurally when premium IS present; every
// caller receives the resolver through require(), so this local type never has
// to be assignable to premium's own SearchProvider.
interface SearchProvider {
  search(...args: any[]): Promise<any[]>;
}

export function resolveCompanySearchProvider(): SearchProvider | null {
  const cm = CredentialsManager.getInstance();

  const tavilyApiKey = cm.getTavilyApiKey();
  if (tavilyApiKey) {
    // Wrapped in try/catch so esbuild treats the unresolvable premium require as
    // a warning (deferred to runtime) instead of a hard build error. Mirrors the
    // conditional-load pattern in electron/main.ts.
    try {
      const {
        TavilySearchProvider,
      } = require('../../premium/electron/knowledge/TavilySearchProvider');
      return new TavilySearchProvider(tavilyApiKey);
    } catch {
      console.log('[CompanySearch] Tavily provider unavailable (premium module not present).');
    }
  }

  const nativelyKey = cm.getNativelyApiKey();
  if (nativelyKey) {
    try {
      const {
        NativelySearchProvider,
      } = require('../../premium/electron/knowledge/NativelySearchProvider');
      // Pass the real trial token when the key is the __trial__ sentinel so the
      // server can authenticate via x-trial-token instead of the invalid key.
      const trialToken = nativelyKey === TRIAL_SENTINEL_KEY ? cm.getTrialToken() : undefined;
      console.log('[CompanySearch] Using Natively API search (no Tavily key configured)');
      return new NativelySearchProvider(nativelyKey, trialToken ?? undefined);
    } catch {
      console.log('[CompanySearch] Natively search provider unavailable (premium module not present).');
    }
  }

  return null;
}
