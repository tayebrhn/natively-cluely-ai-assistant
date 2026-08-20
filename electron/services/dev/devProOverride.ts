export interface DevProOverrideOptions {
  isPackaged: boolean;
  nodeEnv?: string;
  flagValue?: string;
}

/** Explicit, development-only entitlement for local premium feature work. */
export function isDevProOverrideEnabled(options: DevProOverrideOptions): boolean {
  if (options.isPackaged) return false;

  const isDevelopmentEnvironment = options.nodeEnv === 'development' || options.nodeEnv === 'test';
  return isDevelopmentEnvironment && options.flagValue === '1';
}

export function getDevProOverrideStatus() {
  const packaged = require('electron').app.isPackaged;
  const enabled = isDevProOverrideEnabled({
    isPackaged: packaged,
    nodeEnv: process.env.NODE_ENV,
    flagValue: process.env.NATIVELY_DEV_PRO_OVERRIDE,
  });

  return {
    enabled,
    packaged,
    environment: process.env.NODE_ENV === 'test'
      ? 'test'
      : process.env.NODE_ENV === 'development' ? 'development' : 'production',
  } as const;
}
