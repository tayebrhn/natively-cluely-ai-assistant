import assert from 'node:assert/strict';
import test from 'node:test';
import { isDevProOverrideEnabled } from '../../../dist-electron/electron/services/dev/devProOverride.js';

test('development Pro override requires an explicit opt-in', () => {
  assert.equal(isDevProOverrideEnabled({ isPackaged: false, nodeEnv: 'development', flagValue: '1' }), true);
  assert.equal(isDevProOverrideEnabled({ isPackaged: false, nodeEnv: 'test', flagValue: '1' }), true);
  assert.equal(isDevProOverrideEnabled({ isPackaged: false, nodeEnv: 'development', flagValue: undefined }), false);
});

test('development Pro override is disabled in packaged builds', () => {
  assert.equal(isDevProOverrideEnabled({ isPackaged: true, nodeEnv: 'development', flagValue: '1' }), false);
  assert.equal(isDevProOverrideEnabled({ isPackaged: true, nodeEnv: 'test', flagValue: '1' }), false);
  assert.equal(isDevProOverrideEnabled({ isPackaged: false, nodeEnv: 'production', flagValue: '1' }), false);
});
