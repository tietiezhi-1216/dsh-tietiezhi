import assert from 'node:assert/strict';
import { mkdtemp, realpath, rm, readFile, lstat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { createRequire } from 'node:module';
import { pathToFileURL } from 'node:url';
import { test } from 'node:test';
import { createDeletion, createHandler } from '../features/permanent-delete/host.js';

const integration = process.versions.electron && process.env.DSH_INSTALL_DIR;
for (const compression of ['none', 'zstd']) test(`Desktop POST deletes a temporary ${compression} record through installed Connection + Cordis + JSONL`, { skip: !integration }, async () => {
  const require = createRequire(join(process.env.DSH_INSTALL_DIR, 'package.json'));
  const installed = (name) => import(pathToFileURL(require.resolve(name)).href);
  const { Context } = await installed('@deepseek-ai/cordis');
  const persistencePlugin = await installed('@deepseek-ai/dsh-session-persistence-jsonl');
  const { SESSION_FORMAT_VERSION } = await installed('@deepseek-ai/dsh-session');
  const { HostConnectionService } = await installed('@deepseek-ai/dsh-client-connection');
  const base = await realpath(tmpdir());
  const temp = await mkdtemp(join(base, 'ttz-real-host-'));
  const root = new Context();
  try {
    const fiber = root.plugin(persistencePlugin, { root: join(temp, 'sessions'), compression, listConcurrency: 3 });
    await fiber.await();
    const p = root.sessionPersistence;
    assert.ok(p);
    const targetId = 'ttz-integration-target';
    const otherId = 'ttz-integration-other';
    for (const id of [targetId, otherId]) {
      const handle = await p.create({ id, version: SESSION_FORMAT_VERSION, createdAt: Date.now(), isSeeded: false });
      await handle.flush();
      await handle.close();
    }
    const target = await p.stat(targetId);
    const other = await p.stat(otherId);
    const otherBytes = await readFile(other.location.path);
    const targetBytes = await readFile(target.location.path);
    const calls = [];
    root.provide('agents', { get() {} });
    root.provide('sessions', { get() {}, list: () => [] });
    root.provide('workspaceRegistry', {
      list: () => [{ sessionIds: [targetId, otherId], detachSession: async (id) => calls.push(['detach', id]) }],
      unpinSession: async (id) => calls.push(['unpin', id]), unarchiveSession: async (id) => calls.push(['unarchive', id]),
    });
    root.provide('storageDomain', { get: () => ({ table: () => ({ delete: async (id) => calls.push(['cache', id]) }) }) });
    // Only the fixture credential is simulated; use the installed trust fence, route dispatch,
    // peer scopes, Cordis service proxies and disk persistence. Never read the user's credentials.
    new HostConnectionService(root, [], {
      isAuthenticated: ({ headers }) => headers.get('cookie') === 'ttz-test-owner=1',
      authenticateApi: () => undefined,
    });
    const connection = root.connection;
    const path = '/api/dsh-tietiezhi/permanent-delete';
    connection.fetch.register({ path, methods: ['POST'], requestBody: 'streaming', fetch: createHandler(connection, createDeletion(root)) });
    const carrier = connection.createSharedFetchHandler('/api');
    const request = (extra = {}) => new Request(`http://dsh.internal${path}`, {
      method: 'POST', duplex: 'half',
      headers: { host: 'localhost', cookie: 'ttz-test-owner=1', 'content-type': 'application/json', 'x-dsh-tietiezhi-action': 'delete-session', ...extra },
      body: new ReadableStream({ start(controller) {
        controller.enqueue(new TextEncoder().encode(JSON.stringify({ sessionId: targetId })));
        controller.close();
      } }),
    });
    const send = async (input) => {
      const admission = connection.admit({ headers: input.headers });
      if (!admission.peer) return new Response(null, { status: admission.rejection });
      return carrier.fetch(input, admission.peer);
    };
    assert.equal(carrier.requestBodyMode({ method: 'POST', url: new URL(`http://dsh.internal${path}`) }), 'streaming');
    assert.equal((await send(request({ cookie: 'not-authorized' }))).status, 401);
    assert.equal((await send(request({ origin: 'https://evil.example' }))).status, 403);
    assert.equal((await send(request({ host: 'evil.example' }))).status, 403);
    assert.deepEqual(await readFile(target.location.path), targetBytes);
    const response = await send(request());
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { ok: true });
    assert.equal(await p.stat(targetId), undefined);
    await assert.rejects(lstat(dirname(target.location.path)), { code: 'ENOENT' });
    assert.deepEqual(await readFile(other.location.path), otherBytes);
    assert.ok(calls.every(([, id]) => id === targetId));
  } finally {
    try { await root.fiber.dispose(); }
    finally { if (dirname(temp) === base && await realpath(temp) === temp) await rm(temp, { recursive: true }); }
  }
});
