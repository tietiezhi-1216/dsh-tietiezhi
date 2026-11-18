import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, readdir, realpath, rm, symlink, lstat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { apply, createDeletion, createHandler } from '../features/permanent-delete/host.js';

async function fixture(t) {
  const base = await realpath(tmpdir());
  const temp = await mkdtemp(join(base, 'ttz-test-'));
  t.after(async () => {
    if (dirname(temp) === base && await realpath(temp) === temp) await rm(temp, { recursive: true });
  });
  const root = join(temp, 'sessions');
  const dir = join(root, '--project--', 'target');
  const other = join(root, '--project--', 'other');
  await Promise.all([mkdir(dir, { recursive: true }), mkdir(other, { recursive: true })]);
  const log = join(dir, 'session.v4.jsonl');
  const siblingLog = join(other, 'session.v4.jsonl');
  for (const file of [log, siblingLog, join(dir, 'session.v3.jsonl'), join(temp, 'output.txt')]) await writeFile(file, 'unchanged');
  const events = [];
  const snapshot = { header: { id: 'target' }, revision: 'initial', formatStatus: 'current', location: { path: log, kind: 'jsonl' } };
  const ctx = {
    sessions: { get() {}, list: () => [] }, agents: { get() {} },
    sessionPersistence: {
      name: 'session-persistence-jsonl', root, stat: async () => snapshot, list: async () => [snapshot],
      open: async (id, mode) => {
        assert.equal(mode, 'write'); events.push(['lock', id]);
        return { close: async () => { events.push(['unlock', id]); } };
      },
    },
    workspaceRegistry: {
      list: () => [{ sessionIds: ['other', 'target'], detachSession: async (id) => events.push(['detach', id]) }],
      unarchiveSession: async (id) => events.push(['unarchive', id]), unpinSession: async (id) => events.push(['unpin', id]),
    },
    storageDomain: { get: () => ({ table: () => ({ delete: async (id) => events.push(['uncache', id]) }) }) },
    logger: { warn: () => {} }, emit: (...args) => events.push(args),
  };
  return { ctx, events, temp, root, dir, log, siblingLog, snapshot, remove: createDeletion(ctx) };
}

test('permanently removes all generations for exactly the confirmed session', async (t) => {
  const f = await fixture(t);
  assert.deepEqual(await f.remove('target'), { ok: true });
  await assert.rejects(lstat(f.dir), { code: 'ENOENT' });
  assert.equal(await readFile(f.siblingLog, 'utf8'), 'unchanged');
  assert.equal(await readFile(join(f.temp, 'output.txt'), 'utf8'), 'unchanged');
  assert.deepEqual(f.events.map(([event]) => event).sort(), ['api-session/removed', 'detach', 'lock', 'unarchive', 'uncache', 'unlock', 'unpin']);
  assert.ok(f.events.every(([, id]) => id === 'target'));
  assert.ok(!(await readdir(f.temp)).some((path) => path.startsWith('.dsh-tietiezhi-delete-')));
});

test('live, legacy and dependent sessions are refused without deleting their log', async (t) => {
  const f = await fixture(t);
  f.ctx.agents.get = () => ({});
  assert.deepEqual(await f.remove('target'), { ok: false, code: 'active' });
  assert.deepEqual(f.events, []);
  f.ctx.agents.get = () => undefined;
  f.snapshot.formatStatus = 'migration-required';
  assert.deepEqual(await f.remove('target'), { ok: false, code: 'unsupported' });
  f.snapshot.formatStatus = 'current';
  f.ctx.sessionPersistence.list = async () => [{ header: { id: 'child', parentSession: 'target' } }];
  assert.deepEqual(await f.remove('target'), { ok: false, code: 'dependent' });
  assert.deepEqual(f.events.map(([name]) => name), ['lock', 'unlock']);
  assert.equal(await readFile(f.log, 'utf8'), 'unchanged');
});

test('rejects a symlinked project and traversal ids', async (t) => {
  const f = await fixture(t);
  const project = join(f.root, '--alias--');
  await symlink(dirname(f.dir), project, 'dir');
  f.snapshot.location.path = join(project, 'target', 'session.v4.jsonl');
  assert.deepEqual(await f.remove('target'), { ok: false, code: 'unsupported' });
  assert.deepEqual(await f.remove('../other'), { ok: false, code: 'invalid' });
  assert.equal(await readFile(f.log, 'utf8'), 'unchanged');
  assert.deepEqual(f.events, []);
});

test('lease contention and repeated submission do not modify storage', async (t) => {
  const f = await fixture(t);
  let release;
  f.ctx.sessionPersistence.open = () => new Promise((_, reject) => { release = reject; });
  const pending = f.remove('target');
  assert.deepEqual(await f.remove('target'), { ok: false, code: 'concurrent' });
  while (!release) await new Promise((resolve) => setImmediate(resolve));
  release(Object.assign(new Error('in use'), { name: 'SessionAlreadyOwnedError' }));
  assert.deepEqual(await pending, { ok: false, code: 'active' });
  assert.equal(await readFile(f.log, 'utf8'), 'unchanged');
});

test('metadata cleanup failure is surfaced, not reported as success', async (t) => {
  const f = await fixture(t);
  f.ctx.workspaceRegistry.unpinSession = async () => { throw new Error('test failure'); };
  assert.deepEqual(await f.remove('target'), { ok: false, code: 'partial' });
  await assert.rejects(lstat(f.dir), { code: 'ENOENT' });
  assert.equal(await readFile(f.siblingLog, 'utf8'), 'unchanged');
});

test('endpoint requires the authenticated browser peer, same authority and action header', async () => {
  const browserPeer = { id: 'browser' };
  let peer = browserPeer;
  const calls = [];
  const handle = createHandler({ browserPeer, admit: () => ({ peer }) }, async (id) => { calls.push(id); return { ok: true }; });
  const request = (headers = {}, body = '{"sessionId":"target"}') => new Request('http://dsh.internal/api/dsh-tietiezhi/permanent-delete', {
    method: 'POST', headers: { host: '127.0.0.1:59580', origin: 'http://127.0.0.1:59580', 'content-type': 'application/json',
      'x-dsh-tietiezhi-action': 'delete-session', ...headers }, body,
  });
  assert.equal((await handle(request({ origin: 'http://127.0.0.1:1111' }))).status, 403);
  assert.equal((await handle(request({ 'x-dsh-tietiezhi-action': '' }))).status, 403);
  assert.equal((await handle(request({}, ' '.repeat(1025)))).status, 413);
  assert.equal((await handle(request({}, '{bad'))).status, 400);
  peer = { id: 'api' };
  assert.equal((await handle(request())).status, 403);
  peer = browserPeer;
  assert.equal((await handle(request())).status, 200);
  assert.deepEqual(calls, ['target']);
});

test('enabling the feature registers status and deletion, disabling withdraws both', async (t) => {
  const { ctx } = await fixture(t);
  const routes = new Map();
  const disposers = [];
  const browserPeer = { id: 'browser' };
  let peer = browserPeer;
  ctx.connection = {
    browserPeer, admit: () => ({ peer }),
    fetch: { register(route) {
      routes.set(route.path, route);
      return async () => { routes.delete(route.path); };
    } },
  };
  ctx.effect = (register) => disposers.push(register());
  apply(ctx);
  const base = '/api/dsh-tietiezhi/permanent-delete';
  assert.deepEqual(routes.get(base).methods, ['POST']);
  const probe = routes.get(`${base}/status`);
  assert.deepEqual(probe.methods, ['GET']);
  assert.deepEqual(await (await probe.fetch(new Request(`http://dsh.internal${probe.path}`))).json(), { ok: true, enabled: true });
  peer = undefined;
  assert.equal((await probe.fetch(new Request(`http://dsh.internal${probe.path}`))).status, 403);
  for (const dispose of disposers) await dispose();
  assert.equal(routes.size, 0);
});
