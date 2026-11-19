import { lstat, mkdtemp, readdir, realpath, rename, rm, rmdir } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, sep } from 'node:path';

export const inject = ['tietiezhiBundle', 'connection', 'sessionPersistence', 'sessions', 'agents', 'workspaceRegistry', 'storageDomain'];
const route = '/api/dsh-tietiezhi/permanent-delete';
const SESSION_ID = /^[a-zA-Z0-9_-]{1,128}$/;
const LOG_NAME = /^session(?:\.v[1-9][0-9]*)?\.jsonl(?:\.zstd)?$/;
const LIMIT = 1024;
const status = { invalid: 400, forbidden: 403, missing: 404, unavailable: 409, active: 409, dependent: 409, concurrent: 409, large: 413, unsupported: 422, failed: 500, partial: 500 };
class Refusal extends Error { constructor(code) { super(code); this.code = code; } }
const reject = (code) => { throw new Refusal(code); };
const same = (a, b) => a.ino === b.ino && a.dev === b.dev && a.birthtimeNs === b.birthtimeNs;
const identity = (path) => lstat(path, { bigint: true });

async function locate(persistence, snapshot, id) {
  const log = snapshot.location?.path;
  const root = persistence.root;
  if (persistence.name !== 'session-persistence-jsonl' || snapshot.header?.id !== id || snapshot.formatStatus !== 'current' ||
      snapshot.location?.kind !== 'jsonl' || !log || !isAbsolute(log) || !root || !isAbsolute(root) || !LOG_NAME.test(basename(log))) reject('unsupported');
  const parts = relative(root, log).split(sep);
  if (parts.length !== 3 || parts[0] === '..' || parts[1] !== id) reject('unsupported');
  const realRoot = await realpath(root);
  const project = join(realRoot, parts[0]);
  const dir = join(project, id);
  const physicalLog = join(dir, parts[2]);
  const [projectStat, dirStat, fileStat, resolvedLog] = await Promise.all([
    identity(project), identity(dir), identity(physicalLog), realpath(log),
  ]);
  if (!projectStat.isDirectory() || !dirStat.isDirectory() || !fileStat.isFile() || resolvedLog !== physicalLog ||
      projectStat.dev !== dirStat.dev || dirStat.dev !== fileStat.dev) reject('unsupported');
  return { root: realRoot, dir, dirStat, fileStat, physicalLog };
}

/** Single confirmed operation, with no agent tool or generic filesystem endpoint. */
export function createDeletion(ctx, enabled = () => true) {
  let busy = false;
  return async (id, signal) => {
    if (!enabled()) return { ok: false, code: 'unavailable' };
    if (typeof id !== 'string' || !SESSION_ID.test(id)) return { ok: false, code: 'invalid' };
    if (busy) return { ok: false, code: 'concurrent' };
    busy = true;
    let lease, stage, staged, retained;
    let renamed = false;
    try {
      if (process.platform === 'win32') reject('unsupported');
      const { sessionPersistence: persistence, sessions, agents, workspaceRegistry: registry, storageDomain } = ctx;
      const live = () => sessions.get(id) || agents.get(id);
      if (live()) reject('active');
      const snapshot = await persistence.stat(id, { signal });
      if (!snapshot) reject('missing');
      if (snapshot.header?.origin === 'subagent' || snapshot.header?.parentSession) reject('dependent');
      const location = await locate(persistence, snapshot, id);
      const workspaces = registry.list().filter((workspace) => workspace.sessionIds.includes(id));
      const cache = storageDomain.get('session_projcache')?.table('sessions');
      if (!cache || workspaces.some((workspace) => typeof workspace.detachSession !== 'function')) reject('unsupported');
      // Same lock normal sessions use; never stop an Agent or force an active writer out.
      lease = await persistence.open(id, 'write', { signal });
      const children = await persistence.list({ signal });
      if (children.some(({ header }) => header.parentSession === id) ||
          sessions.list().some((session) => session.header?.parentSession === id)) reject('dependent');
      const latest = await persistence.stat(id, { signal });
      if (live()) reject('active');
      if (!latest || latest.revision !== snapshot.revision || latest.location?.path !== snapshot.location.path) reject('active');
      // Current JSONL storage is flat. Refuse unexpected subdirectories, links and mount points.
      for (const entry of await readdir(location.dir)) {
        const file = await identity(join(location.dir, entry));
        if (!file.isFile() || file.dev !== location.dirStat.dev) reject('unsupported');
      }
      stage = await mkdtemp(join(dirname(location.root), '.dsh-tietiezhi-delete-'));
      retained = await identity(stage);
      if (retained.dev !== location.dirStat.dev || await realpath(stage) !== stage) reject('unsupported');
      staged = join(stage, id);
      const check = await locate(persistence, latest, id);
      if (live() || !same(check.dirStat, location.dirStat) || !same(check.fileStat, location.fileStat)) reject('active');
      signal?.throwIfAborted();
      if (!enabled()) reject('unavailable');
      await rename(location.dir, staged);
      renamed = true;
      if (!same(await identity(staged), location.dirStat) || await realpath(staged) !== staged ||
          !same(await identity(join(staged, basename(location.physicalLog))), location.fileStat)) {
        // Do not delete, or overwrite a potentially re-created source, on an identity mismatch.
        reject('partial');
      }
      await lease.close();
      lease = undefined;
      const cleanup = await Promise.allSettled([
        ...workspaces.map((workspace) => workspace.detachSession(id)), cache.delete(id),
        registry.unpinSession(id), registry.unarchiveSession(id),
      ]);
      ctx.emit('api-session/removed', id);
      // Do not recursively remove any other path (including the staging parent).
      if (!same(await identity(stage), retained) || !same(await identity(staged), location.dirStat) || await realpath(staged) !== staged) reject('partial');
      await rm(staged, { recursive: true });
      if (cleanup.some((result) => result.status === 'rejected')) throw new AggregateError(cleanup.filter((r) => r.status === 'rejected').map((r) => r.reason), 'Session metadata cleanup failed');
      return { ok: true };
    } catch (error) {
      const code = renamed ? 'partial' : error instanceof Refusal ? error.code :
        error?.name === 'SessionAlreadyOwnedError' ? 'active' : error?.name === 'SessionPersistenceNotFoundError' ? 'missing' : 'failed';
      if (code === 'partial' || code === 'failed') ctx.logger.warn('dsh-tietiezhi deletion (%s, %s), staging: %s: %s', id, code, staged ?? 'none', error);
      return { ok: false, code };
    } finally {
      try {
        await lease?.close();
      } catch (error) { ctx.logger.warn('dsh-tietiezhi could not close deletion lease: %s', error); }
      try {
        if (stage && retained && same(await identity(stage), retained)) await rmdir(stage);
      } catch (error) {
        if (!['ENOENT', 'ENOTEMPTY'].includes(error?.code)) ctx.logger.warn('dsh-tietiezhi could not remove empty staging directory: %s', error);
      } finally { busy = false; }
    }
  };
}

const json = (result) => Response.json(result, { status: result.ok ? 200 : status[result.code], headers: { 'cache-control': 'no-store' } });

export function createHandler(connection, remove) {
  return async (request) => {
    // Connection checks Host/Origin and the signed credential. Desktop forwarding deliberately
    // strips Origin, so require the authenticated browser peer and a non-form action header;
    // validate same-authority Origin when supplied, rather than rejecting the Desktop carrier.
    const origin = request.headers.get('origin');
    const host = request.headers.get('host');
    const admission = connection.admit({ headers: request.headers });
    if (request.method !== 'POST' || admission.peer?.id !== connection.browserPeer.id ||
        !host || request.headers.get('sec-fetch-site') === 'cross-site' ||
        request.headers.get('x-dsh-tietiezhi-action') !== 'delete-session') return json({ ok: false, code: 'forbidden' });
    if (origin !== null) {
      try {
        const url = new URL(origin);
        if (!['http:', 'https:'].includes(url.protocol) || origin !== url.origin || new URL(`${url.protocol}//${host}`).origin !== origin) reject('forbidden');
      } catch { return json({ ok: false, code: 'forbidden' }); }
    }
    try {
      if (request.headers.get('content-type')?.split(';')[0].trim().toLowerCase() !== 'application/json' || !request.body) reject('invalid');
      const reader = request.body.getReader();
      let body = '';
      let size = 0;
      const decoder = new TextDecoder('utf-8', { fatal: true });
      try {
        for (;;) {
          const { done, value } = await reader.read();
          if (done) break;
          size += value.byteLength;
          if (size > LIMIT) reject('large');
          body += decoder.decode(value, { stream: true });
        }
        body += decoder.decode();
      } finally { reader.releaseLock(); }
      const input = JSON.parse(body);
      if (!input || Object.keys(input).length !== 1 || !Object.hasOwn(input, 'sessionId')) reject('invalid');
      return json(await remove(input.sessionId, request.signal));
    } catch (error) {
      return json({ ok: false, code: error instanceof Refusal ? error.code : 'invalid' });
    }
  };
}

export function apply(ctx) {
  let active = true;
  registerPermanentDelete(ctx, () => active);
  ctx.effect(() => () => { active = false; }, 'dsh-tietiezhi: close deletion gate');
}

export function registerPermanentDelete(ctx, enabled) {
  const remove = createDeletion(ctx, enabled);
  const connection = ctx.connection;
  ctx.effect(() => connection.fetch.register({
    path: route, methods: ['POST'], requestBody: 'streaming',
    fetch: createHandler(connection, remove),
  }), 'dsh-tietiezhi: permanent delete endpoint');
  ctx.effect(() => connection.fetch.register({
    path: `${route}/status`, methods: ['GET'], requestBody: 'buffered',
    fetch: async (request) => {
      const { peer } = connection.admit({ headers: request.headers });
      return peer?.id === connection.browserPeer.id ? json({ ok: true, enabled: enabled() }) : json({ ok: false, code: 'forbidden' });
    },
  }), 'dsh-tietiezhi: permanent delete status');
}
