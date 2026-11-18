// The root owns the shared browser module and the bundle-wide activation gate.
export function apply(ctx) {
  ctx.provide('tietiezhiBundle', Object.freeze({}));
}
