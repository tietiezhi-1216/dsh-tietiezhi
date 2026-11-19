// Root entry delivers the shared browser artifact and gates all feature components.
export function apply(ctx) {
  ctx.provide('tietiezhiBundle', Object.freeze({}));
}
