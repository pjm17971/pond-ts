/** Shared test schema and helpers for @pond-ts/react tests. */

export const schema = [
  { name: 'time', kind: 'time' },
  { name: 'cpu', kind: 'number' },
  { name: 'host', kind: 'string' },
] as const;

export type Schema = typeof schema;

/** Produce a row tuple with an absolute timestamp offset by `i` minutes. */
export function row(
  i: number,
  cpu: number,
  host = 'api-1',
): [Date, number, string] {
  return [new Date(1_700_000_000_000 + i * 60_000), cpu, host];
}
