export type DurationInput = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`;

export function parseDuration(value: DurationInput): number {
  if (typeof value === 'number') {
    if (!Number.isFinite(value) || value <= 0) {
      throw new TypeError(
        'duration must be a positive finite number of milliseconds',
      );
    }
    return value;
  }

  const match = /^(\d+)(ms|s|m|h|d)$/.exec(value);
  if (!match) {
    throw new TypeError(`unsupported duration '${value}'`);
  }

  const amount = Number(match[1]);
  const unit = match[2];
  const multiplier =
    unit === 'ms'
      ? 1
      : unit === 's'
        ? 1_000
        : unit === 'm'
          ? 60_000
          : unit === 'h'
            ? 3_600_000
            : 86_400_000;
  return amount * multiplier;
}
