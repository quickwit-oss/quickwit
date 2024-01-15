// NOTE: this format duration snippet is inspired by jaeger-ui's utils/date
export const ONE_MILLISECOND = 1000 * 1;
export const ONE_SECOND = 1000 * ONE_MILLISECOND;
const ONE_MINUTE = 60 * ONE_SECOND;
const ONE_HOUR = 60 * ONE_MINUTE;
const ONE_DAY = 24 * ONE_HOUR;
const UNIT_STEPS: { unit: string; ms: number; ofPreviousUnit: number }[] = [
  { unit: 'd', ms: ONE_DAY, ofPreviousUnit: 24 },
  { unit: 'h', ms: ONE_HOUR, ofPreviousUnit: 60 },
  { unit: 'm', ms: ONE_MINUTE, ofPreviousUnit: 60 },
  { unit: 's', ms: ONE_SECOND, ofPreviousUnit: 1000 },
  { unit: 'ms', ms: ONE_MILLISECOND, ofPreviousUnit: 1000 },
  { unit: 'µs', ms: 1, ofPreviousUnit: 1000 },
];

export function formatDuration(duration:number, precise?:boolean) : string {
  // Skip units that are too large
  let i=0;
  while(i < UNIT_STEPS.length - 1) {
    if (UNIT_STEPS[i]!.ms <= duration) break; i++;
  }
  const [primaryUnit, secondaryUnit] = UNIT_STEPS.slice(i)
  // Format value
  const primaryValue = Math.floor(duration / primaryUnit!.ms);
  const primaryUnitString = `${primaryValue}${primaryUnit!.unit}`;
  if (precise && secondaryUnit) {
    const secondaryValue = Math.round((duration / secondaryUnit!.ms) % primaryUnit!.ofPreviousUnit);
    const secondaryUnitString = `${secondaryValue}${secondaryUnit!.unit}`;
    return secondaryValue === 0 ? primaryUnitString : `${primaryUnitString} ${secondaryUnitString}`;
  }
    return primaryUnitString;
}