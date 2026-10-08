//! Minimal flat-JSON reader for vector/corpus lines — `{"k":num,"k":"str"}`
//! objects only (no nesting, no escapes needed for our grammar). Keeps the
//! bench binary and golden runner dependency-free.

export function getStr(line: string, key: string): string | undefined {
  const pat = `"${key}":`;
  const start = line.indexOf(pat);
  if (start < 0) return undefined;
  const rest = line.slice(start + pat.length);
  if (rest.startsWith('"')) {
    const end = rest.indexOf('"', 1);
    if (end < 0) return undefined;
    return rest.slice(1, end);
  }
  const m = /^[^,}]*/.exec(rest);
  return m === null ? undefined : m[0].trim();
}

export function getI64(line: string, key: string): number | undefined {
  const s = getStr(line, key);
  if (s === undefined) return undefined;
  const n = Number(s);
  return Number.isFinite(n) ? n : undefined;
}
