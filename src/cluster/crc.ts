// CRC-32C (Castagnoli), slicing-by-8. Node's zlib.crc32 is the other
// polynomial, so this one is ours.

const T: Uint32Array[] = Array.from({ length: 8 }, () => new Uint32Array(256));
for (let i = 0; i < 256; i++) {
  let c = i;
  for (let k = 0; k < 8; k++) c = c & 1 ? (c >>> 1) ^ 0x82f63b78 : c >>> 1;
  T[0][i] = c >>> 0;
}
for (let i = 0; i < 256; i++) for (let k = 1; k < 8; k++) T[k][i] = (T[k - 1][i] >>> 8) ^ T[0][T[k - 1][i] & 0xff];
const [T0, T1, T2, T3, T4, T5, T6, T7] = T;

/// Extend a finished CRC (start from 0) with b[from..to).
export function crc32cUpdate(crc: number, b: Uint8Array, from = 0, to = b.length): number {
  let c = ~crc;
  let i = from;
  for (; i + 8 <= to; i += 8) {
    const lo = (b[i] | (b[i + 1] << 8) | (b[i + 2] << 16) | (b[i + 3] << 24)) ^ c;
    const hi = b[i + 4] | (b[i + 5] << 8) | (b[i + 6] << 16) | (b[i + 7] << 24);
    c = T7[lo & 0xff] ^ T6[(lo >>> 8) & 0xff] ^ T5[(lo >>> 16) & 0xff] ^ T4[lo >>> 24] ^
        T3[hi & 0xff] ^ T2[(hi >>> 8) & 0xff] ^ T1[(hi >>> 16) & 0xff] ^ T0[hi >>> 24];
  }
  for (; i < to; i++) c = T0[(c ^ b[i]) & 0xff] ^ (c >>> 8);
  return ~c >>> 0;
}

export function crc32c(b: Uint8Array, from = 0, to = b.length): number {
  return crc32cUpdate(0, b, from, to);
}
