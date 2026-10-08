//! Open-addressed order-id → pool-index map. Fibonacci hash with high-bit
//! fold (sequential ids distribute evenly), power-of-two table, linear probe.

export class OrderMap {
  private keys: Float64Array;
  private vals: Int32Array;
  private used: Uint8Array;
  private mask: number;
  private count = 0;

  constructor(capHint: number) {
    let cap = 16;
    while (cap < capHint * 2) cap <<= 1;
    this.keys = new Float64Array(cap);
    this.vals = new Int32Array(cap);
    this.used = new Uint8Array(cap);
    this.mask = cap - 1;
  }

  private static mix(id: number): number {
    let h = Math.imul(id | 0, 0x9e3779b1) | 0;
    h ^= h >>> 16;
    return h >>> 0;
  }

  contains(id: number): boolean {
    return this.find(id) >= 0;
  }

  get(id: number): number {
    const i = this.find(id);
    return i < 0 ? -1 : this.vals[i];
  }

  private find(id: number): number {
    let i = OrderMap.mix(id) & this.mask;
    for (;;) {
      if (!this.used[i]) return -1;
      if (this.keys[i] === id) return i;
      i = (i + 1) & this.mask;
    }
  }

  insert(id: number, val: number): void {
    if (this.count * 4 >= this.keys.length * 3) this.grow();
    let i = OrderMap.mix(id) & this.mask;
    while (this.used[i]) i = (i + 1) & this.mask;
    this.used[i] = 1;
    this.keys[i] = id;
    this.vals[i] = val;
    this.count++;
  }

  remove(id: number): void {
    let i = this.find(id);
    if (i < 0) return;
    // Backward-shift deletion keeps probe chains intact (no tombstones).
    for (;;) {
      let j = i;
      this.used[i] = 0;
      for (;;) {
        j = (j + 1) & this.mask;
        if (!this.used[j]) {
          this.count--;
          return;
        }
        const k = OrderMap.mix(this.keys[j]) & this.mask;
        // Element at j may move to i only if its ideal slot k is NOT in the
        // cyclic interval (i, j].
        const kInPath = i <= j ? k > i && k <= j : k > i || k <= j;
        if (!kInPath) break;
      }
      this.keys[i] = this.keys[j];
      this.vals[i] = this.vals[j];
      this.used[i] = 1;
      i = j;
    }
  }

  private grow(): void {
    const ok = this.keys, ov = this.vals, ou = this.used;
    const cap = ok.length << 1;
    this.keys = new Float64Array(cap);
    this.vals = new Int32Array(cap);
    this.used = new Uint8Array(cap);
    this.mask = cap - 1;
    this.count = 0;
    for (let i = 0; i < ou.length; i++) {
      if (ou[i]) this.insert(ok[i], ov[i]);
    }
  }
}
