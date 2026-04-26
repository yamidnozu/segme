// ndc_core.ts - NDC22 (streams + distance buckets) FIXED

const dec = new TextDecoder();
export const fmt = (n: number | null | undefined) => n == null ? "N/A" : n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(2)} KB` : `${(n / 1048576).toFixed(2)} MB`;
export const sha256 = async (b: Uint8Array) => new Uint8Array(await crypto.subtle.digest("SHA-256", b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer));
const ru32 = (b: Uint8Array, p: number) => new DataView(b.buffer, b.byteOffset + p, 4).getUint32(0, false);
const concat = (a: Uint8Array[]) => { const s = a.reduce((x, y) => x + y.length, 0); const o = new Uint8Array(s); let p = 0; for (const v of a) { o.set(v, p); p += v.length } return o };

export const MAGIC = new TextEncoder().encode("NDC20");
export const VERSION = 1;

class BW {
  private c: Uint8Array[] = []; private b = new Uint8Array(65536); private p = 0;
  wB(x: number) { if (this.b.length - this.p < 1) { this.c.push(this.b.slice(0, this.p)); this.p = 0 } this.b[this.p++] = x & 255 }
  wA(d: Uint8Array) { if (!d.length) return; this.c.push(this.b.slice(0, this.p)); this.p = 0; this.c.push(d) }
  wU(n: number) { this.wB(n >>> 24); this.wB(n >>> 16); this.wB(n >>> 8); this.wB(n) }
  wV(n: number) { while (n >= 0x80) { this.wB((n & 0x7f) | 0x80); n >>>= 7 } this.wB(n) }
  finish() { this.c.push(this.b.slice(0, this.p)); return concat(this.c) }
}

const HB = 16, HS = 1 << HB, HSL = 8, HM = HS - 1, WIN = 4 * 1024 * 1024, MINM = 6, MAXM = 258, MINR = 8;
const h4 = (d: Uint8Array, p: number) => p + 3 >= d.length ? 0 : (Math.imul(d[p], 2654435761) ^ Math.imul(d[p + 1], 2246822519) ^ Math.imul(d[p + 2], 3266489917) ^ Math.imul(d[p + 3], 668265263)) >>> 0 & HM;
const cnt = (d: Uint8Array, a: number, b: number, m: number) => { let l = 0; const lim = Math.min(m, d.length - b); while (l < lim && d[a + l] === d[b + l]) l++; return l };

function huffEnc(d: Uint8Array): { t: Uint8Array; p: Uint8Array; b: number } | null {
  if (d.length < 32) return null; const f = new Uint32Array(256); for (const x of d) f[x]++; const h: { s: number; f: number; l?: any; r?: any }[] = []; for (let i = 0; i < 256; i++)if (f[i]) h.push({ s: i, f: f[i] }); if (h.length < 2) return null;
  h.sort((a, b) => a.f - b.f); while (h.length > 1) { const a = h.shift()!, b = h.shift()!, n = { s: -1, f: a.f + b.f, l: a, r: b }; let i = 0; while (i < h.length && h[i].f <= n.f) i++; h.splice(i, 0, n) }
  const ln = new Uint8Array(256); const w = (n: any, d: number) => { if (n.s >= 0) ln[n.s] = d; else { w(n.l, d + 1); w(n.r, d + 1) } }; w(h[0], 1); const maxLen = ln.reduce((m, v) => Math.max(m, v), 0); if (maxLen > 24) return null;
  const s = Array.from({ length: 256 }, (_, i) => i).filter(i => ln[i]).sort((a, b) => ln[a] - ln[b] || a - b); const c = new Uint32Array(256); let cd = 0, pl = 0; for (const x of s) { const l = ln[x]; if (l > pl) { cd <<= l - pl; pl = l } c[x] = cd++ }
  const bw = new BW(); let a = 0, b = 0, bl = 0; const wb = (v: number, l: number) => { a = ((a << l) | v) >>> 0; b += l; bl += l; while (b >= 8) { b -= 8; bw.wB((a >>> b) & 255); a &= ((1 << b) - 1) } }; for (const x of d) wb(c[x], ln[x]); const rl = bl; if (b) wb(0, 8 - b); return { t: ln, p: bw.finish(), b: rl };
}

function huffDec(t: Uint8Array, p: Uint8Array, ln: number, bl: number): Uint8Array {
  const maxLen = Math.max(...t); if (!maxLen) return new Uint8Array(ln); if (maxLen > 24) throw new Error("ml"); const syms = Array.from({ length: 256 }, (_, i) => i).filter(i => t[i]).sort((a, b) => t[a] - t[b] || a - b);
  const codes = new Uint32Array(256); let cd = 0, prev = 0; for (const s of syms) { const l = t[s]; if (l > prev) { cd <<= l - prev; prev = l } codes[s] = cd++ }
  const tree: { l?: number; r?: number; sym?: number }[] = [{}]; for (const s of syms) { const l = t[s]; const c = codes[s]; let n = 0; for (let i = l - 1; i >= 0; i--) { const bit = (c >>> i) & 1; if (bit === 0) { if (tree[n].l === undefined) { tree[n].l = tree.length; tree.push({}) } n = tree[n].l! } else { if (tree[n].r === undefined) { tree[n].r = tree.length; tree.push({}) } n = tree[n].r! } } tree[n].sym = s }
  const out = new Uint8Array(ln); let op = 0, n = 0; for (let i = 0; i < bl && op < ln; i++) { const bit = (p[i >>> 3] >>> (7 - (i & 7))) & 1; const nx = bit ? tree[n].r : tree[n].l; if (nx === undefined) throw new Error("p"); n = nx; if (tree[n].sym !== undefined) { out[op++] = tree[n].sym!; n = 0 } } if (op !== ln) throw new Error("lh"); return out;
}

function lzEncode22(data: Uint8Array) {
  const t = new Int32Array(HS * HSL); t.fill(-1);
  const types: number[] = []; const litLens: number[] = []; const matchLens: number[] = []; const distBuckets: number[] = []; const distExtras: number[] = []; const distExtraBits: number[] = [];
  const runVals: number[] = []; const runLens: number[] = []; const lits: number[] = [];

  const upd = (p: number) => { if (p + 4 > data.length) return; const h = h4(data, p); const b = h * HSL; for (let k = HSL - 1; k > 0; k--)t[b + k] = t[b + k - 1]; t[b] = p };
  const find = (p: number) => { if (p + MINM > data.length) return null; const h = h4(data, p); const b = h * HSL; let bl = 0, bd = 0; for (let k = 0; k < HSL; k++) { const o = t[b + k]; if (o < 0) continue; const d = p - o; if (d <= 0 || d > WIN) continue; const l = cnt(data, o, p, MAXM); if (l > bl || (l === bl && d < bd)) { bl = l; bd = d } } return bl >= MINM ? { l: bl, d: bd } : null };
  const distBucket = (d: number) => { let b = 0; let v = 1; while (v * 2 <= d && b < 30) { v *= 2; b++ } return b };

  let buf: number[] = [];
  for (let i = 0; i < data.length;) {
    let r = 1; while (i + r < data.length && data[i + r] === data[i] && r < 65535) r++;
    if (r >= MINR) {
      if (buf.length) { types.push(0); litLens.push(buf.length); lits.push(...buf); buf = [] }
      types.push(2); runVals.push(data[i]); runLens.push(r); upd(i); if (r > 1) upd(i + r - 1); i += r; continue
    }
    const m1 = find(i); upd(i); const m2 = i + 1 < data.length ? find(i + 1) : null;
    if (m1 && (!m2 || m1.l >= m2.l + 1)) {
      if (buf.length) { types.push(0); litLens.push(buf.length); lits.push(...buf); buf = [] }
      types.push(1); matchLens.push(m1.l - MINM); // store as offset (0..252) to fit uint8
      const b = distBucket(m1.d); distBuckets.push(b); const base = 1 << b; distExtras.push(m1.d - base); distExtraBits.push(b);
      if (m1.l > 8) { upd(i + 1); upd(i + (m1.l >> 1)); upd(i + m1.l - 1) } i += m1.l
    } else { buf.push(data[i++]); if (buf.length === 255) { types.push(0); litLens.push(buf.length); lits.push(...buf); buf = [] } }
  }
  if (buf.length) { types.push(0); litLens.push(buf.length); lits.push(...buf) }

  return {
    types: new Uint8Array(types),
    litLens: new Uint8Array(litLens),
    matchLens: new Uint8Array(matchLens),
    distBuckets: new Uint8Array(distBuckets),
    distExtras, distExtraBits,
    runVals: new Uint8Array(runVals),
    runLens,
    lits: new Uint8Array(lits)
  };
}

export async function compressNDC_AB(data: Uint8Array, log: (x: string) => void = () => { }): Promise<{ file: Uint8Array; hash: Uint8Array }> {
  const hash = await sha256(data);
  const s = lzEncode22(data);

  const w = new BW(); w.wA(MAGIC); w.wB(VERSION); w.wB(2); w.wU(data.length); w.wA(hash); w.wB(1);

  const ws = (d: Uint8Array) => {
    const h = huffEnc(d);
    if (h && h.p.length + 269 < d.length) { w.wB(1); w.wU(d.length); w.wA(h.t); w.wU(h.b); w.wU(h.p.length); w.wA(h.p); return true }
    else { w.wB(0); w.wU(d.length); w.wA(d); return false }
  };

  const tH = ws(s.types);
  const llH = ws(s.litLens);
  const mlH = ws(s.matchLens);
  const dbH = ws(s.distBuckets);

  const ebw = new BW(); let acc = 0, bits = 0; let totalBits = 0;
  for (let i = 0; i < s.distExtras.length; i++) {
    const b = s.distExtraBits[i]; const v = s.distExtras[i];
    acc = (acc << b) | v; bits += b; totalBits += b;
    while (bits >= 8) { bits -= 8; ebw.wB((acc >>> bits) & 255) }
  }
  if (bits > 0) ebw.wB((acc << (8 - bits)) & 255);
  const extras = ebw.finish();
  w.wB(0); w.wU(s.distExtras.length); w.wU(totalBits); w.wU(extras.length); w.wA(extras);

  const rvH = ws(s.runVals);
  const rl = new Uint8Array(s.runLens.length * 2); const dv = new DataView(rl.buffer); s.runLens.forEach((x, i) => dv.setUint16(i * 2, x, false));
  const rlH = ws(rl);
  const lH = ws(s.lits);

  const f = w.finish();
  log(`NDC22 T${tH ? 'H' : 'R'} LL${llH ? 'H' : 'R'} ML${mlH ? 'H' : 'R'} DB${dbH ? 'H' : 'R'} RV${rvH ? 'H' : 'R'} RL${rlH ? 'H' : 'R'} L${lH ? 'H' : 'R'} ${fmt(f.length)}`);
  return { file: f, hash };
}

export async function decompressNDC(f: Uint8Array): Promise<Uint8Array> {
  let p = 0; if (dec.decode(f.slice(0, 5)) !== "NDC20") throw new Error("bad"); p += 5; const ver = f[p++]; if (ver !== VERSION) throw new Error("v"); const ph = f[p++]; if (ph !== 2) throw new Error("phase"); const sz = ru32(f, p); p += 4; const hh = f.slice(p, p + 32); p += 32; const codec = f[p++]; if (codec !== 1) throw new Error("codec");

  const rd = () => { const m = f[p++]; const l = ru32(f, p); p += 4; if (m === 1) { const tb = f.slice(p, p + 256); p += 256; const bl = ru32(f, p); p += 4; const pl = ru32(f, p); p += 4; const d = huffDec(tb, f.slice(p, p + pl), l, bl); p += pl; return d } else { const d = f.slice(p, p + l); p += l; return d } };

  const types = rd(); const litLens = rd(); const matchLens = rd(); const distBuckets = rd();
  p++; // consume mode byte (always 0 = raw) written by encoder before extras block
  const dc = ru32(f, p); p += 4; const totalBits = ru32(f, p); p += 4; const el = ru32(f, p); p += 4; const extras = f.slice(p, p + el); p += el;
  const runVals = rd(); const rl = rd(); const lits = rd();

  const runLens = new Array(rl.length / 2); const dv = new DataView(rl.buffer, rl.byteOffset); for (let i = 0; i < runLens.length; i++)runLens[i] = dv.getUint16(i * 2, false);

  const dists: number[] = []; let bitPos = 0;
  for (let i = 0; i < dc; i++) {
    const b = distBuckets[i]; const base = 1 << b; let v = 0;
    for (let j = 0; j < b && bitPos < totalBits; j++) { const byte = bitPos >> 3; const bit = 7 - (bitPos & 7); v = (v << 1) | ((extras[byte] >> bit) & 1); bitPos++ }
    dists.push(base + v);
  }

  const o = new Uint8Array(sz); let op = 0, li = 0, mi = 0, di = 0, ri = 0, ti = 0;
  for (let i = 0; i < types.length; i++) {
    const t = types[i];
    if (t === 0) { const l = litLens[ti++]; o.set(lits.subarray(li, li + l), op); op += l; li += l }
    else if (t === 1) { const l = matchLens[mi++] + MINM; const d = dists[di++]; const s = op - d; for (let k = 0; k < l; k++)o[op++] = o[s + k] }
    else { const v = runVals[ri]; const l = runLens[ri++]; o.fill(v, op, op + l); op += l }
  }
  if (op !== sz) throw new Error("sz"); if (!(await sha256(o)).every((v, i) => v === hh[i])) throw new Error("h"); return o;
}

export const compressNDC = compressNDC_AB;
export async function deflateEnc(d: Uint8Array): Promise<Uint8Array> { const { deflate } = await import("pako"); return deflate(d, { level: 9 }) }