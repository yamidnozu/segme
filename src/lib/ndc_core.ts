// ndc_core.ts - NDC29 (HSL16 + MINM3 + lazy+3)
const dec = new TextDecoder();
export const fmt = (n: number | null | undefined) => n == null ? "N/A" : n < 1024 ? `${n} B` : n < 1048576 ? `${(n / 1024).toFixed(2)} KB` : `${(n / 1048576).toFixed(2)} MB`;
export const sha256 = async (b: Uint8Array) => new Uint8Array(await crypto.subtle.digest("SHA-256", b.buffer.slice(b.byteOffset, b.byteOffset + b.byteLength) as ArrayBuffer));
const ru32 = (b: Uint8Array, p: number) => new DataView(b.buffer, b.byteOffset + p, 4).getUint32(0, false);
const concat = (a: Uint8Array[]) => { const s = a.reduce((x, y) => x + y.length, 0); const o = new Uint8Array(s); let p = 0; for (const v of a) { o.set(v, p); p += v.length } return o };
export const MAGIC = new TextEncoder().encode("NDC20"); export const VERSION = 1;

class BW { private c: Uint8Array[] = []; private b = new Uint8Array(65536); private p = 0; wB(x: number) { if (this.b.length - this.p < 1) { this.c.push(this.b.slice(0, this.p)); this.p = 0 } this.b[this.p++] = x & 255 } wA(d: Uint8Array) { if (!d.length) return; this.c.push(this.b.slice(0, this.p)); this.p = 0; this.c.push(d) } wU(n: number) { this.wB(n >>> 24); this.wB(n >>> 16); this.wB(n >>> 8); this.wB(n) } finish() { this.c.push(this.b.slice(0, this.p)); return concat(this.c) } }

const HB = 16, HS = 1 << HB, HSL = 16, HM = HS - 1, WIN = 4 * 1024 * 1024, MINM = 3, MAXM = 258, MINR = 8;
const h4 = (d: Uint8Array, p: number) => p + 3 >= d.length ? 0 : (Math.imul(d[p], 2654435761) ^ Math.imul(d[p + 1], 2246822519) ^ Math.imul(d[p + 2], 3266489917) ^ Math.imul(d[p + 3], 668265263)) >>> 0 & HM;
const cnt = (d: Uint8Array, a: number, b: number, m: number) => { let l = 0; const lim = Math.min(m, d.length - b); while (l < lim && d[a + l] === d[b + l]) l++; return l };
const ctx4 = (p: number) => p < 64 ? 0 : p < 128 ? 1 : p < 192 ? 2 : 3;
const ctx16 = (p: number) => p >>> 4;

function huffEnc(d: Uint8Array) { if (d.length < 32) return null; const f = new Uint32Array(256); for (const x of d) f[x]++; const h: { s: number; f: number; l?: any; r?: any }[] = []; for (let i = 0; i < 256; i++)if (f[i]) h.push({ s: i, f: f[i] }); if (h.length < 2) return null; h.sort((a, b) => a.f - b.f); while (h.length > 1) { const a = h.shift()!, b = h.shift()!, n = { s: -1, f: a.f + b.f, l: a, r: b }; let i = 0; while (i < h.length && h[i].f <= n.f) i++; h.splice(i, 0, n) } const ln = new Uint8Array(256); const w = (n: any, d: number) => { if (n.s >= 0) ln[n.s] = d; else { w(n.l, d + 1); w(n.r, d + 1) } }; w(h[0], 1); const maxLen = ln.reduce((m, v) => Math.max(m, v), 0); if (maxLen > 24) return null; const s = Array.from({ length: 256 }, (_, i) => i).filter(i => ln[i]).sort((a, b) => ln[a] - ln[b] || a - b); const c = new Uint32Array(256); let cd = 0, pl = 0; for (const x of s) { const l = ln[x]; if (l > pl) { cd <<= l - pl; pl = l } c[x] = cd++ } const bw = new BW(); let a = 0, b = 0, bl = 0; const wb = (v: number, l: number) => { a = ((a << l) | v) >>> 0; b += l; bl += l; while (b >= 8) { b -= 8; bw.wB((a >>> b) & 255); a &= ((1 << b) - 1) } }; for (const x of d) wb(c[x], ln[x]); const rl = bl; if (b) wb(0, 8 - b); return { t: ln, p: bw.finish(), b: rl } }
function huffDec(t: Uint8Array, p: Uint8Array, ln: number, bl: number) { const maxLen = Math.max(...t); if (!maxLen) return new Uint8Array(ln); if (maxLen > 24) throw new Error("ml"); const syms = Array.from({ length: 256 }, (_, i) => i).filter(i => t[i]).sort((a, b) => t[a] - t[b] || a - b); const codes = new Uint32Array(256); let cd = 0, prev = 0; for (const s of syms) { const l = t[s]; if (l > prev) { cd <<= l - prev; prev = l } codes[s] = cd++ } const tree: { l?: number; r?: number; sym?: number }[] = [{}]; for (const s of syms) { const l = t[s]; const c = codes[s]; let n = 0; for (let i = l - 1; i >= 0; i--) { const bit = (c >>> i) & 1; if (bit === 0) { if (tree[n].l === undefined) { tree[n].l = tree.length; tree.push({}) } n = tree[n].l! } else { if (tree[n].r === undefined) { tree[n].r = tree.length; tree.push({}) } n = tree[n].r! } } tree[n].sym = s } const out = new Uint8Array(ln); let op = 0, n = 0; for (let i = 0; i < bl && op < ln; i++) { const bit = (p[i >>> 3] >>> (7 - (i & 7))) & 1; const nx = bit ? tree[n].r : tree[n].l; if (nx === undefined) throw new Error("p"); n = nx; if (tree[n].sym !== undefined) { out[op++] = tree[n].sym!; n = 0 } } if (op !== ln) throw new Error("lh"); return out }

function lzEncode29(data: Uint8Array, mode: 'A' | 'B' | 'C' = 'B') {
  const t = new Int32Array(HS * HSL); t.fill(-1);
  const types: number[] = []; const litLens: number[] = []; const matchLens: number[] = []; const distBuckets: number[] = []; const distExtras: number[] = []; const distExtraBits: number[] = [];
  const runVals: number[] = []; const runLens: number[] = []; const lits: number[] = []; const lits4: number[][] = [[], [], [], []]; const lits16: number[][] = Array.from({ length: 16 }, () => []);
  const upd = (p: number) => { if (p + 3 >= data.length) return; const h = h4(data, p); const b = h * HSL; for (let k = HSL - 1; k > 0; k--) t[b + k] = t[b + k - 1]; t[b] = p };
  const find = (p: number) => { if (p + MINM > data.length) return null; const h = h4(data, p); const b = h * HSL; let bl = 0, bd = 0; for (let k = 0; k < HSL; k++) { const o = t[b + k]; if (o < 0) continue; const d = p - o; if (d <= 0 || d > WIN) continue; const l = cnt(data, o, p, MAXM); if (l > bl || (l === bl && d < bd)) { bl = l; bd = d } } return bl >= MINM ? { l: bl, d: bd } : null };
  const distBucket = (d: number) => 31 - Math.clz32(d | 1);
  const worth = (m: { l: number, d: number } | null) => { if (!m) return null; if (mode === 'C') return m; const b = distBucket(m.d); const need = mode === 'A' ? 4 + (b >> 2) : (b < 8 ? 3 : 4 + Math.max(0, (b - 6) >> 2)); return m.l >= need ? m : null };
  let buf: number[] = [];
  const flush = (pos: number) => { if (!buf.length) return; types.push(0); litLens.push(buf.length); for (let k = 0; k < buf.length; k++) { const b = buf[k]; lits.push(b); const prev = (pos - buf.length + k > 0) ? data[pos - buf.length + k - 1] : 0; lits4[ctx4(prev)].push(b); lits16[ctx16(prev)].push(b) } buf = [] };
  for (let i = 0; i < data.length;) {
    let r = 1; const v = data[i]; while (i + r < data.length && data[i + r] === v && r < 65535) r++;
    if (r >= MINR) { flush(i); types.push(2); runVals.push(v); runLens.push(r); upd(i); if (r > 1) upd(i + r - 1); i += r; continue }
    let m1 = worth(find(i)); upd(i); let m2 = i + 1 < data.length ? worth(find(i + 1)) : null;
    if (m2 && m1 && m2.l > m1.l + 3) { buf.push(data[i]); i++; if (buf.length === 255) flush(i); m1 = m2; upd(i) }
    if (m1 && (!m2 || m1.l >= m2.l)) { flush(i); types.push(1); matchLens.push(m1.l - MINM); const b = distBucket(m1.d); distBuckets.push(b); distExtras.push(m1.d - (1 << b)); distExtraBits.push(b); upd(i + 1); if (m1.l > 8) upd(i + (m1.l >> 1)); if (m1.l > 16) upd(i + m1.l - 4); upd(i + m1.l - 1); i += m1.l } else { buf.push(data[i]); i++; if (buf.length === 255) flush(i) }
  }
  flush(data.length);
  return { types: new Uint8Array(types), litLens: new Uint8Array(litLens), matchLens: new Uint8Array(matchLens), distBuckets: new Uint8Array(distBuckets), distExtras, distExtraBits, runVals: new Uint8Array(runVals), runLens, lits: new Uint8Array(lits), lits4: lits4.map(a => new Uint8Array(a)), lits16: lits16.map(a => new Uint8Array(a)) };
}

export async function compressNDC_AB(data: Uint8Array, log: (x: string) => void = () => { }): Promise<{ file: Uint8Array; hash: Uint8Array }> {
  const hash = await sha256(data);
  let mode: 'A' | 'B' | 'C' = 'B';
  if (data.length > 768 * 1024) {
    const sz = 256 * 1024; const sample = new Uint8Array(Math.min(data.length, sz * 3));
    if (data.length <= sz * 3) sample.set(data.subarray(0, sample.length)); else { sample.set(data.subarray(0, sz), 0); const mid = Math.floor(data.length / 2 - sz / 2); sample.set(data.subarray(mid, mid + sz), sz); sample.set(data.subarray(data.length - sz), sz * 2) }
    const sc = (d: Uint8Array) => { const h = huffEnc(d); const raw = 1 + 4 + d.length; const huff = h ? 1 + 4 + 256 + 4 + 4 + h.p.length : Infinity; return Math.min(raw, huff) };
    const estimate = (tmp: ReturnType<typeof lzEncode29>) => { let eb = 0; for (let i = 0; i < tmp.distExtraBits.length; i++)eb += tmp.distExtraBits[i]; const extras = 1 + 4 + 4 + 4 + Math.ceil(eb / 8); const rl = new Uint8Array(tmp.runLens.length * 2); const dv = new DataView(rl.buffer); tmp.runLens.forEach((x, i) => dv.setUint16(i * 2, x, false)); const base = sc(tmp.types) + sc(tmp.litLens) + sc(tmp.matchLens) + sc(tmp.distBuckets) + extras + sc(tmp.runVals) + sc(rl); const n = sc(tmp.lits); const c4 = 1 + tmp.lits4.reduce((s, a) => s + sc(a), 0); const c16 = 1 + tmp.lits16.reduce((s, a) => s + sc(a), 0); return base + Math.min(n, c4, c16) };
    let best = Infinity; for (const m of ['A', 'B', 'C'] as const) { const tmp = lzEncode29(sample, m); const score = estimate(tmp); if (score < best) { best = score; mode = m } }
  }
  const s = lzEncode29(data, mode);
  const w = new BW(); w.wA(MAGIC); w.wB(VERSION); w.wB(2); w.wU(data.length); w.wA(hash); w.wB(1);
  const ws = (d: Uint8Array) => { const h = huffEnc(d); if (h && h.p.length + 269 < d.length) { w.wB(1); w.wU(d.length); w.wA(h.t); w.wU(h.b); w.wU(h.p.length); w.wA(h.p); return true } else { w.wB(0); w.wU(d.length); w.wA(d); return false } };
  ws(s.types); ws(s.litLens); ws(s.matchLens); ws(s.distBuckets);
  const ebw = new BW(); let acc = 0, bits = 0, totalBits = 0; for (let i = 0; i < s.distExtras.length; i++) { const b = s.distExtraBits[i]; const v = s.distExtras[i]; acc = (acc << b) | v; bits += b; totalBits += b; while (bits >= 8) { bits -= 8; ebw.wB((acc >>> bits) & 255) } } if (bits > 0) ebw.wB((acc << (8 - bits)) & 255); const extras = ebw.finish(); w.wB(0); w.wU(s.distExtras.length); w.wU(totalBits); w.wU(extras.length); w.wA(extras);
  ws(s.runVals); const rl = new Uint8Array(s.runLens.length * 2); const dv = new DataView(rl.buffer); s.runLens.forEach((x, i) => dv.setUint16(i * 2, x, false)); ws(rl);
  const sc = (d: Uint8Array) => { const h = huffEnc(d); const raw = 1 + 4 + d.length; const huff = h ? 1 + 4 + 256 + 4 + 4 + h.p.length : Infinity; return Math.min(raw, huff) }; const nCost = sc(s.lits); const c4Cost = 1 + s.lits4.reduce((sum, a) => sum + sc(a), 0); const c16Cost = 1 + s.lits16.reduce((sum, a) => sum + sc(a), 0); let litMode = 0; if (c4Cost < nCost && c4Cost <= c16Cost) litMode = 1; else if (c16Cost < nCost && c16Cost < c4Cost) litMode = 2;
  w.wB(litMode); if (litMode === 0) { ws(s.lits) } else if (litMode === 1) { for (let i = 0; i < 4; i++)ws(s.lits4[i]) } else { for (let i = 0; i < 16; i++)ws(s.lits16[i]) }
  const f = w.finish(); log(`NDC29-${mode} LIT${litMode === 0 ? 'N' : litMode === 1 ? '4' : '16'} ${fmt(f.length)}`); return { file: f, hash };
}

export async function decompressNDC(f: Uint8Array): Promise<Uint8Array> {
  let p = 0; if (dec.decode(f.slice(0, 5)) !== "NDC20") throw new Error("bad"); p += 5; const ver = f[p++]; if (ver !== VERSION) throw new Error("v"); const ph = f[p++]; if (ph !== 2) throw new Error("phase"); const sz = ru32(f, p); p += 4; const hh = f.slice(p, p + 32); p += 32; const codec = f[p++]; if (codec !== 1) throw new Error("codec");
  const rd = () => { const m = f[p++]; const l = ru32(f, p); p += 4; if (m === 1) { const tb = f.slice(p, p + 256); p += 256; const bl = ru32(f, p); p += 4; const pl = ru32(f, p); p += 4; const d = huffDec(tb, f.slice(p, p + pl), l, bl); p += pl; return d } else { const d = f.slice(p, p + l); p += l; return d } };
  const types = rd(); const litLens = rd(); const matchLens = rd(); const distBuckets = rd(); p++; const dc = ru32(f, p); p += 4; const totalBits = ru32(f, p); p += 4; const el = ru32(f, p); p += 4; const extras = f.slice(p, p + el); p += el; const runVals = rd(); const rl = rd();
  const litMode = f[p++]; let litsArr: Uint8Array[]; if (litMode === 0) { litsArr = [rd()] } else if (litMode === 1) { litsArr = [rd(), rd(), rd(), rd()] } else { litsArr = Array.from({ length: 16 }, () => rd()) }
  const runLens = new Array(rl.length / 2); const dv = new DataView(rl.buffer, rl.byteOffset); for (let i = 0; i < runLens.length; i++)runLens[i] = dv.getUint16(i * 2, false);
  const dists: number[] = []; let bitPos = 0; for (let i = 0; i < dc; i++) { const b = distBuckets[i]; const base = 1 << b; let v = 0; for (let j = 0; j < b && bitPos < totalBits; j++) { const byte = bitPos >> 3; const bit = 7 - (bitPos & 7); v = (v << 1) | ((extras[byte] >> bit) & 1); bitPos++ } dists.push(base + v) }
  const o = new Uint8Array(sz); let op = 0, mi = 0, di = 0, ri = 0, ti = 0; const ptr = litsArr.map(() => 0);
  for (let i = 0; i < types.length; i++) { const t = types[i]; if (t === 0) { const l = litLens[ti++]; for (let k = 0; k < l; k++) { let ctx = 0; if (litMode === 1) ctx = op > 0 ? ctx4(o[op - 1]) : 0; else if (litMode === 2) ctx = op > 0 ? ctx16(o[op - 1]) : 0; if (ptr[ctx] >= litsArr[ctx].length) throw new Error("lits"); o[op++] = litsArr[ctx][ptr[ctx]++] } } else if (t === 1) { const l = matchLens[mi++] + MINM; const d = dists[di++]; const s = op - d; if (s < 0 || op + l > sz) throw new Error("match"); for (let k = 0; k < l; k++)o[op++] = o[s + k] } else { const v = runVals[ri]; const l = runLens[ri++]; if (op + l > sz) throw new Error("run"); o.fill(v, op, op + l); op += l } }
  if (op !== sz) throw new Error("sz"); if (!(await sha256(o)).every((v, i) => v === hh[i])) throw new Error("h"); return o;
}

export const compressNDC = compressNDC_AB;
export async function deflateEnc(d: Uint8Array): Promise<Uint8Array> { const { deflate } = await import("pako"); return deflate(d, { level: 9 }) }