// ndc_core.ts - NDC20 v2.3 clean baseline

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

const rv = (d: Uint8Array, p: number) => { let v = 0, s = 0, i = p; while (true) { if (i >= d.length) throw new Error("varint EOF"); const b = d[i++]; v |= (b & 0x7f) << s; if (!(b & 0x80)) break; s += 7; if (s > 28) throw new Error("overflow") } return { v: v >>> 0, pos: i } };

const HB = 16, HS = 1 << HB, HSL = 8, HM = HS - 1, WIN = 4 * 1024 * 1024, MINM = 6, MAXM = 258, MINR = 8;
const h4 = (d: Uint8Array, p: number) => p + 3 >= d.length ? 0 : (Math.imul(d[p], 2654435761) ^ Math.imul(d[p + 1], 2246822519) ^ Math.imul(d[p + 2], 3266489917) ^ Math.imul(d[p + 3], 668265263)) >>> 0 & HM;
const cnt = (d: Uint8Array, a: number, b: number, m: number) => { let l = 0; const lim = Math.min(m, d.length - b); while (l < lim && d[a + l] === d[b + l]) l++; return l };

function lzEncode(data: Uint8Array): { t: Uint8Array; l: Uint8Array } {
  const t = new Int32Array(HS * HSL); t.fill(-1); const tok = new BW(); const lit = new BW(); const buf: number[] = [];
  const flush = () => { if (!buf.length) return; tok.wB(0); tok.wV(buf.length); lit.wA(new Uint8Array(buf)); buf.length = 0 };
  const upd = (p: number) => { if (p + 4 > data.length) return; const h = h4(data, p); const b = h * HSL; for (let k = HSL - 1; k > 0; k--)t[b + k] = t[b + k - 1]; t[b] = p };
  const find = (p: number) => { if (p + MINM > data.length) return null; const h = h4(data, p); const b = h * HSL; let bl = 0, bd = 0; for (let k = 0; k < HSL; k++) { const o = t[b + k]; if (o < 0) continue; const d = p - o; if (d <= 0 || d > WIN) continue; const l = cnt(data, o, p, MAXM); if (l > bl || (l === bl && d < bd)) { bl = l; bd = d } } return bl >= MINM ? { l: bl, d: bd } : null };
  for (let i = 0; i < data.length;) {
    let r = 1; while (i + r < data.length && data[i + r] === data[i]) r++; if (r >= MINR) { flush(); tok.wB(2); tok.wB(data[i]); tok.wV(r); upd(i); if (r > 1) upd(i + r - 1); i += r; continue }
    const m1 = find(i); upd(i); const m2 = i + 1 < data.length ? find(i + 1) : null; if (m1 && (!m2 || m1.l >= m2.l + 1)) { flush(); tok.wB(1); tok.wV(m1.d); tok.wV(m1.l); if (m1.l > 8) { upd(i + 1); upd(i + (m1.l >> 1)); upd(i + m1.l - 1) } i += m1.l } else { buf.push(data[i++]); if (buf.length === 127) flush() }
  }
  flush(); return { t: tok.finish(), l: lit.finish() };
}

function lzDecode(t: Uint8Array, l: Uint8Array, sz: number): Uint8Array {
  const o = new Uint8Array(sz); let p = 0, ti = 0, li = 0; while (ti < t.length) { const tp = t[ti++]; if (tp === 0) { const r = rv(t, ti); ti = r.pos; if (li + r.v > l.length || p + r.v > sz) throw new Error("lit overflow"); o.set(l.subarray(li, li + r.v), p); p += r.v; li += r.v } else if (tp === 1) { const d = rv(t, ti); ti = d.pos; const ln = rv(t, ti); ti = ln.pos; const s = p - d.v; if (s < 0 || p + ln.v > sz) throw new Error("bad dist"); for (let k = 0; k < ln.v; k++)o[p++] = o[s + k] } else if (tp === 2) { const b = t[ti++]; const ln = rv(t, ti); ti = ln.pos; if (p + ln.v > sz) throw new Error("run overflow"); o.fill(b, p, p + ln.v); p += ln.v } else throw new Error("bad token") } if (li !== l.length || p !== sz) throw new Error("size mismatch"); return o;
}

function huffEnc(d: Uint8Array): { t: Uint8Array; p: Uint8Array; b: number } | null {
  if (d.length < 32) return null; const f = new Uint32Array(256); for (const x of d) f[x]++; const h: { s: number; f: number; l?: any; r?: any }[] = []; for (let i = 0; i < 256; i++)if (f[i]) h.push({ s: i, f: f[i] }); if (h.length < 2) return null;
  h.sort((a, b) => a.f - b.f); while (h.length > 1) { const a = h.shift()!, b = h.shift()!, n = { s: -1, f: a.f + b.f, l: a, r: b }; let i = 0; while (i < h.length && h[i].f <= n.f) i++; h.splice(i, 0, n) }
  const ln = new Uint8Array(256); const w = (n: any, d: number) => { if (n.s >= 0) ln[n.s] = d; else { w(n.l, d + 1); w(n.r, d + 1) } }; w(h[0], 1); const maxLen = ln.reduce((m, v) => Math.max(m, v), 0); if (maxLen > 24) return null;
  const s = Array.from({ length: 256 }, (_, i) => i).filter(i => ln[i]).sort((a, b) => ln[a] - ln[b] || a - b); const c = new Uint32Array(256); let cd = 0, pl = 0; for (const x of s) { const l = ln[x]; if (l > pl) { cd <<= l - pl; pl = l } c[x] = cd++ }
  const bw = new BW(); let a = 0, b = 0, bl = 0; const wb = (v: number, l: number) => { a = ((a << l) | v) >>> 0; b += l; bl += l; while (b >= 8) { b -= 8; bw.wB((a >>> b) & 255); a &= ((1 << b) - 1) } }; for (const x of d) wb(c[x], ln[x]); const rl = bl; if (b) wb(0, 8 - b); return { t: ln, p: bw.finish(), b: rl };
}

function huffDec(t: Uint8Array, p: Uint8Array, ln: number, bl: number): Uint8Array {
  const maxLen = Math.max(...t); if (!maxLen) return new Uint8Array(ln); if (maxLen > 24) throw new Error("huff maxLen"); const syms = Array.from({ length: 256 }, (_, i) => i).filter(i => t[i]).sort((a, b) => t[a] - t[b] || a - b);
  const codes = new Uint32Array(256); let cd = 0, prev = 0; for (const s of syms) { const l = t[s]; if (l > prev) { cd <<= l - prev; prev = l } codes[s] = cd++ }
  const tree: { l?: number; r?: number; sym?: number }[] = [{}]; for (const s of syms) { const l = t[s]; const c = codes[s]; let n = 0; for (let i = l - 1; i >= 0; i--) { const bit = (c >>> i) & 1; if (bit === 0) { if (tree[n].l === undefined) { tree[n].l = tree.length; tree.push({}) } n = tree[n].l! } else { if (tree[n].r === undefined) { tree[n].r = tree.length; tree.push({}) } n = tree[n].r! } } tree[n].sym = s }
  const out = new Uint8Array(ln); let op = 0, n = 0; for (let i = 0; i < bl && op < ln; i++) { const bit = (p[i >>> 3] >>> (7 - (i & 7))) & 1; const nx = bit ? tree[n].r : tree[n].l; if (nx === undefined) throw new Error("huff path"); n = nx; if (tree[n].sym !== undefined) { out[op++] = tree[n].sym!; n = 0 } } if (op !== ln) throw new Error(`huff len ${op}!=${ln}`); return out;
}

export async function compressNDC_AB(data: Uint8Array, log: (x: string) => void = () => { }): Promise<{ file: Uint8Array; hash: Uint8Array }> {
  const hash = await sha256(data); const { t: toks, l: lits } = lzEncode(data); const ht = huffEnc(toks); const hl = huffEnc(lits);
  const htC = ht ? (1 + 4 + 256 + 4 + 4 + ht.p.length) : Infinity; const rtC = 1 + 4 + toks.length; const useHT = ht != null && htC < rtC;
  const hlC = hl ? (1 + 4 + 256 + 4 + 4 + hl.p.length) : Infinity; const rlC = 1 + 4 + lits.length; const useHL = hl != null && hlC < rlC;
  const w = new BW(); w.wA(MAGIC); w.wB(VERSION); w.wB(1); w.wU(data.length); w.wA(hash); w.wB(1);
  if (useHT) { w.wB(1); w.wU(toks.length); w.wA(ht!.t); w.wU(ht!.b); w.wU(ht!.p.length); w.wA(ht!.p) } else { w.wB(0); w.wU(toks.length); w.wA(toks) }
  if (useHL) { w.wB(1); w.wU(lits.length); w.wA(hl!.t); w.wU(hl!.b); w.wU(hl!.p.length); w.wA(hl!.p) } else { w.wB(0); w.wU(lits.length); w.wA(lits) }
  const f = w.finish(); const raw = 5 + 1 + 1 + 4 + 32 + 1 + 4 + data.length; let final: Uint8Array; if (f.length >= raw) { const rw = new BW(); rw.wA(MAGIC); rw.wB(VERSION); rw.wB(1); rw.wU(data.length); rw.wA(hash); rw.wB(0); rw.wU(data.length); rw.wA(data); final = rw.finish(); log(`AB: RAW ${fmt(final.length)}`) } else { final = f; log(`AB: T${useHT ? 'H' : 'R'}L${useHL ? 'H' : 'R'} ${fmt(final.length)} (${(final.length / data.length * 100).toFixed(1)}%)`) } return { file: final, hash };
}

export async function decompressNDC(f: Uint8Array): Promise<Uint8Array> {
  let p = 0; if (f.length < 5 || dec.decode(f.slice(0, 5)) !== "NDC20") throw new Error("bad magic"); p += 5;
  if (p >= f.length) throw new Error("truncated"); const ver = f[p++]; if (ver !== VERSION) throw new Error(`bad version ${ver}`);
  if (p >= f.length) throw new Error("truncated"); const ph = f[p++]; if (ph !== 1) throw new Error(`bad phase ${ph}`);
  if (p + 4 > f.length) throw new Error("truncated"); const sz = ru32(f, p); p += 4;
  if (p + 32 > f.length) throw new Error("truncated"); const hh = f.slice(p, p + 32); p += 32;
  if (p >= f.length) throw new Error("truncated"); const codec = f[p++]; let o: Uint8Array;
  if (codec === 0) {
    if (p + 4 > f.length) throw new Error("RAW truncated"); const len = ru32(f, p); p += 4;
    if (len !== sz) throw new Error(`RAW size mismatch ${len}!=${sz}`);
    if (p + len > f.length) throw new Error("RAW EOF"); o = f.slice(p, p + len)
  } else {
    if (p >= f.length) throw new Error("truncated"); const tm = f[p++]; if (p + 4 > f.length) throw new Error("truncated"); const tl = ru32(f, p); p += 4; let toks: Uint8Array;
    if (tm === 1) { if (p + 256 + 8 > f.length) throw new Error("token huff truncated"); const tb = f.slice(p, p + 256); p += 256; const bl = ru32(f, p); p += 4; const pl = ru32(f, p); p += 4; if (p + pl > f.length) throw new Error("token payload EOF"); toks = huffDec(tb, f.slice(p, p + pl), tl, bl); p += pl } else { if (p + tl > f.length) throw new Error("token EOF"); toks = f.slice(p, p + tl); p += tl }
    if (p >= f.length) throw new Error("truncated"); const lm = f[p++]; if (p + 4 > f.length) throw new Error("truncated"); const ll = ru32(f, p); p += 4; let lits: Uint8Array;
    if (lm === 1) { if (p + 256 + 8 > f.length) throw new Error("lit huff truncated"); const lb = f.slice(p, p + 256); p += 256; const bl = ru32(f, p); p += 4; const pl = ru32(f, p); p += 4; if (p + pl > f.length) throw new Error("lit payload EOF"); lits = huffDec(lb, f.slice(p, p + pl), ll, bl); p += pl } else { if (p + ll > f.length) throw new Error("lit EOF"); lits = f.slice(p, p + ll); p += ll }
    o = lzDecode(toks, lits, sz)
  }
  if (o.length !== sz) throw new Error(`size ${o.length}!=${sz}`); const oh = await sha256(o); if (!oh.every((v, i) => v === hh[i])) throw new Error("hash mismatch"); return o;
}

export const compressNDC = compressNDC_AB;
export async function deflateEnc(d: Uint8Array): Promise<Uint8Array> { const { deflate } = await import("pako"); return deflate(d, { level: 9 }) }