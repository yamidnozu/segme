/**
 * @file ndc_core.ts
 * @description Core engine for the NDC (Next Data Compression) architecture.
 * Implements a multi-stage compression pipeline including LZ-based dictionary matching,
 * canonical Huffman encoding, and various data-specific transformations.
 * 
 * Version: NDC51 RICE-MARKOV
 * Architecture: LZ hash-mult × 12 slots × 4 MiB + Huffman + stream FILL/RLE
 */

// Base estable retomada desde NDC42: TLM2 + core LZ/rANS/FILL/RLE + DFRAW + validación round-trip.
// Se retiran del camino activo las capas que en pruebas empeoraron o no aportaron: CTX256, AP, STEP extra, MINMAX extra.
// Mejora segura: torneo interno de modos LZ A/B/C para candidatos clave, sin cambiar formato.
// Núcleo: LZ hash-mult × 12 slots × 4 MiB + Huffman canónico + stream FILL/RLE
// Capas reversibles:
//   0 = normal
//   1 = LTP1 inline: plantillas de línea + variables inline
//   2 = TLM2 columnar: plantillas de línea + variables por columnas + delta/dict
//
// Idea aplicada:
// - No tokens estáticos.
// - Minería dinámica de plantillas.
// - Transformación tipo matriz por línea: filas = líneas, columnas = variables.
// - Cada columna decide RAW / NUM-DELTA / DICT.
// - Fallback automático si no conviene.
// - Compatible para cualquier archivo: si no es texto/log, gana modo normal.
//
// Formato nuevo: NDC20 version 1 phase 5.
// Incluye lector legacy phase 2 para archivos viejos NDC20.

const dec = new TextDecoder();
const enc = new TextEncoder();

export const MAGIC = enc.encode("NDC20");
export const VERSION = 1;

const PHASE_LEGACY = 2;
const PHASE_NDC37 = 5;
const PHASE_NDC38 = 6;
const PHASE_NDC39 = 7;
const PHASE_NDC40 = 8;
const PHASE_NDC42 = 9;
const PHASE_NDC50 = 10;
const PHASE_NDC51 = 11;

const CORE_CODEC = 1;

const HB = 16;
const HS = 1 << HB;
const HSL = 12;
const HM = HS - 1;
const WIN = 4 * 1024 * 1024;
const MINM = 3;
const MAXM = 258;
const MINR = 8;

const T_RAW = 0;
const T_LTP1 = 1;
const T_TLM2 = 2;
const T_BLC1 = 3;
const T_BWT1 = 4;
const T_STT1 = 5;
const T_LEX1 = 6;

const V_NUM = 1;
const V_UUID = 2;
const V_HEX = 3;

/**
 * Formats a byte size into a human-readable string.
 * @param n Number of bytes.
 * @returns Formatted string (e.g., "1.23 MB").
 */
export const fmt = (n: number | null | undefined) => {
  if (n == null) return "N/A";
  if (n < 1024) return `${n} B`;
  if (n < 1048576) return `${(n / 1024).toFixed(2)} KB`;
  return `${(n / 1048576).toFixed(2)} MB`;
};

/**
 * Calculates the SHA-256 hash of a byte array.
 * @param b Input byte array.
 * @returns Promise resolving to the 32-byte hash.
 */
export const sha256 = async (b: Uint8Array) => {
  const buf = b.buffer.slice(
    b.byteOffset,
    b.byteOffset + b.byteLength
  ) as ArrayBuffer;

  return new Uint8Array(await crypto.subtle.digest("SHA-256", buf));
};

const concat = (a: Uint8Array[]) => {
  const s = a.reduce((x, y) => x + y.length, 0);
  const o = new Uint8Array(s);
  let p = 0;

  for (const v of a) {
    o.set(v, p);
    p += v.length;
  }

  return o;
};

const readU32 = (b: Uint8Array, p: number) => {
  if (p + 4 > b.length) throw new Error("u32 out of bounds");
  return new DataView(b.buffer, b.byteOffset + p, 4).getUint32(0, false);
};

class BW {
  private chunks: Uint8Array[] = [];
  private buf = new Uint8Array(65536);
  private pos = 0;

  wB(x: number) {
    if (this.buf.length - this.pos < 1) {
      this.chunks.push(this.buf.slice(0, this.pos));
      this.pos = 0;
    }

    this.buf[this.pos++] = x & 255;
  }

  wA(d: Uint8Array) {
    if (!d.length) return;

    if (this.pos) {
      this.chunks.push(this.buf.slice(0, this.pos));
      this.pos = 0;
    }

    this.chunks.push(d);
  }

  wU(n: number) {
    if (!Number.isFinite(n) || n < 0 || n > 0xffffffff) {
      throw new Error("invalid u32");
    }

    this.wB(n >>> 24);
    this.wB(n >>> 16);
    this.wB(n >>> 8);
    this.wB(n);
  }

  wVar(n: number) {
    if (!Number.isSafeInteger(n) || n < 0) throw new Error("bad varuint");

    while (n >= 0x80) {
      this.wB((n & 0x7f) | 0x80);
      n = Math.floor(n / 128);
    }

    this.wB(n);
  }

  wSVar(n: number) {
    if (!Number.isSafeInteger(n)) throw new Error("bad svarint");
    const z = n < 0 ? ((-n) * 2 - 1) : n * 2;
    this.wVar(z);
  }

  finish() {
    if (this.pos) {
      this.chunks.push(this.buf.slice(0, this.pos));
      this.pos = 0;
    }

    return concat(this.chunks);
  }
}

class BR {
  p = 0;
  b: Uint8Array;

  constructor(b: Uint8Array) {
    this.b = b;
  }

  rB() {
    if (this.p >= this.b.length) throw new Error("read byte out of bounds");
    return this.b[this.p++];
  }

  rA(n: number) {
    if (n < 0 || this.p + n > this.b.length) throw new Error("read array out of bounds");
    const v = this.b.slice(this.p, this.p + n);
    this.p += n;
    return v;
  }

  rU() {
    const v = readU32(this.b, this.p);
    this.p += 4;
    return v;
  }

  rVar(max: number = Number.MAX_SAFE_INTEGER) {
    let shift = 0;
    let out = 0;

    for (let bytes = 0; bytes < 8; bytes++) {
      const x = this.rB();
      const part = x & 0x7f;

      out += part * Math.pow(2, shift);

      if (!Number.isSafeInteger(out) || out > max) {
        throw new Error("varuint too large");
      }

      if ((x & 0x80) === 0) return out;

      shift += 7;
    }

    throw new Error("varuint too large");
  }

  rSVar() {
    const z = this.rVar();
    return (z & 1) ? -((z + 1) / 2) : z / 2;
  }
}

class BitWriter {
  private bytes: number[] = [];
  private acc = 0;
  private bits = 0;

  writeBit(bit: number) {
    this.acc = (this.acc << 1) | (bit & 1);
    this.bits++;

    if (this.bits === 8) {
      this.bytes.push(this.acc);
      this.acc = 0;
      this.bits = 0;
    }
  }

  writeBits(value: number, count: number) {
    for (let i = count - 1; i >= 0; i--) {
      this.writeBit((value >>> i) & 1);
    }
  }

  finish() {
    if (this.bits > 0) {
      this.bytes.push((this.acc << (8 - this.bits)) & 255);
      this.acc = 0;
      this.bits = 0;
    }

    return new Uint8Array(this.bytes);
  }
}

class BitReader {
  private bitPos = 0;
  private data: Uint8Array;
  private bitLimit: number;

  constructor(data: Uint8Array, bitLimit: number) {
    this.data = data;
    this.bitLimit = bitLimit;
  }

  readBits(count: number) {
    let v = 0;

    for (let i = 0; i < count; i++) {
      if (this.bitPos >= this.bitLimit) throw new Error("bitstream underflow");

      const byte = this.data[this.bitPos >>> 3];
      const bit = 7 - (this.bitPos & 7);

      v = (v << 1) | ((byte >>> bit) & 1);
      this.bitPos++;
    }

    return v >>> 0;
  }
}

const h4 = (d: Uint8Array, p: number) => {
  if (p + 3 >= d.length) return 0;

  return (
    Math.imul(d[p], 2654435761) ^
    Math.imul(d[p + 1], 2246822519) ^
    Math.imul(d[p + 2], 3266489917) ^
    Math.imul(d[p + 3], 668265263)
  ) >>> 0 & HM;
};

const cnt = (d: Uint8Array, a: number, b: number, m: number) => {
  let l = 0;
  const lim = Math.min(m, d.length - b, d.length - a);

  while (l < lim && d[a + l] === d[b + l]) l++;

  return l;
};

const ctx4 = (p: number) => p < 64 ? 0 : p < 128 ? 1 : p < 192 ? 2 : 3;
const ctx16 = (p: number) => p >>> 4;

type HuffEncResult = {
  t: Uint8Array;
  p: Uint8Array;
  b: number;
};

function huffEnc(d: Uint8Array): HuffEncResult | null {
  if (d.length < 32) return null;

  const freq = new Uint32Array(256);

  for (const x of d) freq[x]++;

  const heap: { s: number; f: number; l?: any; r?: any }[] = [];

  for (let i = 0; i < 256; i++) {
    if (freq[i]) heap.push({ s: i, f: freq[i] });
  }

  if (heap.length < 2) return null;

  heap.sort((a, b) => a.f - b.f || a.s - b.s);

  while (heap.length > 1) {
    const a = heap.shift()!;
    const b = heap.shift()!;
    const n = { s: -1, f: a.f + b.f, l: a, r: b };

    let i = 0;
    while (i < heap.length && heap[i].f <= n.f) i++;
    heap.splice(i, 0, n);
  }

  const lengths = new Uint8Array(256);

  const walk = (n: any, depth: number) => {
    if (n.s >= 0) {
      lengths[n.s] = depth;
      return;
    }

    walk(n.l, depth + 1);
    walk(n.r, depth + 1);
  };

  walk(heap[0], 1);

  const maxLen = lengths.reduce((m, v) => Math.max(m, v), 0);
  if (maxLen > 24) return null;

  const syms = Array.from({ length: 256 }, (_, i) => i)
    .filter(i => lengths[i])
    .sort((a, b) => lengths[a] - lengths[b] || a - b);

  const codes = new Uint32Array(256);
  let code = 0;
  let prevLen = 0;

  for (const s of syms) {
    const len = lengths[s];

    if (len > prevLen) {
      code <<= len - prevLen;
      prevLen = len;
    }

    codes[s] = code++;
  }

  const bw = new BW();
  let acc = 0;
  let bits = 0;
  let bitLen = 0;

  const writeBits = (value: number, len: number) => {
    acc = ((acc << len) | value) >>> 0;
    bits += len;
    bitLen += len;

    while (bits >= 8) {
      bits -= 8;
      bw.wB((acc >>> bits) & 255);
      acc &= bits === 0 ? 0 : ((1 << bits) - 1);
    }
  };

  for (const x of d) {
    writeBits(codes[x], lengths[x]);
  }

  const realBits = bitLen;

  if (bits) {
    writeBits(0, 8 - bits);
  }

  return {
    t: lengths,
    p: bw.finish(),
    b: realBits,
  };
}

function huffDec(table: Uint8Array, payload: Uint8Array, outLen: number, bitLen: number) {
  const maxLen = Math.max(...table);

  if (!maxLen) return new Uint8Array(outLen);

  if (maxLen > 24) throw new Error("invalid huffman max length");

  const syms = Array.from({ length: 256 }, (_, i) => i)
    .filter(i => table[i])
    .sort((a, b) => table[a] - table[b] || a - b);

  const codes = new Uint32Array(256);
  let code = 0;
  let prev = 0;

  for (const s of syms) {
    const len = table[s];

    if (len > prev) {
      code <<= len - prev;
      prev = len;
    }

    codes[s] = code++;
  }

  const tree: { l?: number; r?: number; sym?: number }[] = [{}];

  for (const s of syms) {
    const len = table[s];
    const c = codes[s];
    let node = 0;

    for (let i = len - 1; i >= 0; i--) {
      const bit = (c >>> i) & 1;

      if (bit === 0) {
        if (tree[node].l === undefined) {
          tree[node].l = tree.length;
          tree.push({});
        }

        node = tree[node].l!;
      } else {
        if (tree[node].r === undefined) {
          tree[node].r = tree.length;
          tree.push({});
        }

        node = tree[node].r!;
      }
    }

    tree[node].sym = s;
  }

  const out = new Uint8Array(outLen);
  let op = 0;
  let node = 0;

  for (let i = 0; i < bitLen && op < outLen; i++) {
    const bit = (payload[i >>> 3] >>> (7 - (i & 7))) & 1;
    const next = bit ? tree[node].r : tree[node].l;

    if (next === undefined) throw new Error("invalid huffman payload");

    node = next;

    if (tree[node].sym !== undefined) {
      out[op++] = tree[node].sym!;
      node = 0;
    }
  }

  if (op !== outLen) throw new Error("huffman output length mismatch");

  return out;
}

const uniqueByte = (d: Uint8Array) => {
  if (!d.length) return -1;

  const v = d[0];

  for (let i = 1; i < d.length; i++) {
    if (d[i] !== v) return -1;
  }

  return v;
};

function rleByteEnc(d: Uint8Array) {
  if (!d.length) return new Uint8Array();

  const w = new BW();
  let i = 0;

  while (i < d.length) {
    const v = d[i];
    let r = 1;

    while (i + r < d.length && d[i + r] === v) r++;

    w.wB(v);
    w.wVar(r);
    i += r;
  }

  return w.finish();
}

function rleByteDec(payload: Uint8Array, outLen: number) {
  const r = new BR(payload);
  const out = new Uint8Array(outLen);
  let p = 0;

  while (p < outLen) {
    const v = r.rB();
    const len = r.rVar();

    if (p + len > outLen) throw new Error("rle stream overflow");

    out.fill(v, p, p + len);
    p += len;
  }

  if (r.p !== payload.length) throw new Error("rle trailing bytes");

  return out;
}

const RANS_SCALE_BITS = 12;
const RANS_TOTAL = 1 << RANS_SCALE_BITS;
const RANS_L = 1 << 23;

function buildRansFreq(d: Uint8Array) {
  if (d.length < 64) return null;

  const counts = new Uint32Array(256);
  let used = 0;

  for (const x of d) {
    if (counts[x]++ === 0) used++;
  }

  if (used < 2) return null;

  const freq = new Uint16Array(256);
  let sum = 0;

  for (let i = 0; i < 256; i++) {
    if (counts[i]) {
      const f = Math.max(1, Math.floor((counts[i] * RANS_TOTAL) / d.length));
      freq[i] = f;
      sum += f;
    }
  }

  while (sum < RANS_TOTAL) {
    let best = -1;
    let bestScore = -Infinity;

    for (let i = 0; i < 256; i++) {
      if (!counts[i]) continue;

      const score = counts[i] / d.length - freq[i] / RANS_TOTAL;

      if (score > bestScore) {
        bestScore = score;
        best = i;
      }
    }

    freq[best]++;
    sum++;
  }

  while (sum > RANS_TOTAL) {
    let best = -1;
    let bestFreq = -1;

    for (let i = 0; i < 256; i++) {
      if (freq[i] > 1 && freq[i] > bestFreq) {
        bestFreq = freq[i];
        best = i;
      }
    }

    if (best < 0) return null;

    freq[best]--;
    sum--;
  }

  const cum = new Uint16Array(256);
  let c = 0;

  for (let i = 0; i < 256; i++) {
    cum[i] = c;
    c += freq[i];
  }

  if (c !== RANS_TOTAL) return null;

  return {
    freq,
    cum,
  };
}

function ransEnc(d: Uint8Array) {
  const model = buildRansFreq(d);

  if (!model) return null;

  const { freq, cum } = model;
  const out: number[] = [];
  let x = RANS_L;
  const base = Math.floor(RANS_L / RANS_TOTAL) * 256;

  for (let i = d.length - 1; i >= 0; i--) {
    const s = d[i];
    const f = freq[s];
    const c = cum[s];
    const xMax = base * f;

    while (x >= xMax) {
      out.push(x & 255);
      x = Math.floor(x / 256);
    }

    x = Math.floor(x / f) * RANS_TOTAL + (x % f) + c;
  }

  const payload = new Uint8Array(4 + out.length);
  payload[0] = (x >>> 24) & 255;
  payload[1] = (x >>> 16) & 255;
  payload[2] = (x >>> 8) & 255;
  payload[3] = x & 255;

  for (let i = 0; i < out.length; i++) {
    payload[4 + i] = out[i];
  }

  return {
    freq,
    payload,
  };
}

function ransDec(freqBytes: Uint8Array, payload: Uint8Array, outLen: number) {
  if (payload.length < 4) throw new Error("rans payload too small");

  const freq = new Uint16Array(256);
  const cum = new Uint16Array(256);
  const table = new Uint8Array(RANS_TOTAL);
  let c = 0;

  for (let i = 0; i < 256; i++) {
    const f = (freqBytes[i * 2] << 8) | freqBytes[i * 2 + 1];
    freq[i] = f;
    cum[i] = c;

    for (let j = 0; j < f; j++) {
      table[c + j] = i;
    }

    c += f;
  }

  if (c !== RANS_TOTAL) throw new Error("bad rans frequency total");

  let x =
    payload[0] * 0x1000000 +
    (payload[1] << 16) +
    (payload[2] << 8) +
    payload[3];

  let ptr = payload.length;
  const out = new Uint8Array(outLen);

  for (let i = 0; i < outLen; i++) {
    const m = x & (RANS_TOTAL - 1);
    const s = table[m];

    out[i] = s;

    x = freq[s] * Math.floor(x / RANS_TOTAL) + (m - cum[s]);

    while (x < RANS_L && ptr > 4) {
      x = x * 256 + payload[--ptr];
    }
  }

  if (ptr !== 4) throw new Error("rans trailing bytes");

  return out;
}

function ransFreqToBytes(freq: Uint16Array) {
  const out = new Uint8Array(512);

  for (let i = 0; i < 256; i++) {
    out[i * 2] = freq[i] >>> 8;
    out[i * 2 + 1] = freq[i] & 255;
  }

  return out;
}

function streamCost(d: Uint8Array) {
  const raw = 1 + 4 + d.length;

  let best = raw;

  const fill = uniqueByte(d);
  if (fill >= 0 && d.length > 4) {
    best = Math.min(best, 1 + 4 + 1);
  }

  const h = huffEnc(d);
  if (h) {
    best = Math.min(best, 1 + 4 + 256 + 4 + 4 + h.p.length);
  }

  if (d.length >= 16) {
    const rle = rleByteEnc(d);
    best = Math.min(best, 1 + 4 + 4 + rle.length);
  }

  const ra = ransEnc(d);
  if (ra) {
    best = Math.min(best, 1 + 4 + 512 + 4 + ra.payload.length);
  }

  return best;
}

function encodeStream(w: BW, d: Uint8Array) {
  let bestMode = 0;
  let bestPayload = d;
  let bestCost = 1 + 4 + d.length;

  const fill = uniqueByte(d);

  if (fill >= 0 && d.length > 4) {
    const cost = 1 + 4 + 1;

    if (cost < bestCost) {
      bestMode = 2;
      bestPayload = new Uint8Array([fill]);
      bestCost = cost;
    }
  }

  const h = huffEnc(d);

  if (h) {
    const cost = 1 + 4 + 256 + 4 + 4 + h.p.length;

    if (cost < bestCost) {
      bestMode = 1;
      bestPayload = h.p;
      bestCost = cost;
    }
  }

  let rlePayload: Uint8Array | null = null;

  if (d.length >= 16) {
    rlePayload = rleByteEnc(d);
    const cost = 1 + 4 + 4 + rlePayload.length;

    if (cost < bestCost) {
      bestMode = 3;
      bestPayload = rlePayload;
      bestCost = cost;
    }
  }

  const ra = ransEnc(d);

  if (ra) {
    const cost = 1 + 4 + 512 + 4 + ra.payload.length;

    if (cost < bestCost) {
      bestMode = 4;
      bestPayload = ra.payload;
      bestCost = cost;
    }
  }

  if (bestMode === 0) {
    w.wB(0);
    w.wU(d.length);
    w.wA(d);
  } else if (bestMode === 1) {
    w.wB(1);
    w.wU(d.length);
    w.wA(h!.t);
    w.wU(h!.b);
    w.wU(h!.p.length);
    w.wA(h!.p);
  } else if (bestMode === 2) {
    w.wB(2);
    w.wU(d.length);
    w.wB(bestPayload[0]);
  } else if (bestMode === 3) {
    w.wB(3);
    w.wU(d.length);
    w.wU(bestPayload.length);
    w.wA(bestPayload);
  } else {
    w.wB(4);
    w.wU(d.length);
    w.wA(ransFreqToBytes(ra!.freq));
    w.wU(bestPayload.length);
    w.wA(bestPayload);
  }
}

function readStream(r: BR) {
  const mode = r.rB();
  const len = r.rU();

  if (mode === 0) {
    return r.rA(len);
  }

  if (mode === 1) {
    const table = r.rA(256);
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);

    return huffDec(table, payload, len, bitLen);
  }

  if (mode === 2) {
    const v = r.rB();
    const out = new Uint8Array(len);
    out.fill(v);
    return out;
  }

  if (mode === 3) {
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    return rleByteDec(payload, len);
  }

  if (mode === 4) {
    const freq = r.rA(512);
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    return ransDec(freq, payload, len);
  }

  throw new Error("unknown stream mode");
}

function readStreamLegacy(r: BR) {
  const mode = r.rB();
  const len = r.rU();

  if (mode === 0) {
    return r.rA(len);
  }

  if (mode === 1) {
    const table = r.rA(256);
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);

    return huffDec(table, payload, len, bitLen);
  }

  throw new Error("unknown legacy stream mode");
}

type LzMode = "A" | "B" | "C";

type LzStreams = {
  types: Uint8Array;
  litLens: Uint8Array;
  matchLens: Uint8Array;
  distBuckets: Uint8Array;
  distExtras: number[];
  distExtraBits: number[];
  runVals: Uint8Array;
  runLens: number[];
  lits: Uint8Array;
  lits4: Uint8Array[];
  lits16: Uint8Array[];
};

function lzEncodeCore(data: Uint8Array, mode: LzMode = "B"): LzStreams {
  const table = new Int32Array(HS * HSL);
  table.fill(-1);

  const types: number[] = [];
  const litLens: number[] = [];
  const matchLens: number[] = [];
  const distBuckets: number[] = [];
  const distExtras: number[] = [];
  const distExtraBits: number[] = [];
  const runVals: number[] = [];
  const runLens: number[] = [];
  const lits: number[] = [];
  const lits4: number[][] = [[], [], [], []];
  const lits16: number[][] = Array.from({ length: 16 }, () => []);

  const update = (p: number) => {
    if (p + 3 >= data.length) return;

    const h = h4(data, p);
    const base = h * HSL;

    for (let k = HSL - 1; k > 0; k--) {
      table[base + k] = table[base + k - 1];
    }

    table[base] = p;
  };

  const find = (p: number) => {
    if (p + MINM > data.length) return null;

    const h = h4(data, p);
    const base = h * HSL;

    let bestLen = 0;
    let bestDist = 0;

    for (let k = 0; k < HSL; k++) {
      const old = table[base + k];

      if (old < 0) continue;

      const dist = p - old;

      if (dist <= 0 || dist > WIN) continue;

      const len = cnt(data, old, p, MAXM);

      if (len > bestLen || (len === bestLen && dist < bestDist)) {
        bestLen = len;
        bestDist = dist;
      }
    }

    return bestLen >= MINM ? { l: bestLen, d: bestDist } : null;
  };

  const distBucket = (d: number) => 31 - Math.clz32(d | 1);

  const worth = (m: { l: number; d: number } | null) => {
    if (!m) return null;

    if (mode === "C") return m;

    const b = distBucket(m.d);

    const need =
      mode === "A"
        ? 4 + (b >> 2)
        : b < 8
          ? 3
          : 4 + Math.max(0, (b - 6) >> 2);

    return m.l >= need ? m : null;
  };

  let litBuf: number[] = [];

  const flush = (pos: number) => {
    if (!litBuf.length) return;

    types.push(0);
    litLens.push(litBuf.length);

    for (let k = 0; k < litBuf.length; k++) {
      const b = litBuf[k];
      const absolutePos = pos - litBuf.length + k;
      const prev = absolutePos > 0 ? data[absolutePos - 1] : 0;

      lits.push(b);
      lits4[ctx4(prev)].push(b);
      lits16[ctx16(prev)].push(b);
    }

    litBuf = [];
  };

  for (let i = 0; i < data.length;) {
    let run = 1;
    const value = data[i];

    while (i + run < data.length && data[i + run] === value && run < 65535) {
      run++;
    }

    if (run >= MINR) {
      flush(i);

      types.push(2);
      runVals.push(value);
      runLens.push(run);

      update(i);
      if (run > 1) update(i + run - 1);

      i += run;
      continue;
    }

    let m1 = worth(find(i));

    update(i);

    let m2 = i + 1 < data.length ? worth(find(i + 1)) : null;

    if (m2 && m1 && m2.l > m1.l + 3) {
      litBuf.push(data[i]);
      i++;

      if (litBuf.length === 255) flush(i);

      m1 = m2;
      update(i);
    }

    if (m1 && (!m2 || m1.l >= m2.l)) {
      flush(i);

      types.push(1);
      matchLens.push(m1.l - MINM);

      const b = distBucket(m1.d);
      distBuckets.push(b);
      distExtras.push(m1.d - (1 << b));
      distExtraBits.push(b);

      update(i + 1);
      if (m1.l > 8) update(i + (m1.l >> 1));
      if (m1.l > 16) update(i + m1.l - 4);
      update(i + m1.l - 1);

      i += m1.l;
    } else {
      litBuf.push(data[i]);
      i++;

      if (litBuf.length === 255) flush(i);
    }
  }

  flush(data.length);

  return {
    types: new Uint8Array(types),
    litLens: new Uint8Array(litLens),
    matchLens: new Uint8Array(matchLens),
    distBuckets: new Uint8Array(distBuckets),
    distExtras,
    distExtraBits,
    runVals: new Uint8Array(runVals),
    runLens,
    lits: new Uint8Array(lits),
    lits4: lits4.map(a => new Uint8Array(a)),
    lits16: lits16.map(a => new Uint8Array(a)),
  };
}

const estimateStreams = (tmp: LzStreams) => {
  let extraBits = 0;

  for (let i = 0; i < tmp.distExtraBits.length; i++) {
    extraBits += tmp.distExtraBits[i];
  }

  const extrasCost = 1 + 4 + 4 + 4 + Math.ceil(extraBits / 8);

  const rl = new Uint8Array(tmp.runLens.length * 2);
  const dv = new DataView(rl.buffer);

  tmp.runLens.forEach((x, i) => dv.setUint16(i * 2, x, false));

  const base =
    streamCost(tmp.types) +
    streamCost(tmp.litLens) +
    streamCost(tmp.matchLens) +
    streamCost(tmp.distBuckets) +
    extrasCost +
    streamCost(tmp.runVals) +
    streamCost(rl);

  const normal = streamCost(tmp.lits);
  const c4 = 1 + tmp.lits4.reduce((s, a) => s + streamCost(a), 0);
  const c16 = 1 + tmp.lits16.reduce((s, a) => s + streamCost(a), 0);

  return base + Math.min(normal, c4, c16);
};

function chooseMode(data: Uint8Array): LzMode {
  let mode: LzMode = "B";

  if (data.length <= 768 * 1024) return mode;

  const sampleSize = 256 * 1024;
  const sample = new Uint8Array(Math.min(data.length, sampleSize * 3));

  if (data.length <= sampleSize * 3) {
    sample.set(data.subarray(0, sample.length));
  } else {
    sample.set(data.subarray(0, sampleSize), 0);

    const mid = Math.floor(data.length / 2 - sampleSize / 2);

    sample.set(data.subarray(mid, mid + sampleSize), sampleSize);
    sample.set(data.subarray(data.length - sampleSize), sampleSize * 2);
  }

  let best = Infinity;

  for (const m of ["A", "B", "C"] as const) {
    const tmp = lzEncodeCore(sample, m);
    const score = estimateStreams(tmp);

    if (score < best) {
      best = score;
      mode = m;
    }
  }

  return mode;
}

function coreEncode(data: Uint8Array, forcedMode?: LzMode): Uint8Array {
  const mode = forcedMode ?? chooseMode(data);
  const s = lzEncodeCore(data, mode);
  const w = new BW();

  w.wB(CORE_CODEC);
  w.wU(data.length);

  encodeStream(w, s.types);
  encodeStream(w, s.litLens);
  encodeStream(w, s.matchLens);
  encodeStream(w, s.distBuckets);

  const ebw = new BitWriter();
  let totalBits = 0;

  for (let i = 0; i < s.distExtras.length; i++) {
    const b = s.distExtraBits[i];
    const v = s.distExtras[i];

    ebw.writeBits(v, b);
    totalBits += b;
  }

  const extras = ebw.finish();

  w.wB(0);
  w.wU(s.distExtras.length);
  w.wU(totalBits);
  w.wU(extras.length);
  w.wA(extras);

  encodeStream(w, s.runVals);

  const rl = new Uint8Array(s.runLens.length * 2);
  const rldv = new DataView(rl.buffer);

  s.runLens.forEach((x, i) => rldv.setUint16(i * 2, x, false));

  encodeStream(w, rl);

  const normalCost = streamCost(s.lits);
  const c4Cost = 1 + s.lits4.reduce((sum, a) => sum + streamCost(a), 0);
  const c16Cost = 1 + s.lits16.reduce((sum, a) => sum + streamCost(a), 0);

  let litMode = 0;

  if (c4Cost < normalCost && c4Cost <= c16Cost) {
    litMode = 1;
  } else if (c16Cost < normalCost && c16Cost < c4Cost) {
    litMode = 2;
  }

  w.wB(litMode);

  if (litMode === 0) {
    encodeStream(w, s.lits);
  } else if (litMode === 1) {
    for (let i = 0; i < 4; i++) encodeStream(w, s.lits4[i]);
  } else {
    for (let i = 0; i < 16; i++) encodeStream(w, s.lits16[i]);
  }

  return w.finish();
}

type CoreEncodeBestResult = {
  inner: Uint8Array;
  mode: LzMode;
  tried: string;
};

function coreEncodeBest(data: Uint8Array, candidateName = ""): CoreEncodeBestResult {
  const baseMode = chooseMode(data);
  const modes: LzMode[] = [baseMode];

  // Optimización segura: no es una capa nueva; solo prueba los 3 perfiles que ya existen.
  // Se limita para no volver pesado el dashboard. TLM2 y normal son los que sí han dado resultados.
  const tournament =
    data.length <= 2 * 1024 * 1024 ||
    candidateName === "TLM2" ||
    candidateName === "normal";

  if (tournament) {
    for (const m of ["A", "B", "C"] as const) {
      if (!modes.includes(m)) modes.push(m);
    }
  }

  let bestInner: Uint8Array | null = null;
  let bestMode: LzMode = baseMode;
  const tried: string[] = [];

  for (const m of modes) {
    const inner = coreEncode(data, m);
    tried.push(`${m}:${inner.length}`);

    if (!bestInner || inner.length < bestInner.length) {
      bestInner = inner;
      bestMode = m;
    }
  }

  return {
    inner: bestInner!,
    mode: bestMode,
    tried: tried.join(","),
  };
}

function coreDecode(inner: Uint8Array): Uint8Array {
  const r = new BR(inner);
  const codec = r.rB();

  if (codec !== CORE_CODEC) throw new Error("bad core codec");

  const originalSize = r.rU();

  const types = readStream(r);
  const litLens = readStream(r);
  const matchLens = readStream(r);
  const distBuckets = readStream(r);

  const extrasMode = r.rB();

  if (extrasMode !== 0) throw new Error("unsupported extras mode");

  const distCount = r.rU();
  const totalBits = r.rU();
  const extrasLen = r.rU();
  const extras = r.rA(extrasLen);

  const runVals = readStream(r);
  const runLensBytes = readStream(r);

  if (runLensBytes.length % 2 !== 0) throw new Error("bad run lens stream");

  const litMode = r.rB();

  let literalsByContext: Uint8Array[];

  if (litMode === 0) {
    literalsByContext = [readStream(r)];
  } else if (litMode === 1) {
    literalsByContext = [readStream(r), readStream(r), readStream(r), readStream(r)];
  } else if (litMode === 2) {
    literalsByContext = Array.from({ length: 16 }, () => readStream(r));
  } else {
    throw new Error("bad literal mode");
  }

  if (r.p !== inner.length) throw new Error("core trailing bytes");

  const runLens = new Array(runLensBytes.length / 2);
  const dv = new DataView(runLensBytes.buffer, runLensBytes.byteOffset, runLensBytes.byteLength);

  for (let i = 0; i < runLens.length; i++) {
    runLens[i] = dv.getUint16(i * 2, false);
  }

  const bitReader = new BitReader(extras, totalBits);
  const dists: number[] = [];

  for (let i = 0; i < distCount; i++) {
    const b = distBuckets[i];
    const base = 1 << b;
    const v = bitReader.readBits(b);

    dists.push(base + v);
  }

  const out = new Uint8Array(originalSize);

  let op = 0;
  let mi = 0;
  let di = 0;
  let ri = 0;
  let litLenIdx = 0;
  const litPtrs = literalsByContext.map(() => 0);

  for (let i = 0; i < types.length; i++) {
    const t = types[i];

    if (t === 0) {
      const len = litLens[litLenIdx++];

      for (let k = 0; k < len; k++) {
        let ctx = 0;

        if (litMode === 1) {
          ctx = op > 0 ? ctx4(out[op - 1]) : 0;
        } else if (litMode === 2) {
          ctx = op > 0 ? ctx16(out[op - 1]) : 0;
        }

        if (litPtrs[ctx] >= literalsByContext[ctx].length) {
          throw new Error("literal stream underflow");
        }

        out[op++] = literalsByContext[ctx][litPtrs[ctx]++];
      }
    } else if (t === 1) {
      if (mi >= matchLens.length || di >= dists.length) {
        throw new Error("match stream underflow");
      }

      const len = matchLens[mi++] + MINM;
      const dist = dists[di++];
      const src = op - dist;

      if (src < 0 || op + len > originalSize) {
        throw new Error("invalid match");
      }

      for (let k = 0; k < len; k++) {
        out[op++] = out[src + k];
      }
    } else if (t === 2) {
      if (ri >= runLens.length || ri >= runVals.length) {
        throw new Error("run stream underflow");
      }

      const value = runVals[ri];
      const len = runLens[ri++];

      if (op + len > originalSize) {
        throw new Error("invalid run");
      }

      out.fill(value, op, op + len);
      op += len;
    } else {
      throw new Error("bad token type");
    }
  }

  if (op !== originalSize) throw new Error("decoded size mismatch");

  for (let i = 0; i < literalsByContext.length; i++) {
    if (litPtrs[i] !== literalsByContext[i].length) {
      throw new Error("unused literal bytes");
    }
  }

  return out;
}

type TemplateShape = {
  key: string;
  parts: Uint8Array[];
  types: number[];
  vars: Uint8Array[];
  fixedLen: number;
};

type SelectedTemplate = {
  id: number;
  key: string;
  parts: Uint8Array[];
  types: number[];
  count: number;
  fixedLen: number;
};

const isDigit = (x: number) => x >= 48 && x <= 57;
const isLowerHex = (x: number) => x >= 97 && x <= 102;
const isUpperHex = (x: number) => x >= 65 && x <= 70;
const isHex = (x: number) => isDigit(x) || isLowerHex(x) || isUpperHex(x);
const isAlphaNum = (x: number) => isDigit(x) || (x >= 65 && x <= 90) || (x >= 97 && x <= 122);

function isUuidAt(d: Uint8Array, p: number) {
  if (p + 36 > d.length) return false;

  const hy = [8, 13, 18, 23];

  for (let i = 0; i < 36; i++) {
    const x = d[p + i];

    if (hy.includes(i)) {
      if (x !== 45) return false;
    } else if (!isHex(x)) {
      return false;
    }
  }

  return true;
}

function hexRun(d: Uint8Array, p: number) {
  let i = p;
  let hasAlpha = false;

  while (i < d.length && isHex(d[i])) {
    if (isLowerHex(d[i]) || isUpperHex(d[i])) hasAlpha = true;
    i++;
  }

  const len = i - p;

  if (!hasAlpha) return 0;

  if (len === 16 || len === 24 || len === 32 || len === 40 || len === 64) {
    return len;
  }

  return 0;
}

function decimalRun(d: Uint8Array, p: number) {
  let i = p;

  while (i < d.length && isDigit(d[i])) i++;

  return i - p;
}

function shapeLine(line: Uint8Array): TemplateShape | null {
  const parts: Uint8Array[] = [];
  const types: number[] = [];
  const vars: Uint8Array[] = [];

  let last = 0;
  let i = 0;

  while (i < line.length) {
    let varType = 0;
    let varLen = 0;

    if (isUuidAt(line, i)) {
      varType = V_UUID;
      varLen = 36;
    } else {
      const hr = hexRun(line, i);

      if (hr >= 16) {
        varType = V_HEX;
        varLen = hr;
      } else {
        const dr = decimalRun(line, i);

        if (dr >= 2) {
          const prev = i > 0 ? line[i - 1] : 0;
          const next = i + dr < line.length ? line[i + dr] : 0;

          // Evita convertir partes como v1 o abc12 si están pegadas a letras.
          if (!isAlphaNum(prev) || !isAlphaNum(next)) {
            varType = V_NUM;
            varLen = dr;
          }
        }
      }
    }

    if (varType) {
      parts.push(line.slice(last, i));
      types.push(varType);
      vars.push(line.slice(i, i + varLen));
      i += varLen;
      last = i;
    } else {
      i++;
    }
  }

  if (types.length < 1) return null;

  parts.push(line.slice(last));

  const fixedLen = parts.reduce((s, p) => s + p.length, 0);

  if (fixedLen < 12) return null;

  const keyParts: string[] = [];

  for (let j = 0; j < types.length; j++) {
    keyParts.push(dec.decode(parts[j]));
    keyParts.push(String.fromCharCode(1 + types[j]));
  }

  keyParts.push(dec.decode(parts[parts.length - 1]));

  return {
    key: keyParts.join(""),
    parts,
    types,
    vars,
    fixedLen,
  };
}

function splitLines(data: Uint8Array) {
  const lines: Uint8Array[] = [];
  let start = 0;

  for (let i = 0; i < data.length; i++) {
    if (data[i] === 10) {
      lines.push(data.slice(start, i + 1));
      start = i + 1;
    }
  }

  if (start < data.length) {
    lines.push(data.slice(start));
  }

  return lines;
}

function looksTextual(data: Uint8Array) {
  if (!data.length) return false;

  const n = Math.min(data.length, 65536);
  let printable = 0;
  let nl = 0;

  for (let i = 0; i < n; i++) {
    const x = data[i];

    if (x === 10) nl++;
    if (x === 9 || x === 10 || x === 13 || (x >= 32 && x <= 126) || x >= 128) {
      printable++;
    }
  }

  return printable / n > 0.92 && nl >= 2;
}

type MineTemplateOptions = {
  maxTemplates?: number;
  minCount?: number;
  minSaving?: number;
  maxSlots?: number;
  name?: string;
};

function mineTemplates(lines: Uint8Array[], opts: MineTemplateOptions = {}) {
  const maxTemplates = opts.maxTemplates ?? 1024;
  const minCount = opts.minCount ?? 2;
  const minSaving = opts.minSaving ?? 24;
  const maxSlots = opts.maxSlots ?? 96;

  const map = new Map<string, { count: number; shape: TemplateShape }>();
  const shapes: (TemplateShape | null)[] = new Array(lines.length);

  for (let i = 0; i < lines.length; i++) {
    const sh = shapeLine(lines[i]);
    shapes[i] = sh;

    if (!sh) continue;

    const old = map.get(sh.key);

    if (old) {
      old.count++;
    } else {
      map.set(sh.key, { count: 1, shape: sh });
    }
  }

  const candidates: SelectedTemplate[] = [];

  for (const [key, v] of map) {
    const count = v.count;
    const sh = v.shape;
    const slotCount = sh.types.length;

    // Estimación conservadora.
    const estimatedSaving = (count - 1) * sh.fixedLen - slotCount * 4 - sh.fixedLen - 16;

    if (count >= minCount && estimatedSaving > minSaving && slotCount <= maxSlots) {
      candidates.push({
        id: -1,
        key,
        parts: sh.parts,
        types: sh.types,
        count,
        fixedLen: sh.fixedLen,
      });
    }
  }

  candidates.sort((a, b) => {
    const sa = (a.count - 1) * a.fixedLen;
    const sb = (b.count - 1) * b.fixedLen;
    return sb - sa;
  });

  const selected = candidates.slice(0, maxTemplates);

  selected.forEach((t, i) => {
    t.id = i;
  });

  const byKey = new Map<string, SelectedTemplate>();

  for (const t of selected) {
    byKey.set(t.key, t);
  }

  return {
    shapes,
    selected,
    byKey,
  };
}

function writeTemplates(w: BW, templates: SelectedTemplate[]) {
  w.wVar(templates.length);

  for (const t of templates) {
    const slots = t.types.length;

    w.wVar(slots);

    for (let i = 0; i < slots; i++) {
      w.wVar(t.parts[i].length);
      w.wA(t.parts[i]);
      w.wB(t.types[i]);
    }

    const last = t.parts[slots];
    w.wVar(last.length);
    w.wA(last);
  }
}

function readTemplates(r: BR) {
  const count = r.rVar();
  const templates: { parts: Uint8Array[]; types: number[] }[] = [];

  for (let i = 0; i < count; i++) {
    const slots = r.rVar();
    const parts: Uint8Array[] = [];
    const types: number[] = [];

    for (let j = 0; j < slots; j++) {
      const len = r.rVar();
      parts.push(r.rA(len));
      types.push(r.rB());
    }

    const lastLen = r.rVar();
    parts.push(r.rA(lastLen));

    templates.push({ parts, types });
  }

  return templates;
}

function parseDecValue(v: Uint8Array) {
  if (!v.length || v.length > 15) return null;

  let n = 0;

  for (const b of v) {
    if (!isDigit(b)) return null;
    n = n * 10 + (b - 48);

    if (n > Math.floor(Number.MAX_SAFE_INTEGER / 4)) return null;
  }

  return n;
}

function writeInlineValue(w: BW, type: number, v: Uint8Array) {
  if (type === V_NUM) {
    const n = parseDecValue(v);

    if (n != null) {
      w.wB(1);
      w.wVar(v.length);
      w.wVar(n);
      return;
    }
  }

  w.wB(0);
  w.wVar(v.length);
  w.wA(v);
}

function readInlineValue(r: BR, type: number) {
  const mode = r.rB();

  if (mode === 1 && type === V_NUM) {
    const len = r.rVar();
    const n = String(r.rVar()).padStart(len, "0");
    return enc.encode(n);
  }

  if (mode !== 0) throw new Error("bad inline value mode");

  const len = r.rVar();
  return r.rA(len);
}

function transformLTP1(data: Uint8Array) {
  if (!looksTextual(data)) return null;

  const lines = splitLines(data);

  if (lines.length < 4) return null;

  const mined = mineTemplates(lines);

  if (!mined.selected.length) return null;

  const w = new BW();

  w.wA(enc.encode("LTP1"));
  w.wB(1);
  w.wU(data.length);

  writeTemplates(w, mined.selected);

  w.wVar(lines.length);

  let hits = 0;
  let rawLines = 0;

  for (let i = 0; i < lines.length; i++) {
    const sh = mined.shapes[i];
    const tpl = sh ? mined.byKey.get(sh.key) : undefined;

    if (sh && tpl) {
      w.wB(1);
      w.wVar(tpl.id);

      for (let j = 0; j < sh.vars.length; j++) {
        writeInlineValue(w, sh.types[j], sh.vars[j]);
      }

      hits++;
    } else {
      w.wB(0);
      w.wVar(lines[i].length);
      w.wA(lines[i]);
      rawLines++;
    }
  }

  const out = w.finish();

  return {
    data: out,
    info: `tpl=${mined.selected.length} hits=${hits} rawLines=${rawLines}`,
  };
}

function inverseLTP1(data: Uint8Array) {
  const r = new BR(data);

  if (dec.decode(r.rA(4)) !== "LTP1") throw new Error("bad LTP1 magic");

  const ver = r.rB();

  if (ver !== 1) throw new Error("bad LTP1 version");

  const originalLen = r.rU();
  const templates = readTemplates(r);
  const lineCount = r.rVar();

  const parts: Uint8Array[] = [];

  for (let i = 0; i < lineCount; i++) {
    const tag = r.rB();

    if (tag === 0) {
      const len = r.rVar();
      parts.push(r.rA(len));
    } else if (tag === 1) {
      const tid = r.rVar();
      const tpl = templates[tid];

      if (!tpl) throw new Error("bad LTP1 template id");

      const out: Uint8Array[] = [];

      for (let j = 0; j < tpl.types.length; j++) {
        out.push(tpl.parts[j]);
        out.push(readInlineValue(r, tpl.types[j]));
      }

      out.push(tpl.parts[tpl.parts.length - 1]);
      parts.push(concat(out));
    } else {
      throw new Error("bad LTP1 line tag");
    }
  }

  if (r.p !== data.length) throw new Error("LTP1 trailing bytes");

  const out = concat(parts);

  if (out.length !== originalLen) throw new Error("LTP1 size mismatch");

  return out;
}

function encodeColumnRaw(values: Uint8Array[]) {
  const w = new BW();

  w.wB(0);

  for (const v of values) {
    w.wVar(v.length);
    w.wA(v);
  }

  return w.finish();
}

function sameBytes(a: Uint8Array, b: Uint8Array) {
  if (a.length !== b.length) return false;

  for (let i = 0; i < a.length; i++) {
    if (a[i] !== b[i]) return false;
  }

  return true;
}

function encodeColumnConst(values: Uint8Array[]) {
  if (!values.length) return null;

  const first = values[0];

  for (let i = 1; i < values.length; i++) {
    if (!sameBytes(first, values[i])) return null;
  }

  const w = new BW();

  w.wB(5);
  w.wVar(first.length);
  w.wA(first);

  return w.finish();
}

function writeLensModel(w: BW, lens: number[]) {
  if (!lens.length) {
    w.wB(0);
    w.wVar(0);
    return;
  }

  let same = true;

  for (let i = 1; i < lens.length; i++) {
    if (lens[i] !== lens[0]) {
      same = false;
      break;
    }
  }

  if (same) {
    w.wB(0);
    w.wVar(lens[0]);
    return;
  }

  const rle = new BW();
  let i = 0;

  while (i < lens.length) {
    const v = lens[i];
    let run = 1;

    while (i + run < lens.length && lens[i + run] === v) run++;

    rle.wVar(v);
    rle.wVar(run);
    i += run;
  }

  const rlePayload = rle.finish();

  const raw = new BW();
  for (const l of lens) raw.wVar(l);
  const rawPayload = raw.finish();

  if (rlePayload.length < rawPayload.length) {
    w.wB(2);
    w.wVar(rlePayload.length);
    w.wA(rlePayload);
  } else {
    w.wB(1);
    w.wA(rawPayload);
  }
}

function readLensModel(r: BR, count: number) {
  const mode = r.rB();
  const lens: number[] = [];

  if (mode === 0) {
    const len = r.rVar();

    for (let i = 0; i < count; i++) lens.push(len);

    return lens;
  }

  if (mode === 1) {
    for (let i = 0; i < count; i++) lens.push(r.rVar());

    return lens;
  }

  if (mode === 2) {
    const payloadLen = r.rVar();
    const rr = new BR(r.rA(payloadLen));

    while (lens.length < count) {
      const len = rr.rVar();
      const run = rr.rVar();

      for (let i = 0; i < run; i++) lens.push(len);
    }

    if (lens.length !== count || rr.p !== rr.b.length) throw new Error("bad length RLE");

    return lens;
  }

  throw new Error("bad length model");
}

function parseNumColumn(values: Uint8Array[]) {
  const nums: number[] = [];
  const lens: number[] = [];

  for (const v of values) {
    const n = parseDecValue(v);

    if (n == null) return null;

    nums.push(n);
    lens.push(v.length);
  }

  return { nums, lens };
}

function encodeColumnNumDelta(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);

  if (!parsed) return null;

  const { nums, lens } = parsed;
  const w = new BW();

  w.wB(1);

  let prev = 0;

  for (let i = 0; i < nums.length; i++) {
    w.wVar(lens[i]);
    w.wSVar(nums[i] - prev);
    prev = nums[i];
  }

  return w.finish();
}

function encodeColumnNumDeltaCompact(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);

  if (!parsed) return null;

  const { nums, lens } = parsed;
  const w = new BW();

  w.wB(3);
  writeLensModel(w, lens);

  if (!nums.length) return w.finish();

  w.wVar(nums[0]);

  for (let i = 1; i < nums.length; i++) {
    w.wSVar(nums[i] - nums[i - 1]);
  }

  return w.finish();
}

function encodeColumnNumDelta2Compact(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);

  if (!parsed) return null;

  const { nums, lens } = parsed;
  const w = new BW();

  w.wB(4);
  writeLensModel(w, lens);

  if (!nums.length) return w.finish();

  w.wVar(nums[0]);

  if (nums.length === 1) return w.finish();

  let prevDelta = nums[1] - nums[0];

  w.wSVar(prevDelta);

  for (let i = 2; i < nums.length; i++) {
    const delta = nums[i] - nums[i - 1];
    w.wSVar(delta - prevDelta);
    prevDelta = delta;
  }

  return w.finish();
}


function unsignedBitWidth(max: number) {
  if (max <= 0) return 0;

  let w = 0;
  let x = max;

  while (x > 0) {
    w++;
    x = Math.floor(x / 2);
  }

  return w;
}

function zigZag32(n: number) {
  return n < 0 ? ((-n) * 2 - 1) : n * 2;
}

function unZigZag32(z: number) {
  return (z & 1) ? -((z + 1) / 2) : z / 2;
}

function packUnsigned(values: number[], width: number) {
  const bw = new BitWriter();

  if (width > 0) {
    for (const v of values) {
      bw.writeBits(v >>> 0, width);
    }
  }

  return {
    bitLen: values.length * width,
    payload: bw.finish(),
  };
}

function unpackUnsigned(payload: Uint8Array, bitLen: number, count: number, width: number) {
  const out: number[] = [];
  const br = new BitReader(payload, bitLen);

  if (width === 0) {
    for (let i = 0; i < count; i++) out.push(0);
    return out;
  }

  for (let i = 0; i < count; i++) {
    out.push(br.readBits(width));
  }

  return out;
}

function riceChooseK(zvals: number[]) {
  if (!zvals.length) return 0;
  let sum = 0;
  for (const z of zvals) {
    if (!Number.isSafeInteger(z) || z < 0) return -1;
    sum += z;
  }
  const mean = sum / zvals.length;
  if (!Number.isFinite(mean)) return -1;
  return Math.max(0, Math.min(20, Math.floor(Math.log2(Math.max(1, mean)))));
}

function riceEncodeZigZagSigned(values: number[]) {
  const zvals: number[] = [];
  for (const v of values) {
    const z = zigZag32(v);
    if (!Number.isSafeInteger(z) || z < 0) return null;
    zvals.push(z);
  }
  const k = riceChooseK(zvals);
  if (k < 0) return null;
  const bw = new BitWriter();
  let bitLen = 0;
  const div = Math.pow(2, k);
  for (const z of zvals) {
    const q = Math.floor(z / div);
    const rem = z - q * div;
    if (q > 4096) return null;
    for (let i = 0; i < q; i++) bw.writeBit(1);
    bw.writeBit(0);
    bitLen += q + 1;
    if (k > 0) {
      bw.writeBits(rem, k);
      bitLen += k;
    }
    if (bitLen > 0xffffffff) return null;
  }
  return { k, bitLen, payload: bw.finish() };
}

function riceDecodeZigZagSigned(payload: Uint8Array, bitLen: number, count: number, k: number) {
  const br = new BitReader(payload, bitLen);
  const out: number[] = [];
  const div = Math.pow(2, k);
  for (let i = 0; i < count; i++) {
    let q = 0;
    while (br.readBits(1) === 1) {
      q++;
      if (q > 1000000) throw new Error("rice unary too large");
    }
    const rem = k > 0 ? br.readBits(k) : 0;
    out.push(unZigZag32(q * div + rem));
  }
  return out;
}

function encodeColumnNumDeltaRice(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);
  if (!parsed || parsed.nums.length < 8) return null;
  const { nums, lens } = parsed;
  const deltas: number[] = [];
  for (let i = 1; i < nums.length; i++) deltas.push(nums[i] - nums[i - 1]);
  const rice = riceEncodeZigZagSigned(deltas);
  if (!rice) return null;
  const w = new BW();
  w.wB(19);
  writeLensModel(w, lens);
  w.wVar(nums[0]);
  w.wB(rice.k);
  w.wU(rice.bitLen);
  w.wU(rice.payload.length);
  w.wA(rice.payload);
  return w.finish();
}

function encodeColumnNumDelta2Rice(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);
  if (!parsed || parsed.nums.length < 12) return null;
  const { nums, lens } = parsed;
  let prevDelta = nums[1] - nums[0];
  const residuals: number[] = [];
  for (let i = 2; i < nums.length; i++) {
    const delta = nums[i] - nums[i - 1];
    residuals.push(delta - prevDelta);
    prevDelta = delta;
  }
  const rice = riceEncodeZigZagSigned(residuals);
  if (!rice) return null;
  const w = new BW();
  w.wB(20);
  writeLensModel(w, lens);
  w.wVar(nums[0]);
  w.wSVar(nums[1] - nums[0]);
  w.wB(rice.k);
  w.wU(rice.bitLen);
  w.wU(rice.payload.length);
  w.wA(rice.payload);
  return w.finish();
}

function encodeColumnNumMinBitpack(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);

  if (!parsed || parsed.nums.length < 8) return null;

  const { nums, lens } = parsed;
  let min = nums[0];
  let max = nums[0];

  for (const n of nums) {
    if (n < min) min = n;
    if (n > max) max = n;
  }

  const range = max - min;
  if (range < 0 || range > 0xffffffff) return null;

  const width = unsignedBitWidth(range);
  const vals = nums.map(n => n - min);
  const packed = packUnsigned(vals, width);
  const w = new BW();

  w.wB(7);
  writeLensModel(w, lens);
  w.wVar(min);
  w.wB(width);
  w.wU(packed.bitLen);
  w.wU(packed.payload.length);
  w.wA(packed.payload);

  return w.finish();
}

function encodeColumnNumDeltaBitpack(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);

  if (!parsed || parsed.nums.length < 8) return null;

  const { nums, lens } = parsed;
  const vals: number[] = [];
  let max = 0;

  for (let i = 1; i < nums.length; i++) {
    const z = zigZag32(nums[i] - nums[i - 1]);
    vals.push(z);
    if (z > max) max = z;
  }

  if (max > 0xffffffff) return null;

  const width = unsignedBitWidth(max);
  const packed = packUnsigned(vals, width);
  const w = new BW();

  w.wB(8);
  writeLensModel(w, lens);
  w.wVar(nums[0]);
  w.wB(width);
  w.wU(packed.bitLen);
  w.wU(packed.payload.length);
  w.wA(packed.payload);

  return w.finish();
}

function encodeColumnNumDelta2Bitpack(values: Uint8Array[]) {
  const parsed = parseNumColumn(values);

  if (!parsed || parsed.nums.length < 12) return null;

  const { nums, lens } = parsed;
  let prevDelta = nums[1] - nums[0];
  const vals: number[] = [];
  let max = 0;

  for (let i = 2; i < nums.length; i++) {
    const delta = nums[i] - nums[i - 1];
    const z = zigZag32(delta - prevDelta);
    vals.push(z);
    if (z > max) max = z;
    prevDelta = delta;
  }

  if (max > 0xffffffff) return null;

  const width = unsignedBitWidth(max);
  const packed = packUnsigned(vals, width);
  const w = new BW();

  w.wB(9);
  writeLensModel(w, lens);
  w.wVar(nums[0]);
  w.wSVar(nums[1] - nums[0]);
  w.wB(width);
  w.wU(packed.bitLen);
  w.wU(packed.payload.length);
  w.wA(packed.payload);

  return w.finish();
}

function buildDict(values: Uint8Array[]) {
  const keyOf = (v: Uint8Array) => {
    let s = "";

    for (const b of v) s += String.fromCharCode(b);

    return s;
  };

  const dict = new Map<string, number>();
  const arr: Uint8Array[] = [];
  const idx: number[] = [];

  for (const v of values) {
    const k = keyOf(v);
    let id = dict.get(k);

    if (id === undefined) {
      id = arr.length;
      dict.set(k, id);
      arr.push(v);
    }

    idx.push(id);
  }

  return { arr, idx };
}

function encodeColumnDict(values: Uint8Array[]) {
  if (values.length < 3) return null;

  const { arr, idx } = buildDict(values);

  if (arr.length >= values.length) return null;
  if (arr.length > 4096) return null;

  const w = new BW();

  w.wB(2);
  w.wVar(arr.length);

  for (const v of arr) {
    w.wVar(v.length);
    w.wA(v);
  }

  for (const id of idx) {
    w.wVar(id);
  }

  return w.finish();
}

function bitWidthFor(n: number) {
  let width = 0;
  let x = Math.max(1, n - 1);

  while (x > 0) {
    width++;
    x >>>= 1;
  }

  return Math.max(1, width);
}

function encodeColumnDictPacked(values: Uint8Array[]) {
  if (values.length < 8) return null;

  const { arr, idx } = buildDict(values);

  if (arr.length <= 1 || arr.length >= values.length) return null;
  if (arr.length > 256) return null;

  const bw = new BitWriter();
  const bits = bitWidthFor(arr.length);

  for (const id of idx) {
    bw.writeBits(id, bits);
  }

  const packed = bw.finish();
  const w = new BW();

  w.wB(6);
  w.wVar(arr.length);

  for (const v of arr) {
    w.wVar(v.length);
    w.wA(v);
  }

  w.wB(bits);
  w.wU(idx.length * bits);
  w.wU(packed.length);
  w.wA(packed);

  return w.finish();
}

function encodeColumn(values: Uint8Array[], type: number) {
  const raw = encodeColumnRaw(values);
  let best = raw;

  const cn = encodeColumnConst(values);
  if (cn && cn.length < best.length) best = cn;

  if (type === V_NUM) {
    const nd = encodeColumnNumDelta(values);
    if (nd && nd.length < best.length) best = nd;

    const ndc = encodeColumnNumDeltaCompact(values);
    if (ndc && ndc.length < best.length) best = ndc;

    const nd2 = encodeColumnNumDelta2Compact(values);
    if (nd2 && nd2.length < best.length) best = nd2;

    const nmin = encodeColumnNumMinBitpack(values);
    if (nmin && nmin.length < best.length) best = nmin;

    const ndbp = encodeColumnNumDeltaBitpack(values);
    if (ndbp && ndbp.length < best.length) best = ndbp;

    const nd2bp = encodeColumnNumDelta2Bitpack(values);
    if (nd2bp && nd2bp.length < best.length) best = nd2bp;

    const ndr = encodeColumnNumDeltaRice(values);
    if (ndr && ndr.length < best.length) best = ndr;

    const nd2r = encodeColumnNumDelta2Rice(values);
    if (nd2r && nd2r.length < best.length) best = nd2r;
  }

  const dict = encodeColumnDict(values);
  if (dict && dict.length < best.length) best = dict;

  const packed = encodeColumnDictPacked(values);
  if (packed && packed.length < best.length) best = packed;

  return best;
}

function readColumn(r: BR, count: number, type: number) {
  const mode = r.rB();

  if (mode === 0) {
    const out: Uint8Array[] = [];

    for (let i = 0; i < count; i++) {
      const len = r.rVar();
      out.push(r.rA(len));
    }

    return out;
  }

  if (mode === 1 && type === V_NUM) {
    const out: Uint8Array[] = [];
    let prev = 0;

    for (let i = 0; i < count; i++) {
      const len = r.rVar();
      const delta = r.rSVar();
      const n = prev + delta;
      prev = n;

      out.push(enc.encode(String(n).padStart(len, "0")));
    }

    return out;
  }

  if (mode === 2) {
    const dictLen = r.rVar();
    const dict: Uint8Array[] = [];

    for (let i = 0; i < dictLen; i++) {
      const len = r.rVar();
      dict.push(r.rA(len));
    }

    const out: Uint8Array[] = [];

    for (let i = 0; i < count; i++) {
      const id = r.rVar();

      if (id < 0 || id >= dict.length) throw new Error("bad column dict id");

      out.push(dict[id]);
    }

    return out;
  }

  if (mode === 3 && type === V_NUM) {
    const lens = readLensModel(r, count);
    const out: Uint8Array[] = [];

    if (count === 0) return out;

    let prev = r.rVar();
    out.push(enc.encode(String(prev).padStart(lens[0], "0")));

    for (let i = 1; i < count; i++) {
      prev += r.rSVar();
      out.push(enc.encode(String(prev).padStart(lens[i], "0")));
    }

    return out;
  }

  if (mode === 4 && type === V_NUM) {
    const lens = readLensModel(r, count);
    const out: Uint8Array[] = [];

    if (count === 0) return out;

    let prev = r.rVar();
    out.push(enc.encode(String(prev).padStart(lens[0], "0")));

    if (count === 1) return out;

    let prevDelta = r.rSVar();
    prev += prevDelta;
    out.push(enc.encode(String(prev).padStart(lens[1], "0")));

    for (let i = 2; i < count; i++) {
      prevDelta += r.rSVar();
      prev += prevDelta;
      out.push(enc.encode(String(prev).padStart(lens[i], "0")));
    }

    return out;
  }

  if (mode === 7 && type === V_NUM) {
    const lens = readLensModel(r, count);
    const min = r.rVar();
    const width = r.rB();
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    const vals = unpackUnsigned(payload, bitLen, count, width);
    const out: Uint8Array[] = [];

    for (let i = 0; i < count; i++) {
      out.push(enc.encode(String(min + vals[i]).padStart(lens[i], "0")));
    }

    return out;
  }

  if (mode === 8 && type === V_NUM) {
    const lens = readLensModel(r, count);
    const out: Uint8Array[] = [];

    if (count === 0) return out;

    let prev = r.rVar();
    out.push(enc.encode(String(prev).padStart(lens[0], "0")));

    const width = r.rB();
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    const vals = unpackUnsigned(payload, bitLen, Math.max(0, count - 1), width);

    for (let i = 1; i < count; i++) {
      prev += unZigZag32(vals[i - 1]);
      out.push(enc.encode(String(prev).padStart(lens[i], "0")));
    }

    return out;
  }

  if (mode === 9 && type === V_NUM) {
    const lens = readLensModel(r, count);
    const out: Uint8Array[] = [];

    if (count === 0) return out;

    let prev = r.rVar();
    out.push(enc.encode(String(prev).padStart(lens[0], "0")));

    if (count === 1) return out;

    let prevDelta = r.rSVar();
    prev += prevDelta;
    out.push(enc.encode(String(prev).padStart(lens[1], "0")));

    const width = r.rB();
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    const vals = unpackUnsigned(payload, bitLen, Math.max(0, count - 2), width);

    for (let i = 2; i < count; i++) {
      prevDelta += unZigZag32(vals[i - 2]);
      prev += prevDelta;
      out.push(enc.encode(String(prev).padStart(lens[i], "0")));
    }

    return out;
  }

  if (mode === 19 && type === V_NUM) {
    const lens = readLensModel(r, count);
    const out: Uint8Array[] = [];
    if (count === 0) return out;
    let prev = r.rVar();
    out.push(enc.encode(String(prev).padStart(lens[0], "0")));
    const k = r.rB();
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    const deltas = riceDecodeZigZagSigned(payload, bitLen, Math.max(0, count - 1), k);
    for (let i = 1; i < count; i++) {
      prev += deltas[i - 1];
      out.push(enc.encode(String(prev).padStart(lens[i], "0")));
    }
    return out;
  }

  if (mode === 20 && type === V_NUM) {
    const lens = readLensModel(r, count);
    const out: Uint8Array[] = [];
    if (count === 0) return out;
    let prev = r.rVar();
    out.push(enc.encode(String(prev).padStart(lens[0], "0")));
    if (count === 1) return out;
    let prevDelta = r.rSVar();
    prev += prevDelta;
    out.push(enc.encode(String(prev).padStart(lens[1], "0")));
    const k = r.rB();
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    const residuals = riceDecodeZigZagSigned(payload, bitLen, Math.max(0, count - 2), k);
    for (let i = 2; i < count; i++) {
      prevDelta += residuals[i - 2];
      prev += prevDelta;
      out.push(enc.encode(String(prev).padStart(lens[i], "0")));
    }
    return out;
  }

  if (mode === 5) {
    const len = r.rVar();
    const v = r.rA(len);
    const out: Uint8Array[] = [];

    for (let i = 0; i < count; i++) out.push(v);

    return out;
  }

  if (mode === 6) {
    const dictLen = r.rVar();
    const dict: Uint8Array[] = [];

    for (let i = 0; i < dictLen; i++) {
      const len = r.rVar();
      dict.push(r.rA(len));
    }

    const bits = r.rB();
    const bitLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);
    const br = new BitReader(payload, bitLen);
    const out: Uint8Array[] = [];

    for (let i = 0; i < count; i++) {
      const id = br.readBits(bits);

      if (id < 0 || id >= dict.length) throw new Error("bad packed dict id");

      out.push(dict[id]);
    }

    return out;
  }

  throw new Error("bad column mode");
}

function transformTLM2(data: Uint8Array, opts: MineTemplateOptions = {}) {
  if (!looksTextual(data)) return null;

  const lines = splitLines(data);

  if (lines.length < 4) return null;

  const mined = mineTemplates(lines, opts);

  if (!mined.selected.length) return null;

  const columns: Uint8Array[][][] = mined.selected.map(t =>
    Array.from({ length: t.types.length }, () => [])
  );

  const entries: ({ tag: 0; raw: Uint8Array } | { tag: 1; tid: number })[] = [];

  let hits = 0;
  let rawLines = 0;
  let vars = 0;

  for (let i = 0; i < lines.length; i++) {
    const sh = mined.shapes[i];
    const tpl = sh ? mined.byKey.get(sh.key) : undefined;

    if (sh && tpl) {
      entries.push({ tag: 1, tid: tpl.id });

      for (let j = 0; j < sh.vars.length; j++) {
        columns[tpl.id][j].push(sh.vars[j]);
        vars++;
      }

      hits++;
    } else {
      entries.push({ tag: 0, raw: lines[i] });
      rawLines++;
    }
  }

  const w = new BW();

  w.wA(enc.encode("TLM2"));
  w.wB(2);
  w.wU(data.length);

  writeTemplates(w, mined.selected);

  w.wVar(entries.length);

  let prevTid = -1;
  let sameTidHits = 0;

  for (const e of entries) {
    if (e.tag === 0) {
      w.wB(0);
      w.wVar(e.raw.length);
      w.wA(e.raw);
    } else if (e.tid === prevTid) {
      w.wB(2);
      sameTidHits++;
    } else {
      w.wB(1);
      w.wVar(e.tid);
      prevTid = e.tid;
    }
  }

  for (const t of mined.selected) {
    for (let slot = 0; slot < t.types.length; slot++) {
      const payload = encodeColumn(columns[t.id][slot], t.types[slot]);
      w.wVar(payload.length);
      w.wA(payload);
    }
  }

  const out = w.finish();

  const variant = opts.name ? ` variant=${opts.name}` : "";

  return {
    data: out,
    info: `tpl=${mined.selected.length} hits=${hits} rawLines=${rawLines} vars=${vars} sameTid=${sameTidHits}${variant}`,
  };
}

function inverseTLM2(data: Uint8Array) {
  const r = new BR(data);

  if (dec.decode(r.rA(4)) !== "TLM2") throw new Error("bad TLM2 magic");

  const ver = r.rB();

  if (ver !== 1 && ver !== 2) throw new Error("bad TLM2 version");

  const originalLen = r.rU();
  const templates = readTemplates(r);
  const lineCount = r.rVar();

  const entries: ({ tag: 0; raw: Uint8Array } | { tag: 1; tid: number })[] = [];
  const counts = new Array(templates.length).fill(0);
  let prevTid = -1;

  for (let i = 0; i < lineCount; i++) {
    const tag = r.rB();

    if (tag === 0) {
      const len = r.rVar();
      entries.push({ tag: 0, raw: r.rA(len) });
    } else if (tag === 1) {
      const tid = r.rVar();

      if (tid < 0 || tid >= templates.length) throw new Error("bad TLM2 template id");

      entries.push({ tag: 1, tid });
      counts[tid]++;
      prevTid = tid;
    } else if (tag === 2 && ver === 2) {
      if (prevTid < 0 || prevTid >= templates.length) throw new Error("bad TLM2 repeated template id");
      entries.push({ tag: 1, tid: prevTid });
      counts[prevTid]++;
    } else {
      throw new Error("bad TLM2 line tag");
    }
  }

  const columns: Uint8Array[][][] = templates.map(t =>
    Array.from({ length: t.types.length }, () => [])
  );

  for (let tid = 0; tid < templates.length; tid++) {
    const tpl = templates[tid];

    for (let slot = 0; slot < tpl.types.length; slot++) {
      const payloadLen = r.rVar();
      const payload = r.rA(payloadLen);
      columns[tid][slot] = readColumn(new BR(payload), counts[tid], tpl.types[slot]);
    }
  }

  if (r.p !== data.length) throw new Error("TLM2 trailing bytes");

  const ptrs = templates.map(t => new Array(t.types.length).fill(0));
  const parts: Uint8Array[] = [];

  for (const e of entries) {
    if (e.tag === 0) {
      parts.push(e.raw);
    } else {
      const tpl = templates[e.tid];
      const out: Uint8Array[] = [];

      for (let j = 0; j < tpl.types.length; j++) {
        out.push(tpl.parts[j]);

        const p = ptrs[e.tid][j]++;

        if (p >= columns[e.tid][j].length) throw new Error("TLM2 column underflow");

        out.push(columns[e.tid][j][p]);
      }

      out.push(tpl.parts[tpl.parts.length - 1]);
      parts.push(concat(out));
    }
  }

  const out = concat(parts);

  if (out.length !== originalLen) throw new Error("TLM2 size mismatch");

  return out;
}


function delta1(d: Uint8Array) {
  const out = new Uint8Array(d.length);

  if (!d.length) return out;

  out[0] = d[0];

  for (let i = 1; i < d.length; i++) {
    out[i] = (d[i] - d[i - 1]) & 255;
  }

  return out;
}

function undelta1(d: Uint8Array) {
  const out = new Uint8Array(d.length);

  if (!d.length) return out;

  out[0] = d[0];

  for (let i = 1; i < d.length; i++) {
    out[i] = (out[i - 1] + d[i]) & 255;
  }

  return out;
}

function xor1(d: Uint8Array) {
  const out = new Uint8Array(d.length);

  if (!d.length) return out;

  out[0] = d[0];

  for (let i = 1; i < d.length; i++) {
    out[i] = d[i] ^ d[i - 1];
  }

  return out;
}

function unxor1(d: Uint8Array) {
  const out = new Uint8Array(d.length);

  if (!d.length) return out;

  out[0] = d[0];

  for (let i = 1; i < d.length; i++) {
    out[i] = out[i - 1] ^ d[i];
  }

  return out;
}

function shuffleWidth(d: Uint8Array, width: number) {
  const groups = Math.floor(d.length / width);
  const main = groups * width;
  const out = new Uint8Array(d.length);
  let p = 0;

  for (let k = 0; k < width; k++) {
    for (let g = 0; g < groups; g++) {
      out[p++] = d[g * width + k];
    }
  }

  out.set(d.subarray(main), p);

  return out;
}

function unshuffleWidth(d: Uint8Array, width: number) {
  const groups = Math.floor(d.length / width);
  const main = groups * width;
  const out = new Uint8Array(d.length);
  let p = 0;

  for (let k = 0; k < width; k++) {
    for (let g = 0; g < groups; g++) {
      out[g * width + k] = d[p++];
    }
  }

  out.set(d.subarray(p), main);

  return out;
}

function setPackedNibble(out: Uint8Array, base: number, idx: number, value: number) {
  const p = base + (idx >> 1);

  if ((idx & 1) === 0) {
    out[p] = (out[p] & 0x0f) | ((value & 15) << 4);
  } else {
    out[p] = (out[p] & 0xf0) | (value & 15);
  }
}

function getPackedNibble(d: Uint8Array, base: number, idx: number) {
  const x = d[base + (idx >> 1)];
  return (idx & 1) === 0 ? (x >>> 4) & 15 : x & 15;
}

function nibbleSplit(d: Uint8Array) {
  const half = Math.ceil(d.length / 2);
  const out = new Uint8Array(half * 2);

  for (let i = 0; i < d.length; i++) {
    setPackedNibble(out, 0, i, d[i] >>> 4);
    setPackedNibble(out, half, i, d[i] & 15);
  }

  return out;
}

function unnibbleSplit(d: Uint8Array, originalLength: number) {
  const half = Math.ceil(originalLength / 2);
  const out = new Uint8Array(originalLength);

  for (let i = 0; i < originalLength; i++) {
    out[i] = (getPackedNibble(d, 0, i) << 4) | getPackedNibble(d, half, i);
  }

  return out;
}


function bitplaneSplit(d: Uint8Array) {
  const planeBytes = Math.ceil(d.length / 8);
  const out = new Uint8Array(planeBytes * 8);

  for (let i = 0; i < d.length; i++) {
    const v = d[i];

    for (let bit = 0; bit < 8; bit++) {
      if ((v >>> (7 - bit)) & 1) {
        out[bit * planeBytes + (i >> 3)] |= 1 << (7 - (i & 7));
      }
    }
  }

  return out;
}

function unbitplaneSplit(d: Uint8Array, originalLength: number) {
  const planeBytes = Math.ceil(originalLength / 8);
  const out = new Uint8Array(originalLength);

  if (d.length < planeBytes * 8) throw new Error("bad bitplane payload");

  for (let i = 0; i < originalLength; i++) {
    let v = 0;

    for (let bit = 0; bit < 8; bit++) {
      const set = (d[bit * planeBytes + (i >> 3)] >>> (7 - (i & 7))) & 1;
      v = (v << 1) | set;
    }

    out[i] = v;
  }

  return out;
}

function getU32LE(d: Uint8Array, p: number) {
  return (d[p] | (d[p + 1] << 8) | (d[p + 2] << 16) | (d[p + 3] * 0x1000000)) >>> 0;
}

function setU32LE(d: Uint8Array, p: number, v: number) {
  d[p] = v & 255;
  d[p + 1] = (v >>> 8) & 255;
  d[p + 2] = (v >>> 16) & 255;
  d[p + 3] = (v >>> 24) & 255;
}

function delta32LE(d: Uint8Array) {
  const out = d.slice();
  const n = Math.floor(d.length / 4);
  let prev = 0;

  for (let i = 0; i < n; i++) {
    const p = i * 4;
    const v = getU32LE(d, p);
    setU32LE(out, p, (v - prev) >>> 0);
    prev = v;
  }

  return out;
}

function undelta32LE(d: Uint8Array) {
  const out = d.slice();
  const n = Math.floor(d.length / 4);
  let prev = 0;

  for (let i = 0; i < n; i++) {
    const p = i * 4;
    const dv = getU32LE(d, p);
    const v = (prev + dv) >>> 0;
    setU32LE(out, p, v);
    prev = v;
  }

  return out;
}

function ieee32Split(d: Uint8Array) {
  const n = Math.floor(d.length / 4);
  const signBytes = Math.ceil(n / 8);
  const out = new Uint8Array(n + signBytes + n * 3 + (d.length - n * 4));
  let p = 0;

  for (let i = 0; i < n; i++) {
    const b2 = d[i * 4 + 2];
    const b3 = d[i * 4 + 3];
    out[p++] = ((b3 & 0x7f) << 1) | (b2 >>> 7);
  }

  const signBase = p;
  p += signBytes;

  for (let i = 0; i < n; i++) {
    if (d[i * 4 + 3] & 0x80) out[signBase + (i >> 3)] |= 1 << (7 - (i & 7));
  }

  for (let i = 0; i < n; i++) out[p++] = d[i * 4];
  for (let i = 0; i < n; i++) out[p++] = d[i * 4 + 1];
  for (let i = 0; i < n; i++) out[p++] = d[i * 4 + 2] & 0x7f;
  out.set(d.subarray(n * 4), p);

  return out;
}

function unieee32Split(d: Uint8Array, originalLength: number) {
  const n = Math.floor(originalLength / 4);
  const signBytes = Math.ceil(n / 8);
  const needed = n + signBytes + n * 3 + (originalLength - n * 4);

  if (d.length < needed) throw new Error("bad ieee32 payload");

  const out = new Uint8Array(originalLength);
  let p = 0;
  const expBase = p;
  p += n;
  const signBase = p;
  p += signBytes;
  const m0Base = p;
  p += n;
  const m1Base = p;
  p += n;
  const m2Base = p;
  p += n;

  for (let i = 0; i < n; i++) {
    const e = d[expBase + i];
    const sign = (d[signBase + (i >> 3)] >>> (7 - (i & 7))) & 1;
    out[i * 4] = d[m0Base + i];
    out[i * 4 + 1] = d[m1Base + i];
    out[i * 4 + 2] = ((e & 1) << 7) | (d[m2Base + i] & 0x7f);
    out[i * 4 + 3] = (sign << 7) | (e >>> 1);
  }

  out.set(d.subarray(p, p + (originalLength - n * 4)), n * 4);

  return out;
}

function bcjX86(d: Uint8Array, decodeMode: boolean) {
  const out = d.slice();

  for (let i = 0; i + 4 < out.length; i++) {
    const op = out[i];

    if (op !== 0xe8 && op !== 0xe9) continue;

    const v =
      out[i + 1] |
      (out[i + 2] << 8) |
      (out[i + 3] << 16) |
      (out[i + 4] * 0x1000000);

    const n = decodeMode ? (v - i - 5) >>> 0 : (v + i + 5) >>> 0;

    out[i + 1] = n & 255;
    out[i + 2] = (n >>> 8) & 255;
    out[i + 3] = (n >>> 16) & 255;
    out[i + 4] = (n >>> 24) & 255;

    i += 4;
  }

  return out;
}

function byteEntropyScore(d: Uint8Array) {
  if (!d.length) return 0;

  const f = new Uint32Array(256);

  for (const x of d) f[x]++;

  let h = 0;

  for (let i = 0; i < 256; i++) {
    if (!f[i]) continue;

    const p = f[i] / d.length;
    h -= p * Math.log2(p);
  }

  return h;
}

function encodeOneBinaryBlock(block: Uint8Array) {
  let bestMode = 0;
  let bestPayload = block;
  let bestCost = 1 + 4 + 4 + block.length;

  const tryPayload = (mode: number, transformed: Uint8Array, forceRaw = false) => {
    const core = forceRaw ? transformed : coreEncode(transformed);
    const cost = 1 + 4 + 4 + core.length;

    if (cost < bestCost) {
      bestMode = mode;
      bestPayload = core;
      bestCost = cost;
    }
  };

  tryPayload(1, block);

  const entropy = byteEntropyScore(block);
  const allowHeavy = block.length >= 4096 && entropy < 7.92;

  if (allowHeavy) {
    tryPayload(2, delta1(block));
    tryPayload(3, xor1(block));
    tryPayload(4, shuffleWidth(block, 2));
    tryPayload(5, shuffleWidth(block, 4));

    if (block.length >= 8192) {
      tryPayload(6, shuffleWidth(block, 8));
    }

    tryPayload(7, nibbleSplit(block));
    tryPayload(9, bitplaneSplit(block));

    if (block.length >= 4096 && block.length % 4 !== 1) {
      tryPayload(10, delta32LE(block));
      tryPayload(11, ieee32Split(block));
    }

    let bcjMarkers = 0;
    for (let i = 0; i + 4 < block.length; i++) {
      if (block[i] === 0xe8 || block[i] === 0xe9) bcjMarkers++;
    }

    if (bcjMarkers >= 8) {
      tryPayload(8, bcjX86(block, false));
    }
  }

  return {
    mode: bestMode,
    payload: bestPayload,
  };
}

function decodeOneBinaryBlock(mode: number, payload: Uint8Array, rawLen: number) {
  if (mode === 0) return payload.slice(0, rawLen);

  const decoded = coreDecode(payload);

  if (mode === 1) return decoded;
  if (mode === 2) return undelta1(decoded);
  if (mode === 3) return unxor1(decoded);
  if (mode === 4) return unshuffleWidth(decoded, 2);
  if (mode === 5) return unshuffleWidth(decoded, 4);
  if (mode === 6) return unshuffleWidth(decoded, 8);
  if (mode === 7) return unnibbleSplit(decoded, rawLen);
  if (mode === 8) return bcjX86(decoded, true);
  if (mode === 9) return unbitplaneSplit(decoded, rawLen);
  if (mode === 10) return undelta32LE(decoded);
  if (mode === 11) return unieee32Split(decoded, rawLen);

  throw new Error("bad binary block mode");
}

function encodeBlockedCoreWithSize(data: Uint8Array, blockSize: number) {
  const w = new BW();
  const blockCount = Math.ceil(data.length / blockSize);

  w.wA(enc.encode("BLC1"));
  w.wB(1);
  w.wU(blockSize);
  w.wU(blockCount);

  let coreBlocks = 0;
  let rawBlocks = 0;
  let transformedBlocks = 0;

  for (let p = 0; p < data.length; p += blockSize) {
    const block = data.subarray(p, Math.min(p + blockSize, data.length));
    const encoded = encodeOneBinaryBlock(block);

    w.wB(encoded.mode);
    w.wU(block.length);
    w.wU(encoded.payload.length);
    w.wA(encoded.payload);

    if (encoded.mode === 0) rawBlocks++;
    else if (encoded.mode === 1) coreBlocks++;
    else transformedBlocks++;
  }

  return {
    data: w.finish(),
    info: `bs=${fmt(blockSize)} blocks=${blockCount} raw=${rawBlocks} core=${coreBlocks} xform=${transformedBlocks}`,
  };
}

function encodeBlockedCore(data: Uint8Array) {
  if (data.length < 128 * 1024) return null;

  const sizes = data.length < 1024 * 1024
    ? [128 * 1024, 256 * 1024]
    : [256 * 1024, 512 * 1024];

  let best: { data: Uint8Array; info: string } | null = null;

  for (const size of sizes) {
    const candidate = encodeBlockedCoreWithSize(data, size);

    if (!best || candidate.data.length < best.data.length) {
      best = candidate;
    }
  }

  return best;
}

function decodeBlockedCore(data: Uint8Array) {
  const r = new BR(data);

  if (dec.decode(r.rA(4)) !== "BLC1") throw new Error("bad BLC1 magic");

  const version = r.rB();

  if (version !== 1) throw new Error("bad BLC1 version");

  const blockSize = r.rU();
  const blockCount = r.rU();
  const parts: Uint8Array[] = [];

  for (let i = 0; i < blockCount; i++) {
    const mode = r.rB();
    const rawLen = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);

    if (rawLen > blockSize && i + 1 !== blockCount) {
      throw new Error("bad BLC1 block size");
    }

    parts.push(decodeOneBinaryBlock(mode, payload, rawLen));
  }

  if (r.p !== data.length) throw new Error("BLC1 trailing bytes");

  return concat(parts);
}


function cyclicSuffixArray(d: Uint8Array) {
  const n = d.length;
  const sa = Array.from({ length: n }, (_, i) => i);
  let rank = new Int32Array(n);
  let nextRank = new Int32Array(n);

  for (let i = 0; i < n; i++) rank[i] = d[i];

  for (let k = 1; k < n; k <<= 1) {
    sa.sort((a, b) => {
      const ra = rank[a] - rank[b];
      if (ra) return ra;

      const rb = rank[(a + k) % n] - rank[(b + k) % n];
      if (rb) return rb;

      return a - b;
    });

    nextRank[sa[0]] = 0;
    let cls = 0;

    for (let i = 1; i < n; i++) {
      const a = sa[i - 1];
      const b = sa[i];

      if (rank[a] !== rank[b] || rank[(a + k) % n] !== rank[(b + k) % n]) {
        cls++;
      }

      nextRank[b] = cls;
    }

    const tmp = rank;
    rank = nextRank;
    nextRank = tmp;

    if (cls === n - 1) break;
  }

  return sa;
}

function mtfEncode(d: Uint8Array) {
  const list = new Uint8Array(256);
  const pos = new Uint8Array(256);
  const out = new Uint8Array(d.length);

  for (let i = 0; i < 256; i++) {
    list[i] = i;
    pos[i] = i;
  }

  for (let i = 0; i < d.length; i++) {
    const sym = d[i];
    const idx = pos[sym];

    out[i] = idx;

    if (idx) {
      for (let j = idx; j > 0; j--) {
        const v = list[j - 1];
        list[j] = v;
        pos[v] = j;
      }

      list[0] = sym;
      pos[sym] = 0;
    }
  }

  return out;
}

function mtfDecode(d: Uint8Array) {
  const list = new Uint8Array(256);
  const out = new Uint8Array(d.length);

  for (let i = 0; i < 256; i++) list[i] = i;

  for (let i = 0; i < d.length; i++) {
    const idx = d[i];
    const sym = list[idx];

    out[i] = sym;

    if (idx) {
      for (let j = idx; j > 0; j--) {
        list[j] = list[j - 1];
      }

      list[0] = sym;
    }
  }

  return out;
}

function bwtBlockEncode(block: Uint8Array) {
  const n = block.length;

  if (n === 0) {
    return {
      primary: 0,
      data: new Uint8Array(),
    };
  }

  if (n === 1) {
    return {
      primary: 0,
      data: mtfEncode(block),
    };
  }

  const sa = cyclicSuffixArray(block);
  const bwt = new Uint8Array(n);
  let primary = 0;

  for (let i = 0; i < n; i++) {
    const p = sa[i];

    if (p === 0) primary = i;

    bwt[i] = block[(p + n - 1) % n];
  }

  return {
    primary,
    data: mtfEncode(bwt),
  };
}

function bwtBlockDecode(payload: Uint8Array, primary: number) {
  const l = mtfDecode(payload);
  const n = l.length;

  if (n === 0) return new Uint8Array();
  if (primary < 0 || primary >= n) throw new Error("bad BWT primary");

  const counts = new Uint32Array(256);

  for (const x of l) counts[x]++;

  const starts = new Uint32Array(256);
  let sum = 0;

  for (let i = 0; i < 256; i++) {
    starts[i] = sum;
    sum += counts[i];
  }

  const seen = new Uint32Array(256);
  const next = new Uint32Array(n);

  for (let i = 0; i < n; i++) {
    const c = l[i];
    next[i] = starts[c] + seen[c];
    seen[c]++;
  }

  const out = new Uint8Array(n);
  let p = primary;

  for (let i = n - 1; i >= 0; i--) {
    const c = l[p];
    out[i] = c;
    p = next[p];
  }

  return out;
}

function transformBWT1(data: Uint8Array) {
  if (data.length < 4096 || data.length > 1024 * 1024) return null;

  const textual = looksTextual(data);
  const entropy = byteEntropyScore(data.subarray(0, Math.min(data.length, 256 * 1024)));

  if (!textual && entropy > 7.35) return null;

  const blockSize = data.length <= 128 * 1024 ? 32768 : 65536;
  const blockCount = Math.ceil(data.length / blockSize);
  const w = new BW();

  w.wA(enc.encode("BWT1"));
  w.wB(1);
  w.wU(data.length);
  w.wU(blockSize);
  w.wU(blockCount);

  for (let p = 0; p < data.length; p += blockSize) {
    const block = data.subarray(p, Math.min(p + blockSize, data.length));
    const encoded = bwtBlockEncode(block);

    w.wU(block.length);
    w.wU(encoded.primary);
    w.wU(encoded.data.length);
    w.wA(encoded.data);
  }

  return {
    data: w.finish(),
    info: `bs=${fmt(blockSize)} blocks=${blockCount}`,
  };
}

function inverseBWT1(data: Uint8Array) {
  const r = new BR(data);

  if (dec.decode(r.rA(4)) !== "BWT1") throw new Error("bad BWT1 magic");

  const version = r.rB();

  if (version !== 1) throw new Error("bad BWT1 version");

  const originalLen = r.rU();
  const blockSize = r.rU();
  const blockCount = r.rU();
  const parts: Uint8Array[] = [];

  for (let i = 0; i < blockCount; i++) {
    const rawLen = r.rU();
    const primary = r.rU();
    const payloadLen = r.rU();
    const payload = r.rA(payloadLen);

    if (rawLen > blockSize && i + 1 !== blockCount) {
      throw new Error("bad BWT1 block size");
    }

    const block = bwtBlockDecode(payload, primary);

    if (block.length !== rawLen) throw new Error("bad BWT1 decoded block size");

    parts.push(block);
  }

  if (r.p !== data.length) throw new Error("BWT1 trailing bytes");

  const out = concat(parts);

  if (out.length !== originalLen) throw new Error("BWT1 size mismatch");

  return out;
}


function bytesKey(d: Uint8Array, p: number, len: number) {
  let s = "";

  for (let i = 0; i < len; i++) s += String.fromCharCode(d[p + i]);

  return s;
}

type STTPhrase = {
  id: number;
  bytes: Uint8Array;
  first: number;
};

function transformSTT1(data: Uint8Array) {
  if (!looksTextual(data)) return null;
  if (data.length < 4096 || data.length > 2 * 1024 * 1024) return null;

  const maxScan = Math.min(data.length, 1024 * 1024);
  const counts = new Map<string, { count: number; bytes: Uint8Array }>();

  for (let i = 0; i < maxScan; i++) {
    const first = data[i];

    if (first === 255 || first === 10 || first === 13 || first === 9 || first === 32) continue;

    for (let len = 3; len <= 10 && i + len <= maxScan; len++) {
      const last = data[i + len - 1];

      if (last === 10 || last === 13) break;

      const k = bytesKey(data, i, len);
      const old = counts.get(k);

      if (old) old.count++;
      else counts.set(k, { count: 1, bytes: data.slice(i, i + len) });
    }
  }

  const candidates: { score: number; bytes: Uint8Array; count: number }[] = [];

  for (const v of counts.values()) {
    if (v.count < 4) continue;

    const len = v.bytes.length;
    const score = (len - 2) * v.count - len - 4;

    if (score > 16) candidates.push({ score, bytes: v.bytes, count: v.count });
  }

  candidates.sort((a, b) => b.score - a.score || b.bytes.length - a.bytes.length);

  const selected: STTPhrase[] = [];
  const seen = new Set<string>();

  for (const c of candidates) {
    const k = bytesKey(c.bytes, 0, c.bytes.length);
    if (seen.has(k)) continue;

    selected.push({ id: selected.length, bytes: c.bytes, first: c.bytes[0] });
    seen.add(k);

    if (selected.length >= 254) break;
  }

  if (!selected.length) return null;

  const byFirst: STTPhrase[][] = Array.from({ length: 256 }, () => []);

  for (const p of selected) byFirst[p.first].push(p);

  for (const list of byFirst) {
    list.sort((a, b) => b.bytes.length - a.bytes.length || a.id - b.id);
  }

  const payload = new BW();
  let hits = 0;
  let escaped = 0;

  for (let i = 0; i < data.length;) {
    let found: STTPhrase | null = null;
    const list = byFirst[data[i]];

    for (const p of list) {
      if (i + p.bytes.length > data.length) continue;

      let ok = true;

      for (let k = 0; k < p.bytes.length; k++) {
        if (data[i + k] !== p.bytes[k]) {
          ok = false;
          break;
        }
      }

      if (ok) {
        found = p;
        break;
      }
    }

    if (found) {
      payload.wB(255);
      payload.wB(found.id);
      i += found.bytes.length;
      hits++;
    } else {
      const b = data[i++];

      if (b === 255) {
        payload.wB(255);
        payload.wB(255);
        escaped++;
      } else {
        payload.wB(b);
      }
    }
  }

  const encodedPayload = payload.finish();
  const w = new BW();

  w.wA(enc.encode("STT1"));
  w.wB(1);
  w.wU(data.length);
  w.wVar(selected.length);

  for (const p of selected) {
    w.wVar(p.bytes.length);
    w.wA(p.bytes);
  }

  w.wU(encodedPayload.length);
  w.wA(encodedPayload);

  const out = w.finish();

  if (out.length >= data.length) return null;

  return {
    data: out,
    info: `dict=${selected.length} hits=${hits} esc=${escaped}`,
  };
}

function inverseSTT1(data: Uint8Array) {
  const r = new BR(data);

  if (dec.decode(r.rA(4)) !== "STT1") throw new Error("bad STT1 magic");

  const version = r.rB();

  if (version !== 1) throw new Error("bad STT1 version");

  const originalLen = r.rU();
  const dictLen = r.rVar();
  const dict: Uint8Array[] = [];

  for (let i = 0; i < dictLen; i++) {
    const len = r.rVar();
    dict.push(r.rA(len));
  }

  const payloadLen = r.rU();
  const payload = r.rA(payloadLen);

  if (r.p !== data.length) throw new Error("STT1 trailing bytes");

  const out = new BW();

  for (let i = 0; i < payload.length;) {
    const b = payload[i++];

    if (b !== 255) {
      out.wB(b);
      continue;
    }

    if (i >= payload.length) throw new Error("STT1 bad escape");

    const id = payload[i++];

    if (id === 255) out.wB(255);
    else {
      if (id >= dict.length) throw new Error("STT1 bad dict id");
      out.wA(dict[id]);
    }
  }

  const decoded = out.finish();

  if (decoded.length !== originalLen) throw new Error("STT1 size mismatch");

  return decoded;
}


function lexKeyOf(bytes: Uint8Array) {
  let s = "";

  for (let i = 0; i < bytes.length; i++) {
    s += String.fromCharCode(bytes[i]);
  }

  return s;
}

function lexIsRunByte(x: number) {
  return (
    (x >= 48 && x <= 57) ||
    (x >= 65 && x <= 90) ||
    (x >= 97 && x <= 122) ||
    x === 95 || x === 45 || x === 46 || x === 47 || x === 58 || x === 64 || x === 36
  );
}

function lexIsMostlyNumeric(bytes: Uint8Array) {
  if (!bytes.length) return false;

  let digits = 0;

  for (const b of bytes) {
    if (b >= 48 && b <= 57) digits++;
  }

  return digits / bytes.length > 0.75;
}

function lexVarLen(n: number) {
  let c = 1;

  while (n >= 0x80) {
    n = Math.floor(n / 128);
    c++;
  }

  return c;
}

function addLexCandidate(
  map: Map<string, { bytes: Uint8Array; count: number }>,
  bytes: Uint8Array,
  maxMapSize: number
) {
  if (bytes.length < 4 || bytes.length > 128) return;
  if (lexIsMostlyNumeric(bytes)) return;

  const key = lexKeyOf(bytes);
  const old = map.get(key);

  if (old) {
    old.count++;
    return;
  }

  if (map.size >= maxMapSize) return;

  map.set(key, { bytes: bytes.slice(), count: 1 });
}

function mineLex1Dictionary(data: Uint8Array) {
  const map = new Map<string, { bytes: Uint8Array; count: number }>();
  const maxMapSize = data.length > 8 * 1024 * 1024 ? 180000 : 90000;

  for (let i = 0; i < data.length;) {
    const b = data[i];

    if (lexIsRunByte(b)) {
      const start = i;
      i++;

      while (i < data.length && lexIsRunByte(data[i])) i++;

      const len = i - start;

      if (len >= 4) {
        const maxLen = Math.min(128, len);
        addLexCandidate(map, data.slice(start, start + maxLen), maxMapSize);

        if (len >= 16) {
          addLexCandidate(map, data.slice(start, start + Math.min(32, len)), maxMapSize);
          addLexCandidate(map, data.slice(start + len - Math.min(32, len), start + len), maxMapSize);
        }
      }

      continue;
    }

    if (b === 34 || b === 39 || b === 96) {
      const quote = b;
      const start = i;
      i++;
      let escaped = false;

      while (i < data.length) {
        const x = data[i++];

        if (escaped) {
          escaped = false;
          continue;
        }

        if (x === 92) {
          escaped = true;
          continue;
        }

        if (x === quote) break;
      }

      const len = i - start;

      if (len >= 6 && len <= 160) {
        addLexCandidate(map, data.slice(start, i), maxMapSize);
      }

      continue;
    }

    i++;
  }

  const scored: { bytes: Uint8Array; score: number; count: number }[] = [];

  for (const v of map.values()) {
    if (v.count < 2) continue;

    const replaceCost = 2;
    const dictCost = v.bytes.length + lexVarLen(v.bytes.length) + 2;
    const score = v.count * (v.bytes.length - replaceCost) - dictCost;

    if (score > 16) {
      scored.push({ bytes: v.bytes, score, count: v.count });
    }
  }

  scored.sort((a, b) => b.score - a.score || b.bytes.length - a.bytes.length);

  const dict: Uint8Array[] = [];
  const seen = new Set<string>();

  for (const item of scored) {
    const key = lexKeyOf(item.bytes);
    if (seen.has(key)) continue;

    let redundant = false;
    if (item.score < 128) {
      for (let i = 0; i < Math.min(dict.length, 64); i++) {
        const dk = lexKeyOf(dict[i]);
        if (dk.includes(key)) {
          redundant = true;
          break;
        }
      }
    }

    if (redundant) continue;

    seen.add(key);
    dict.push(item.bytes);

    if (dict.length >= 4096) break;
  }

  return dict;
}

function transformLEX1(data: Uint8Array) {
  if (!looksTextual(data) && data.length < 128 * 1024) return null;
  if (data.length < 2048) return null;

  const dict = mineLex1Dictionary(data);

  if (!dict.length) return null;

  const byFirst = new Map<number, { id: number; bytes: Uint8Array }[]>();

  for (let id = 0; id < dict.length; id++) {
    const bytes = dict[id];
    const first = bytes[0];
    const arr = byFirst.get(first) ?? [];
    arr.push({ id, bytes });
    byFirst.set(first, arr);
  }

  for (const arr of byFirst.values()) {
    arr.sort((a, b) => b.bytes.length - a.bytes.length || a.id - b.id);
  }

  const payload = new BW();
  let hits = 0;
  let saved = 0;

  for (let i = 0; i < data.length;) {
    const b = data[i];
    const arr = byFirst.get(b);
    let bestId = -1;
    let bestLen = 0;

    if (arr) {
      for (const c of arr) {
        const bytes = c.bytes;

        if (bytes.length <= bestLen) break;
        if (i + bytes.length > data.length) continue;

        let ok = true;
        for (let k = 0; k < bytes.length; k++) {
          if (data[i + k] !== bytes[k]) {
            ok = false;
            break;
          }
        }

        if (ok) {
          bestId = c.id;
          bestLen = bytes.length;
          break;
        }
      }
    }

    if (bestId >= 0 && bestLen >= 4) {
      payload.wB(255);
      payload.wVar(bestId + 1);
      hits++;
      saved += bestLen - 1 - lexVarLen(bestId + 1);
      i += bestLen;
    } else {
      if (b === 255) {
        payload.wB(255);
        payload.wVar(0);
      } else {
        payload.wB(b);
      }
      i++;
    }
  }

  if (hits < 8 || saved < 64) return null;

  const payloadBytes = payload.finish();
  const w = new BW();

  w.wA(enc.encode("LEX1"));
  w.wB(1);
  w.wU(data.length);
  w.wVar(dict.length);

  for (const item of dict) {
    w.wVar(item.length);
    w.wA(item);
  }

  w.wU(payloadBytes.length);
  w.wA(payloadBytes);

  const out = w.finish();

  if (out.length >= data.length - 32) return null;

  return {
    data: out,
    info: `dict=${dict.length} hits=${hits} saved≈${saved}`,
  };
}

function inverseLEX1(data: Uint8Array) {
  const r = new BR(data);

  if (dec.decode(r.rA(4)) !== "LEX1") throw new Error("bad LEX1 magic");

  const ver = r.rB();
  if (ver !== 1) throw new Error("bad LEX1 version");

  const originalLen = r.rU();
  const dictLen = r.rVar();
  const dict: Uint8Array[] = [];

  for (let i = 0; i < dictLen; i++) {
    const len = r.rVar();
    dict.push(r.rA(len));
  }

  const payloadLen = r.rU();
  const payload = r.rA(payloadLen);

  if (r.p !== data.length) throw new Error("LEX1 trailing bytes");

  const pr = new BR(payload);
  const out = new BW();

  while (pr.p < payload.length) {
    const b = pr.rB();

    if (b !== 255) {
      out.wB(b);
      continue;
    }

    const id = pr.rVar(dict.length);

    if (id === 0) {
      out.wB(255);
    } else {
      const item = dict[id - 1];
      if (!item) throw new Error("LEX1 bad dict id");
      out.wA(item);
    }
  }

  const decoded = out.finish();

  if (decoded.length !== originalLen) throw new Error("LEX1 size mismatch");

  return decoded;
}

function buildOuter(originalSize: number, hash: Uint8Array, transform: number, inner: Uint8Array) {
  const w = new BW();

  w.wA(MAGIC);
  w.wB(VERSION);
  w.wB(PHASE_NDC51);
  w.wU(originalSize);
  w.wA(hash);
  w.wB(transform);
  w.wU(inner.length);
  w.wA(inner);

  return w.finish();
}

function legacyDecodePhase2(f: Uint8Array, pAfterPhase: number) {
  const r = new BR(f);
  r.p = pAfterPhase;

  const originalSize = r.rU();
  const expectedHash = r.rA(32);
  const codec = r.rB();

  if (codec !== CORE_CODEC) throw new Error("unsupported legacy codec");

  const types = readStreamLegacy(r);
  const litLens = readStreamLegacy(r);
  const matchLens = readStreamLegacy(r);
  const distBuckets = readStreamLegacy(r);

  const extrasMode = r.rB();

  if (extrasMode !== 0) throw new Error("unsupported legacy extras mode");

  const distCount = r.rU();
  const totalBits = r.rU();
  const extrasLen = r.rU();
  const extras = r.rA(extrasLen);

  const runVals = readStreamLegacy(r);
  const runLensBytes = readStreamLegacy(r);

  const litMode = r.rB();

  let literalsByContext: Uint8Array[];

  if (litMode === 0) {
    literalsByContext = [readStreamLegacy(r)];
  } else if (litMode === 1) {
    literalsByContext = [readStreamLegacy(r), readStreamLegacy(r), readStreamLegacy(r), readStreamLegacy(r)];
  } else if (litMode === 2) {
    literalsByContext = Array.from({ length: 16 }, () => readStreamLegacy(r));
  } else {
    throw new Error("bad legacy literal mode");
  }

  const runLens = new Array(runLensBytes.length / 2);
  const dv = new DataView(runLensBytes.buffer, runLensBytes.byteOffset, runLensBytes.byteLength);

  for (let i = 0; i < runLens.length; i++) {
    runLens[i] = dv.getUint16(i * 2, false);
  }

  const bitReader = new BitReader(extras, totalBits);
  const dists: number[] = [];

  for (let i = 0; i < distCount; i++) {
    const b = distBuckets[i];
    const base = 1 << b;
    const v = bitReader.readBits(b);

    dists.push(base + v);
  }

  return {
    originalSize,
    expectedHash,
    types,
    litLens,
    matchLens,
    dists,
    runVals,
    runLens,
    litMode,
    literalsByContext,
  };
}

async function finishLegacyDecode(obj: ReturnType<typeof legacyDecodePhase2>) {
  const out = new Uint8Array(obj.originalSize);

  let op = 0;
  let mi = 0;
  let di = 0;
  let ri = 0;
  let litLenIdx = 0;
  const litPtrs = obj.literalsByContext.map(() => 0);

  for (let i = 0; i < obj.types.length; i++) {
    const t = obj.types[i];

    if (t === 0) {
      const len = obj.litLens[litLenIdx++];

      for (let k = 0; k < len; k++) {
        let ctx = 0;

        if (obj.litMode === 1) ctx = op > 0 ? ctx4(out[op - 1]) : 0;
        else if (obj.litMode === 2) ctx = op > 0 ? ctx16(out[op - 1]) : 0;

        out[op++] = obj.literalsByContext[ctx][litPtrs[ctx]++];
      }
    } else if (t === 1) {
      const len = obj.matchLens[mi++] + MINM;
      const dist = obj.dists[di++];
      const src = op - dist;

      if (src < 0 || op + len > obj.originalSize) throw new Error("legacy invalid match");

      for (let k = 0; k < len; k++) out[op++] = out[src + k];
    } else if (t === 2) {
      const value = obj.runVals[ri];
      const len = obj.runLens[ri++];

      if (op + len > obj.originalSize) throw new Error("legacy invalid run");

      out.fill(value, op, op + len);
      op += len;
    } else {
      throw new Error("legacy bad token type");
    }
  }

  if (op !== obj.originalSize) throw new Error("legacy decoded size mismatch");

  const got = await sha256(out);

  for (let i = 0; i < 32; i++) {
    if (got[i] !== obj.expectedHash[i]) throw new Error("legacy sha256 mismatch");
  }

  return out;
}

/**
 * Compresses data using the NDC-AB algorithm.
 * Performs a "tournament" between different transformations (LTP1, TLM2, LEX1, RAW)
 * and selects the most efficient one.
 * 
 * @param data The input bytes to compress.
 * @param log Optional callback for status messages.
 * @returns A promise resolving to the compressed file and its original hash.
 */
export async function compressNDC_AB(
  data: Uint8Array,
  log: (x: string) => void = () => { }
): Promise<{ file: Uint8Array; hash: Uint8Array }> {
  if (data.length > 0xffffffff) {
    throw new Error("NDC51 Uint8Array API supports up to 4 GiB; use compressNDCBlobChunked() for larger Blob/File inputs");
  }

  const hash = await sha256(data);

  const candidates: {
    name: string;
    transform: number;
    transformed: Uint8Array;
    info: string;
  }[] = [
      {
        name: "normal",
        transform: T_RAW,
        transformed: data,
        info: "",
      },
    ];

  const safeAddTransform = (name: string, transform: number, fn: () => { data: Uint8Array; info: string } | null) => {
    try {
      const res = fn();
      if (res) {
        candidates.push({
          name,
          transform,
          transformed: res.data,
          info: res.info,
        });
      }
    } catch (err) {
      log(`NDC51 skip ${name}: ${err instanceof Error ? err.message : String(err)}`);
    }
  };

  safeAddTransform("LTP1", T_LTP1, () => transformLTP1(data));

  // NDC48: no agregamos capas nuevas; hacemos torneo de minería TLM2.
  // Mismo transform y mismo decoder, pero con umbrales distintos.
  // Esto evita que una única heurística de plantillas decida todo el resultado.
  if (looksTextual(data)) {
    safeAddTransform("TLM2", T_TLM2, () => transformTLM2(data, { name: "std" }));

    if (data.length >= 512 * 1024) {
      safeAddTransform("TLM2-strict", T_TLM2, () => transformTLM2(data, {
        name: "strict",
        maxTemplates: 512,
        minCount: 3,
        minSaving: 96,
        maxSlots: 64,
      }));

      safeAddTransform("TLM2-wide", T_TLM2, () => transformTLM2(data, {
        name: "wide",
        maxTemplates: 1536,
        minCount: 2,
        minSaving: 12,
        maxSlots: 96,
      }));
    }
  }

  // NDC50: LEX1 es el único experimento activo nuevo.
  // No reemplaza TLM2: cubre código, SQL con lexemas largos y texto no tabular.
  if (looksTextual(data) || data.length >= 256 * 1024) {
    safeAddTransform("LEX1", T_LEX1, () => transformLEX1(data));
  }

  // BWT1/STT1 se conservan para leer archivos viejos, pero no se activan por defecto.
  // En tus pruebas reales no ganaron frente a DFRAW y solo añadían tiempo/ruido al selector.
  const ENABLE_SIDE_TEXT_EXPERIMENTS = false;
  if (ENABLE_SIDE_TEXT_EXPERIMENTS) {
    safeAddTransform("BWT1", T_BWT1, () => transformBWT1(data));
    safeAddTransform("STT1", T_STT1, () => transformSTT1(data));
  }

  type BuiltCandidate = {
    name: string;
    file: Uint8Array;
    inner: number;
    info: string;
    direct: boolean;
    valid?: boolean;
    err?: string;
  };

  const built: BuiltCandidate[] = [];
  const report: string[] = [];

  const buildCandidate = (name: string, transform: number, inner: Uint8Array, info: string) => {
    const file = buildOuter(data.length, hash, transform, inner);
    built.push({ name, file, inner: inner.length, info, direct: false });
    report.push(`${name}=${fmt(file.length)}`);
  };

  const buildDirect = (name: string, payload: Uint8Array, info: string) => {
    built.push({ name, file: payload, inner: payload.length, info, direct: true });
    report.push(`${name}=${fmt(payload.length)}`);
  };

  for (const c of candidates) {
    try {
      const encoded = coreEncodeBest(c.transformed, c.name);
      const info = c.info ? `${c.info} lz=${encoded.mode}` : `lz=${encoded.mode}`;
      buildCandidate(c.name, c.transform, encoded.inner, info);
    } catch (err) {
      report.push(`${c.name}=ERR`);
      log(`NDC51 encode skip ${c.name}: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  try {
    // BLC1 se mantiene para binarios, pero se evita en texto/log/SQL donde TLM2 ya es superior.
    if (!looksTextual(data)) {
      const blocked = encodeBlockedCore(data);
      if (blocked) {
        buildCandidate("BLC1", T_BLC1, blocked.data, blocked.info);
      }
    }
  } catch (err) {
    report.push("BLC1=ERR");
    log(`NDC51 encode skip BLC1: ${err instanceof Error ? err.message : String(err)}`);
  }

  try {
    const pako = await import("pako");
    const deflateRaw = (pako as any).deflateRaw as undefined | ((input: Uint8Array, opts?: any) => Uint8Array);

    if (deflateRaw) {
      const df = deflateRaw(data, { level: 9 });
      buildDirect("DFRAW", df, "standard-deflate-raw-floor");
    }
  } catch {
    // pako no disponible: se omite el piso DFRAW.
  }

  if (!built.length) throw new Error("NDC51: no compression candidates built");

  built.sort((a, b) => a.file.length - b.file.length);

  const validateCandidate = async (c: BuiltCandidate) => {
    try {
      const got = await decompressNDC(c.file);

      if (got.length !== data.length) {
        c.err = `size ${got.length} != ${data.length}`;
        return false;
      }

      // Los candidatos NDC20 ya validan SHA-256 dentro de decompressNDC().
      // Solo los directos, como DFRAW, necesitan comparación byte a byte aquí.
      if (c.direct) {
        for (let i = 0; i < data.length; i++) {
          if (got[i] !== data[i]) {
            c.err = `byte mismatch at ${i}`;
            return false;
          }
        }
      }

      c.valid = true;
      return true;
    } catch (err) {
      c.err = err instanceof Error ? err.message : String(err);
      return false;
    }
  };

  let best = built[0];
  const invalid: string[] = [];

  for (const c of built) {
    const ok = await validateCandidate(c);

    if (ok) {
      best = c;
      break;
    }

    invalid.push(`${c.name}:${c.err ?? "invalid"}`);
  }

  if (!best.valid) {
    throw new Error(`NDC51: all candidates failed validation: ${invalid.join(" | ")}`);
  }

  log(
    `NDC51-RICE-MARKOV ${fmt(data.length)} -> ${fmt(best.file.length)} | best=${best.name}${best.direct ? " direct" : ""} | inner=${fmt(best.inner)} | ${report.join(" | ")}${best.info ? " | " + best.info : ""}${invalid.length ? " | invalid=" + invalid.join(",") : ""}`
  );

  return {
    file: best.file,
    hash,
  };
}

/**
 * Decompresses an NDC-compressed byte array.
 * Automatically detects the version and phase of the compression algorithm.
 * Includes fallback for standard zlib/deflate streams.
 * 
 * @param f The compressed byte array.
 * @returns A promise resolving to the decompressed bytes.
 * @throws Error if magic number is invalid or checksum fails.
 */
export async function decompressNDC(f: Uint8Array): Promise<Uint8Array> {
  if (f.length < 2) throw new Error("file too small");

  if (f.length < 5 || dec.decode(f.slice(0, 5)) !== "NDC20") {
    try {
      const pako = await import("pako");
      const inflateRaw = (pako as any).inflateRaw as undefined | ((input: Uint8Array) => Uint8Array);
      const inflate = (pako as any).inflate as undefined | ((input: Uint8Array) => Uint8Array);

      if (inflateRaw) {
        try {
          return inflateRaw(f);
        } catch {
          // intenta zlib wrapper abajo
        }
      }

      if (inflate) return inflate(f);
    } catch {
      // cae al error normal
    }

    throw new Error("bad magic");
  }

  let p = 5;

  const version = f[p++];

  if (version !== VERSION) throw new Error("unsupported version");

  const phase = f[p++];

  if (phase === PHASE_LEGACY) {
    const obj = legacyDecodePhase2(f, p);
    return finishLegacyDecode(obj);
  }

  if (phase !== PHASE_NDC37 && phase !== PHASE_NDC38 && phase !== PHASE_NDC39 && phase !== PHASE_NDC40 && phase !== PHASE_NDC42 && phase !== PHASE_NDC50 && phase !== PHASE_NDC51) {
    throw new Error("unsupported phase");
  }

  const r = new BR(f);
  r.p = p;

  const originalSize = r.rU();
  const expectedHash = r.rA(32);
  const transform = r.rB();
  const innerLen = r.rU();
  const inner = r.rA(innerLen);

  if (r.p !== f.length) throw new Error("outer trailing bytes");

  let out: Uint8Array;

  if (transform === T_BLC1) {
    out = decodeBlockedCore(inner);
  } else {
    const transformed = coreDecode(inner);

    if (transform === T_RAW) {
      out = transformed;
    } else if (transform === T_LTP1) {
      out = inverseLTP1(transformed);
    } else if (transform === T_TLM2) {
      out = inverseTLM2(transformed);
    } else if (transform === T_BWT1) {
      out = inverseBWT1(transformed);
    } else if (transform === T_STT1) {
      out = inverseSTT1(transformed);
    } else if (transform === T_LEX1) {
      out = inverseLEX1(transformed);
    } else {
      throw new Error("unknown transform");
    }
  }

  if (out.length !== originalSize) throw new Error("outer size mismatch");

  const actualHash = await sha256(out);

  for (let i = 0; i < 32; i++) {
    if (actualHash[i] !== expectedHash[i]) {
      throw new Error("sha256 mismatch");
    }
  }

  return out;
}



function writeU64(w: BW, n: bigint) {
  if (n < 0n || n > 0xffffffffffffffffn) throw new Error("invalid u64");

  for (let shift = 56n; shift >= 0n; shift -= 8n) {
    w.wB(Number((n >> shift) & 255n));
  }
}

function readU64(r: BR) {
  let n = 0n;

  for (let i = 0; i < 8; i++) {
    n = (n << 8n) | BigInt(r.rB());
  }

  return n;
}

/**
 * API opcional para archivos grandes tipo Blob/File.
 * Lee por partes y evita hacer file.arrayBuffer() completo.
 * Formato contenedor: NDCB1, chunks independientes NDC42.
 * Nota: devuelve Uint8Array final; para archivos gigantes de verdad conviene escribir a stream.
 */
export async function compressNDCBlobChunked(
  blob: Blob,
  log: (x: string) => void = () => { },
  chunkSize = 16 * 1024 * 1024
): Promise<{ file: Uint8Array; hash: Uint8Array }> {
  if (chunkSize <= 0 || chunkSize > 0xffffffff) throw new Error("bad chunk size");

  const chunks: { raw: number; packed: Uint8Array }[] = [];
  const chunkCount = Math.ceil(blob.size / chunkSize);

  // WebCrypto no tiene SHA-256 incremental nativo. Para mantener exactitud sin cargar
  // todo el archivo, este hash externo queda en cero; cada chunk conserva su SHA interno.
  const outerHash = new Uint8Array(32);

  for (let i = 0; i < chunkCount; i++) {
    const start = i * chunkSize;
    const end = Math.min(blob.size, start + chunkSize);
    const buf = new Uint8Array(await blob.slice(start, end).arrayBuffer());
    const packed = (await compressNDC_AB(buf, x => log(`chunk ${i + 1}/${chunkCount}: ${x}`))).file;

    chunks.push({ raw: buf.length, packed });
    log(`NDC42 chunked ${i + 1}/${chunkCount} raw=${fmt(buf.length)} packed=${fmt(packed.length)}`);
  }

  const w = new BW();
  w.wA(enc.encode("NDCB1"));
  w.wB(1);
  writeU64(w, BigInt(blob.size));
  w.wU(chunkSize);
  w.wU(chunks.length);

  for (const c of chunks) {
    w.wU(c.raw);
    w.wU(c.packed.length);
    w.wA(c.packed);
  }

  return { file: w.finish(), hash: outerHash };
}

export async function decompressNDCBlobChunked(file: Uint8Array): Promise<Uint8Array> {
  const r = new BR(file);

  if (dec.decode(r.rA(5)) !== "NDCB1") throw new Error("bad NDCB1 magic");

  const ver = r.rB();
  if (ver !== 1) throw new Error("bad NDCB1 version");

  const totalSize = readU64(r);
  const chunkSize = r.rU();
  const chunkCount = r.rU();
  const parts: Uint8Array[] = [];
  let total = 0n;

  for (let i = 0; i < chunkCount; i++) {
    const raw = r.rU();
    const packedLen = r.rU();
    const packed = r.rA(packedLen);
    const out = await decompressNDC(packed);

    if (out.length !== raw) throw new Error("bad chunk raw size");
    if (raw > chunkSize && i + 1 < chunkCount) throw new Error("bad chunk size");

    total += BigInt(out.length);
    parts.push(out);
  }

  if (total !== totalSize) throw new Error("bad chunked total size");
  if (r.p !== file.length) throw new Error("NDCB1 trailing bytes");

  return concat(parts);
}

export const compressNDC = compressNDC_AB;

export async function deflateEnc(d: Uint8Array): Promise<Uint8Array> {
  const { deflate } = await import("pako");
  return deflate(d, { level: 9 });
}

export async function selfTestNDC51RiceMarkov() {
  const cases: Uint8Array[] = [];

  cases.push(new Uint8Array());
  cases.push(enc.encode("hola hola hola hola hola hola hola hola\n"));
  cases.push(enc.encode("GET /api/v1/users 200\nGET /api/v1/users 200\nGET /api/v1/users 200\n"));

  const syntheticLog = Array.from({ length: 3000 }, (_, i) => {
    const sec = String(i % 60).padStart(2, "0");
    const ms = String((i * 137) % 1000).padStart(3, "0");
    const latency = 40 + (i % 70);
    const status = i % 17 === 0 ? 400 : 200;
    const id = String(100000 + i);
    return `{"@timestamp":"2026-04-23T16:19:${sec}.${ms}","level":"ERROR","message":"Request to: /api/v1/security-filters/reports-charges-pricing/by-client","transactionId":"${id}","status":${status},"time":${latency}}\n`;
  }).join("");

  cases.push(enc.encode(syntheticLog));

  const patterned = new Uint8Array(4096);
  for (let i = 0; i < patterned.length; i++) {
    patterned[i] = i % 2 === 0 ? 7 : 65 + (i % 26);
  }
  cases.push(patterned);

  const runs = new Uint8Array(4096);
  runs.fill(0);
  runs.fill(255, 1000, 2000);
  runs.fill(13, 3000, 4096);
  cases.push(runs);

  const random = new Uint8Array(4096);
  crypto.getRandomValues(random);
  cases.push(random);

  for (let i = 0; i < cases.length; i++) {
    const src = cases[i];
    const { file } = await compressNDC(src);
    const got = await decompressNDC(file);

    if (got.length !== src.length) {
      throw new Error(`selfTest case ${i} length mismatch`);
    }

    for (let j = 0; j < src.length; j++) {
      if (src[j] !== got[j]) {
        throw new Error(`selfTest case ${i} byte mismatch at ${j}`);
      }
    }
  }

  return true;
}

// Alias para no romper UI anterior.
export const selfTestNDC39BIN = selfTestNDC51RiceMarkov;
export const selfTestNDC40Hybrid = selfTestNDC51RiceMarkov;
export const selfTestNDC38TLM3 = selfTestNDC51RiceMarkov;
export const selfTestNDC37TLM2 = selfTestNDC51RiceMarkov;
export const selfTestNDC35LTP = selfTestNDC51RiceMarkov;
export const selfTestNDC36BMT = selfTestNDC51RiceMarkov;
export const selfTestNDC30 = selfTestNDC51RiceMarkov;

export const selfTestNDC42Hybrid = selfTestNDC51RiceMarkov;

export const selfTestNDC47StableGold = selfTestNDC51RiceMarkov;

export const selfTestNDC48RefactoredGold = selfTestNDC51RiceMarkov;

export const selfTestNDC50LexDictLab = selfTestNDC51RiceMarkov;
