import { useMemo, useRef, useState } from "react";
import { Binary, CheckCircle2, Download, FlaskConical, Loader2, Upload, Zap } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { compressNDC_AB, decompressNDC, deflateEnc, sha256, fmt } from "./lib/ndc_core";

function hex(b: Uint8Array) { return [...b].map(x => x.toString(16).padStart(2,"0")).join("") }
function save(bytes: Uint8Array, name: string) {
  const buf = bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength) as ArrayBuffer;
  const url = URL.createObjectURL(new Blob([buf]));
  const a = document.createElement("a"); a.href = url; a.download = name; a.click(); URL.revokeObjectURL(url);
}

type Sizes = { ndc: number | null; zlib: number | null };
type TestRow = { name: string; ok: boolean; orig: number; ndc: number; zlib: number; winner: string };

function pct(size: number | null, orig: number) {
  if (size == null || !orig) return "—";
  return ((1 - size / orig) * 100).toFixed(2) + "%";
}
function pickWinner(ndc: number, zlib: number): string {
  return ndc < zlib ? "NDC-AB 🏆" : zlib < ndc ? "zlib 🏆" : "Empate";
}

export default function App() {
  const inputRef = useRef<HTMLInputElement | null>(null);
  const [fileName, setFileName] = useState("");
  const [original, setOriginal] = useState<Uint8Array | null>(null);
  const [hash, setHash] = useState<Uint8Array | null>(null);
  const [ndcFile, setNdcFile] = useState<Uint8Array | null>(null);
  const [sizes, setSizes] = useState<Sizes>({ ndc: null, zlib: null });
  const [tests, setTests] = useState<TestRow[] | null>(null);
  const [log, setLog] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState("Listo — NDC20-AB Stable v2.1");

  const addLog = (x: string) => setLog(l => [...l.slice(-200), `${new Date().toLocaleTimeString()} ${x}`]);

  const showdown = useMemo(() => {
    if (sizes.ndc == null || sizes.zlib == null) return null;
    return pickWinner(sizes.ndc, sizes.zlib);
  }, [sizes]);

  const orig = original?.length ?? 0;

  async function load(e: React.ChangeEvent<HTMLInputElement>) {
    const f = e.target.files?.[0]; if (!f) return;
    const data = new Uint8Array(await f.arrayBuffer());
    setFileName(f.name); setOriginal(data); setHash(await sha256(data));
    setSizes({ ndc: null, zlib: null }); setNdcFile(null); setTests(null);
    setMsg("Archivo cargado"); setLog([]);
  }

  async function run() {
    if (!original) return;
    setBusy(true); setLog([]); setMsg("Comprimiendo NDC-AB + zlib…");
    setSizes({ ndc: null, zlib: null });
    try {
      const [r, z] = await Promise.all([
        compressNDC_AB(original, addLog),
        deflateEnc(original),
      ]);
      const restored = await decompressNDC(r.file);
      const ok = restored.length === original.length && restored.every((v, i) => v === original[i]);
      setSizes({ ndc: r.file.length, zlib: z.length });
      setNdcFile(r.file);
      setMsg(ok ? "✅ Round-trip verificado" : "❌ Error en round-trip");
    } catch (e) {
      setMsg(`❌ Error: ${e instanceof Error ? e.message : String(e)}`);
    }
    setBusy(false);
  }

  async function runTests() {
    setBusy(true); setLog([]); setMsg("Auto-pruebas…");
    try {
      const enc = new TextEncoder();
      const eqArr = (a: Uint8Array, b: Uint8Array) => a.length === b.length && a.every((v, i) => v === b[i]);
      const cases: [string, Uint8Array][] = [
        ["RLE bytes",        new Uint8Array(512).fill(65)],
        ["Número 10^50",     enc.encode("1" + "0".repeat(50))],
        ["REP decimal",      enc.encode("123".repeat(200))],
        ["Patrón binario",   new Uint8Array(Array.from({length:1024},(_,i)=>[0xde,0xad,0xbe,0xef][i%4]))],
        ["SQL repetido",     enc.encode("INSERT INTO t VALUES(1);\n".repeat(200))],
        ["Random",           new Uint8Array(Array.from({length:512},()=>Math.random()*256|0))],
        ["Empty",            new Uint8Array([])],
      ];
      const rows: TestRow[] = [];
      for (const [name, data] of cases) {
        addLog(`Test: ${name}`);
        const [r, z] = await Promise.all([compressNDC_AB(data, addLog), deflateEnc(data)]);
        const d = await decompressNDC(r.file);
        rows.push({ name, ok: eqArr(data, d), orig: data.length, ndc: r.file.length, zlib: z.length, winner: pickWinner(r.file.length, z.length) });
      }
      setTests(rows); setMsg("Auto-pruebas terminadas");
    } catch (e) {
      setMsg(`❌ Error: ${e instanceof Error ? e.message : String(e)}`);
    }
    setBusy(false);
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 to-slate-100 p-4 md:p-8 text-slate-900">
      <div className="max-w-5xl mx-auto space-y-6">

        {/* Header */}
        <div className="flex flex-col md:flex-row md:items-end md:justify-between gap-4">
          <div>
            <div className="inline-flex items-center gap-2 rounded-full bg-slate-900 text-white px-3 py-1 text-sm mb-3">
              <Binary className="h-4 w-4" /> NDC20-AB Stable v2.1
            </div>
            <h1 className="text-3xl md:text-4xl font-bold">Compresor experimental</h1>
            <p className="text-slate-600 mt-2">LZ (hash mult. × 8 slots × 4 MB) + Huffman canónico order-0 — vs Deflate/zlib nivel 9</p>
          </div>
          <span className="inline-flex items-center gap-2 text-sm text-slate-600">
            <CheckCircle2 className="h-4 w-4 text-emerald-600" /> {msg}
          </span>
        </div>

        {/* Controls */}
        <Card className="rounded-3xl border-slate-200 bg-white/90">
          <CardContent className="p-5 space-y-4">
            <div className="flex flex-wrap gap-2">
              <input ref={inputRef} type="file" onChange={load} className="hidden" />
              <Button className="rounded-xl" onClick={() => inputRef.current?.click()} disabled={busy}>
                <Upload className="h-4 w-4 mr-2" /> Cargar archivo
              </Button>
              <Button className="rounded-xl" onClick={run} disabled={!original || busy}>
                {busy ? <Loader2 className="h-4 w-4 mr-2 animate-spin" /> : <Zap className="h-4 w-4 mr-2" />}
                Comprimir NDC-AB vs zlib
              </Button>
              <Button variant="outline" className="rounded-xl" onClick={runTests} disabled={busy}>
                <FlaskConical className="h-4 w-4 mr-2" /> Auto-pruebas
              </Button>
              <Button variant="outline" className="rounded-xl" onClick={() => ndcFile && save(ndcFile, `${fileName||"archivo"}.ndc20`)} disabled={!ndcFile}>
                <Download className="h-4 w-4 mr-2" /> Descargar .ndc20
              </Button>
            </div>
            {fileName && (
              <p className="text-sm text-slate-600">
                <b>Archivo:</b> {fileName} · <b>Original:</b> {fmt(orig)}{" "}
                {hash && <> · <b>SHA-256:</b> <span className="break-all text-xs font-mono">{hex(hash)}</span></>}
              </p>
            )}
          </CardContent>
        </Card>

        {/* Stats */}
        {orig > 0 && (sizes.ndc != null || sizes.zlib != null) && (
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* Original */}
            <Card className="rounded-2xl border-slate-200">
              <CardContent className="p-4">
                <p className="text-xs text-slate-500 mb-1">Original</p>
                <p className="text-2xl font-bold">{fmt(orig)}</p>
                <p className="text-xs text-slate-400 mt-1">Sin compresión</p>
              </CardContent>
            </Card>
            {/* NDC-AB */}
            <Card className={`rounded-2xl border-slate-200 ${showdown === "NDC-AB 🏆" ? "ring-2 ring-emerald-400 bg-emerald-50" : ""}`}>
              <CardContent className="p-4">
                <p className="text-xs text-slate-500 mb-1">NDC-AB (LZ + Huffman)</p>
                <p className="text-2xl font-bold">{sizes.ndc != null ? fmt(sizes.ndc) : "—"}</p>
                <p className="text-xs text-slate-400 mt-1">Ahorro {pct(sizes.ndc, orig)}</p>
                {showdown === "NDC-AB 🏆" && <p className="text-xs font-semibold text-emerald-600 mt-1">🏆 Ganador</p>}
              </CardContent>
            </Card>
            {/* zlib */}
            <Card className={`rounded-2xl border-slate-200 ${showdown === "zlib 🏆" ? "ring-2 ring-amber-400 bg-amber-50" : ""}`}>
              <CardContent className="p-4">
                <p className="text-xs text-slate-500 mb-1">Deflate/zlib (nivel 9)</p>
                <p className="text-2xl font-bold">{sizes.zlib != null ? fmt(sizes.zlib) : "—"}</p>
                <p className="text-xs text-slate-400 mt-1">Ahorro {pct(sizes.zlib, orig)}</p>
                {showdown === "zlib 🏆" && <p className="text-xs font-semibold text-amber-600 mt-1">🏆 Ganador</p>}
              </CardContent>
            </Card>
          </div>
        )}

        {/* Comparison table */}
        {sizes.ndc != null && sizes.zlib != null && (
          <Card className="rounded-3xl border-slate-200 bg-white/90">
            <CardContent className="p-5">
              <h2 className="text-lg font-semibold mb-3">Comparación directa vs Deflate/zlib</h2>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="bg-slate-100">
                    <tr>
                      <th className="text-left p-2">Codec</th>
                      <th className="text-right p-2">Tamaño</th>
                      <th className="text-right p-2">Ahorro</th>
                      <th className="text-right p-2">vs zlib</th>
                    </tr>
                  </thead>
                  <tbody>
                    {[
                      { label: "NDC-AB (LZ + Huffman)", size: sizes.ndc, key: "ndc" },
                      { label: "Deflate/zlib (nivel 9)", size: sizes.zlib, key: "zlib" },
                    ].map(({ label, size, key }) => {
                      const diff = sizes.ndc! - sizes.zlib!;
                      const isWin = showdown?.startsWith(key === "ndc" ? "NDC" : "zlib");
                      return (
                        <tr key={key} className={`border-t ${isWin ? "bg-emerald-50 font-semibold" : ""}`}>
                          <td className="p-2">{label} {isWin ? "🏆" : ""}</td>
                          <td className="p-2 text-right">{fmt(size)}</td>
                          <td className="p-2 text-right">{pct(size, orig)}</td>
                          <td className={`p-2 text-right ${key==="ndc" ? (diff<0?"text-emerald-600":diff>0?"text-red-500":""):""}`}>
                            {key === "zlib" ? "referencia" : diff < 0 ? `−${fmt(-diff)} mejor` : diff > 0 ? `+${fmt(diff)} peor` : "igual"}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Auto-tests */}
        {tests && (
          <Card className="rounded-3xl border-slate-200 bg-white/90">
            <CardContent className="p-5">
              <h2 className="text-lg font-semibold mb-3">Auto-pruebas</h2>
              <div className="overflow-x-auto">
                <table className="w-full text-sm border border-slate-200">
                  <thead className="bg-slate-100">
                    <tr>
                      <th className="text-left p-2">Caso</th>
                      <th className="text-center p-2">OK</th>
                      <th className="text-right p-2">Original</th>
                      <th className="text-right p-2">NDC-AB</th>
                      <th className="text-right p-2">zlib</th>
                      <th className="text-center p-2">Ganador</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tests.map(t => (
                      <tr key={t.name} className="border-t hover:bg-slate-50">
                        <td className="p-2">{t.name}</td>
                        <td className="p-2 text-center">{t.ok ? "✅" : "❌"}</td>
                        <td className="p-2 text-right">{fmt(t.orig)}</td>
                        <td className={`p-2 text-right ${t.winner.startsWith("NDC") ? "text-emerald-700 font-semibold" : ""}`}>{fmt(t.ndc)}</td>
                        <td className={`p-2 text-right ${t.winner.startsWith("zlib") ? "text-amber-700 font-semibold" : ""}`}>{fmt(t.zlib)}</td>
                        <td className="p-2 text-center text-xs text-slate-500">{t.winner}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </CardContent>
          </Card>
        )}

        {/* Log */}
        <Card className="rounded-3xl border-slate-200 bg-slate-950 text-green-400">
          <CardContent className="p-4">
            <h2 className="text-sm font-semibold mb-2 text-green-300">Log</h2>
            <pre className="text-xs whitespace-pre-wrap min-h-24 max-h-64 overflow-auto">{log.join("\n")}</pre>
          </CardContent>
        </Card>

      </div>
    </div>
  );
}
