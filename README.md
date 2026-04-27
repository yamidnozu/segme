<p align="center">
  <img src="./src/assets/banner.png" alt="SEGME Logo" width="600">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/TypeScript-5.0+-blue?style=for-the-badge&logo=typescript" alt="TypeScript">
  <img src="https://img.shields.io/badge/Vite-6.0+-646CFF?style=for-the-badge&logo=vite" alt="Vite">
  <img src="https://img.shields.io/badge/React-19.0-61DAFB?style=for-the-badge&logo=react" alt="React">
  <img src="https://img.shields.io/badge/License-MIT-green?style=for-the-badge" alt="License">
  <img src="https://img.shields.io/badge/Status-Experimental-orange?style=for-the-badge" alt="Status">
</p>

# SEGME: Next-Gen Data Compression (NDC)

NDC is an experimental lossless compression engine focused on structured logs, SQL dumps, text-like data, and mixed binary workloads.

The project explores a hybrid compression architecture that combines dynamic template mining, columnar variable encoding, LZ-style matching, entropy coding, binary transforms, and safe fallback compression. The goal is not to claim universal superiority over mature codecs, but to investigate where domain-aware reversible transforms can outperform generic compressors such as Deflate.

NDC is currently research-oriented and browser-friendly. It is written in TypeScript and can run directly in modern web environments, including Web Workers.

## Why NDC exists

Most general-purpose compressors operate mostly at the byte level. That works well for arbitrary files, but it misses structure that is obvious in real-world logs and data dumps:

```txt
timestamp level service route status latency trace_id
timestamp level service route status latency trace_id
timestamp level service route status latency trace_id
```

NDC tries to exploit that structure before applying byte-level compression.

Instead of only compressing a stream of bytes, NDC can transform the input into:

```txt
templates
line structure
columns of variables
numeric deltas
dictionary-coded values
residual raw data
```

Then each part is compressed using the most appropriate internal strategy.

## Current status

NDC is experimental.

It is suitable for:

- Research into lossless compression
- Log compression experiments
- Browser-based compression demos
- SQL dump compression tests
- Compression benchmarking
- Educational exploration of LZ, entropy coding, and structural transforms

It is not yet intended as a drop-in replacement for production storage systems.

## Main ideas

NDC combines several reversible techniques:

- Dynamic template mining for structured logs
- Columnar encoding of extracted variables
- Numeric delta and delta-of-delta encoding
- Golomb-Rice encoding for Laplace-like numeric deltas
- Markov-style template repetition coding
- LZ matching with hash-mult tables and multiple parsing modes
- Canonical Huffman coding
- rANS-style entropy coding for selected streams
- RLE and fill-stream optimization
- Binary-local transforms for non-text data
- DeflateRaw fallback for general-purpose safety
- Round-trip validation before selecting a compressed candidate
- Web Worker support for non-blocking browser execution

## Mathematical model

NDC treats compression as a constrained optimization problem over a set of reversible candidate transforms.

Given an input byte sequence:

$$
x = (x_1, x_2, \ldots, x_n), \qquad x_i \in \{0,\ldots,255\}
$$

the compressor evaluates a set of reversible candidates:

$$
\mathcal{C} = \{c_1,c_2,\ldots,c_m\}
$$

and selects the smallest valid candidate:

$$
c^* = \arg\min_{c \in \mathcal{C}} |E_c(x)|
$$

subject to exact reconstruction:

$$
D_c(E_c(x)) = x
$$

where:

- $E_c$ is the encoder for candidate $c$
- $D_c$ is its decoder
- $|E_c(x)|$ is the compressed size in bytes or bits

This means every transform is treated as a hypothesis. A transform is only useful if it reduces description length and preserves exact reconstruction.

### Entropy target

For a discrete symbol source $X$, the theoretical lower bound is the Shannon entropy:

$$
H(X) = -\sum_{s \in \Sigma} p(s)\log_2 p(s)
$$

A practical entropy coder tries to approach:

$$
L(X) \approx nH(X)
$$

where $L(X)$ is the encoded length.

NDC does not assume that the input is independent and identically distributed. Logs and SQL dumps are highly structured, so NDC first tries to expose lower-entropy streams before applying entropy coding.

### Compression ratio

The compression ratio is reported as:

$$
R = \frac{|C(x)|}{|x|}
$$

and the space saving is:

$$
S = 1 - R
$$

For example, if a file of $2.97$ MB is compressed to $306.11$ KB:

$$
R \approx \frac{306.11}{2.97 \cdot 1024} \approx 0.1007
$$

$$
S \approx 1 - 0.1007 = 0.8993
$$

which corresponds to approximately $89.93\%$ reduction.

### Dynamic template mining

NDC models structured text as repeated templates plus variables.

A line can be represented as:

$$
\ell_i = \tau_k(v_{i,1}, v_{i,2}, \ldots, v_{i,r_k})
$$

where:

- $\ell_i$ is a line
- $\tau_k$ is a discovered template
- $v_{i,j}$ is the $j$-th extracted variable
- $r_k$ is the number of variables in template $k$

A template is useful when its description length is smaller than storing all matching lines raw.

A simplified Minimum Description Length score is:

$$
G(\tau) =
\sum_{\ell_i \in M_\tau} |\ell_i|
-
\left(
|\tau| + \sum_{\ell_i \in M_\tau}\sum_j |v_{i,j}| + \Omega_\tau
\right)
$$

where:

- $M_\tau$ is the set of lines matching template $\tau$
- $|\tau|$ is the template metadata size
- $\Omega_\tau$ is overhead for IDs, counters, and stream metadata

NDC keeps a template only when:

$$
G(\tau) > 0
$$

This is why NDC does not rely on hardcoded tokens. It discovers templates dynamically from the input.

### Columnar variable model

Once templates are selected, the variables become a matrix:

$$
V^{(k)} =
\begin{bmatrix}
v_{1,1} & v_{1,2} & \cdots & v_{1,r_k} \\
v_{2,1} & v_{2,2} & \cdots & v_{2,r_k} \\
\vdots  & \vdots  & \ddots & \vdots \\
v_{m,1} & v_{m,2} & \cdots & v_{m,r_k}
\end{bmatrix}
$$

Each column is compressed independently:

$$
V^{(k)}_{*,j} = (v_{1,j}, v_{2,j}, \ldots, v_{m,j})
$$

This is important because a column often has a simpler distribution than the original text:

- status codes repeat
- timestamps grow slowly
- latencies have small deltas
- IDs may be monotonic
- routes and names become dictionary-friendly

### Numeric delta model

For numeric columns:

$$
a = (a_1,a_2,\ldots,a_n)
$$

NDC can encode first-order deltas:

$$
\Delta_i = a_i - a_{i-1}
$$

and second-order deltas:

$$
\Delta^2_i = \Delta_i - \Delta_{i-1}
$$

If the sequence is almost linear, $\Delta^2_i$ tends to be small, which makes it cheaper to encode.

### ZigZag mapping

Signed deltas are mapped to unsigned integers using ZigZag encoding:

$$
Z(d)=
\begin{cases}
2d, & d \ge 0 \\
-2d - 1, & d < 0
\end{cases}
$$

This maps small negative and positive numbers to small unsigned values:

```txt
 0 -> 0
-1 -> 1
 1 -> 2
-2 -> 3
 2 -> 4
```

### Golomb-Rice coding

When deltas follow a Laplace-like distribution, many values are near zero. Golomb-Rice coding is efficient for this case.

For an unsigned value $z$ and parameter $k$:

$$
q = \left\lfloor \frac{z}{2^k} \right\rfloor
$$

$$
r = z \bmod 2^k
$$

The encoded length is approximately:

$$
L_k(z) = q + 1 + k
$$

NDC estimates $k$ from the average absolute delta:

$$
k \approx \max\left(0, \left\lfloor \log_2(\mathbb{E}[|Z(\Delta)|]) \right\rfloor\right)
$$

This is useful for columns such as timestamps, counters, latencies, ports, row IDs, and status-like numeric values.

### Markov-style template repetition

Template IDs are not independent. In logs, if template $t_i$ appears, the next line often uses the same template.

NDC uses a simple Markov-style repetition model:

$$
P(t_i = t_{i-1})
$$

When the current template equals the previous one, the encoder can emit a shorter repeat marker instead of a full template ID.

A simple coding idea is:

$$
t_i =
\begin{cases}
\text{repeat}, & t_i = t_{i-1} \\
\text{new id } t_i, & t_i \ne t_{i-1}
\end{cases}
$$

This reduces overhead in long runs of similar log lines or SQL records.

### LZ-style matching

For byte-level compression, NDC uses LZ-style references.

A repeated substring can be encoded as a match:

$$
m = (d,\ell)
$$

where:

- $d$ is the backward distance
- $\ell$ is the match length

Instead of storing:

$$
x_i,x_{i+1},\ldots,x_{i+\ell-1}
$$

the encoder stores:

$$
(d,\ell)
$$

The match is useful when:

$$
\operatorname{cost}(d,\ell) < \operatorname{cost}(x_i,\ldots,x_{i+\ell-1})
$$

NDC uses hash-based matching and multiple parsing modes to decide which matches are worth emitting.

### Canonical Huffman coding

For a symbol $s$ with probability $p(s)$, an ideal code length is:

$$
\ell(s) \approx -\log_2 p(s)
$$

Huffman coding assigns integer code lengths that minimize expected length under prefix-code constraints:

$$
\mathbb{E}[L] = \sum_{s \in \Sigma} p(s)\ell(s)
$$

NDC uses canonical Huffman codes for compact and deterministic representation.

### rANS entropy coding

NDC also uses rANS-style entropy coding for selected streams.

At a high level, ANS maintains an integer state $x$ and encodes symbols by transforming the state according to a probability model:

$$
x' = C(s,x)
$$

and decodes with the inverse operation:

$$
(s,x) = C^{-1}(x')
$$

The practical benefit is that rANS can approach arithmetic-coding efficiency while remaining fast and table-driven.

### Candidate validation

NDC never trusts a candidate only because it is smaller.

Each candidate must satisfy:

$$
\operatorname{SHA256}(D(E(x))) = \operatorname{SHA256}(x)
$$

and:

$$
D(E(x)) = x
$$

Only then can it be selected as the final output.

## Architecture

NDC uses an adaptive candidate pipeline.

For each input, the compressor evaluates several reversible paths and selects the smallest valid output.

```txt
input
  |
  |-- raw byte-oriented core
  |-- log/template transform
  |-- columnar variable transform
  |-- binary-local transform
  |-- DeflateRaw fallback
  |
validated candidates
  |
smallest round-trip-safe result
```

The most important transform is the template-line model:

```txt
original lines
  |
template mining
  |
template ids + raw lines + variable columns
  |
per-column encoding
  |
LZ + entropy coding
```

This allows NDC to compress repeated log structures more efficiently than treating the file as plain text.

## Example results

The following results are from local benchmark runs. They should be reproduced on your own machine before drawing conclusions.

| File | Original | NDC | Deflate level 9 | Result |
|---|---:|---:|---:|---|
| `test_logs.txt` | 2.97 MB | 306.11 KB | 443.16 KB | NDC smaller |
| `asistapp_prod_backup_20260425_061325.sql` | 16.73 MB | 4.08 MB | 5.35 MB | NDC smaller |
| `Codex Installer.exe` | 1.12 MB | 446.74 KB | 494.50 KB | NDC smaller |
| `WinDirStat-x64.msi` | 2.37 MB | 1.58 MB | 1.58 MB | roughly tied |
| TypeScript source file | 71.27 KB | 16.74 KB | 16.75 KB | roughly tied |

These results show the current strength of NDC: structured logs and text-like data with repeated schema. For already-compressed binaries or installer formats, NDC falls back to general compression behavior.

## Compression model

At a high level, NDC uses this flow:

```txt
1. Analyze the input
2. Detect whether it looks textual or binary
3. Try structural transforms when appropriate
4. Compress each candidate
5. Decompress each candidate internally
6. Verify exact byte-for-byte round trip
7. Select the smallest valid result
```

This validation step is important. NDC is designed to avoid returning a compressed file that cannot reconstruct the original data.

## Web Worker support

NDC includes a Web Worker interface for running compression without blocking the UI thread.

Example worker entry:

```ts
import { compressNDC, decompressNDC, compressNDCBlobChunked } from './ndc_core';

self.onmessage = async event => {
  const msg = event.data;

  if (msg.op === 'compress') {
    const result = await compressNDC(msg.data);
    postMessage({ id: msg.id, type: 'result', file: result.file, hash: result.hash }, [
      result.file.buffer,
      result.hash.buffer,
    ]);
  }

  if (msg.op === 'decompress') {
    const data = await decompressNDC(msg.data);
    postMessage({ id: msg.id, type: 'result', data }, [data.buffer]);
  }

  if (msg.op === 'compressBlob') {
    const result = await compressNDCBlobChunked(msg.blob, undefined, msg.chunkSize);
    postMessage({ id: msg.id, type: 'result', file: result.file, hash: result.hash }, [
      result.file.buffer,
      result.hash.buffer,
    ]);
  }
};
```

## Basic usage

```ts
import { compressNDC, decompressNDC } from './ndc_core';

const input = new TextEncoder().encode('hello hello hello');

const compressed = await compressNDC(input);
const restored = await decompressNDC(compressed.file);

console.log(compressed.file.length);
console.log(restored);
```

## Blob/chunked usage

For larger files in the browser:

```ts
import { compressNDCBlobChunked } from './ndc_core';

const result = await compressNDCBlobChunked(file, message => {
  console.log(message);
}, 16 * 1024 * 1024);

console.log(result.file);
console.log(result.hash);
```

## Benchmarking

To compare against Deflate:

```ts
import { compressNDC, deflateEnc } from './ndc_core';

const ndc = await compressNDC(data);
const deflate = await deflateEnc(data);

console.table({
  original: data.length,
  ndc: ndc.file.length,
  deflate: deflate.length,
});
```

For meaningful results, benchmark across multiple categories:

- structured logs
- JSON logs
- SQL dumps
- TypeScript or JavaScript source
- CSV files
- binary installers
- already-compressed archives
- random/high-entropy data

## Strengths

NDC performs best when the input has structure that can be discovered dynamically:

- repeated log templates
- repeated SQL tuple shapes
- stable JSON keys
- repeated routes or service names
- numeric columns with small deltas
- status codes, counters, latencies, timestamps
- semi-structured text with recurring fields

## Limitations

NDC is not expected to outperform mature compressors on every file.

It may not provide large gains for:

- encrypted data
- random data
- already-compressed archives
- media files
- some installers
- formats that contain compressed internal streams

In these cases, NDC relies on fallback strategies to avoid large regressions.

## Design principles

NDC follows these principles:

1. Lossless only  
   The decompressed output must match the original input exactly.

2. Dynamic discovery  
   No hardcoded log tokens or fixed dictionaries are required.

3. Adaptive selection  
   Transforms are candidates, not assumptions.

4. Round-trip validation  
   A candidate must successfully decompress before being selected.

5. Browser compatibility  
   The implementation targets modern TypeScript and Web APIs.

6. Research transparency  
   Compression ratios should be reproducible and benchmarked honestly.

## Roadmap

Planned research directions:

- Context Tree Weighting for residual literal streams
- Improved Markov coding for template sequences
- Better column dependency modeling
- More efficient Golomb-Rice modeling for numeric deltas
- Optional trained dictionaries for repeated operational logs
- Faster worker-based compression for large files
- Streaming decompression
- Corpus-based benchmark suite
- CLI runner for reproducible compression tests
- WASM acceleration for entropy coding

## Repository goals

This project is intended to become a serious open-source compression research playground.

The target audience includes:

- compression researchers
- observability engineers
- log platform builders
- browser tooling developers
- database backup tooling authors
- systems programmers
- students studying compression algorithms

The long-term goal is to explore whether domain-aware, dynamically discovered structure can make lossless compression more effective for modern machine-generated data.

## Contributing

Contributions are welcome.

Useful contributions include:

- benchmark results on real datasets
- new reversible transforms
- decompression safety tests
- performance profiling
- worker and streaming improvements
- documentation
- corpus design
- comparisons against gzip, zstd, brotli, lz4, xz, and bzip2

Please include:

- input file type
- original size
- NDC compressed size
- baseline compressor and level
- compression time
- decompression time
- whether round-trip verification passed

## Safety and correctness

Every compressed candidate should be considered invalid unless it passes round-trip verification.

The expected invariant is:

```txt
decompress(compress(input)) === input
```

Mathematically:

$$
D(E(x)) = x
$$

NDC uses SHA-256 verification to protect against silent corruption:

$$
\operatorname{SHA256}(D(E(x))) = \operatorname{SHA256}(x)
$$

## License

Choose a license before publishing.

Recommended options:

- MIT for maximum adoption
- Apache-2.0 for patent protection
- MPL-2.0 if you want modifications to core files to remain open

---

<p align="center">
  Developed by <b>eDev Core SAS</b>
</p>

## Short description

Dynamic lossless compression engine for structured logs, SQL dumps, and mixed data, using template mining, columnar transforms, LZ matching, entropy coding, and validated adaptive fallback.
