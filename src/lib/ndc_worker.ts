/**
 * @file ndc_worker.ts
 * @description Web Worker implementation for NDC compression.
 * Allows running computationally intensive compression tasks in a background thread
 * to maintain UI responsiveness.
 */

import { compressNDC, decompressNDC, compressNDCBlobChunked } from './ndc_core';

type CompressMsg = {
  id: string;
  op: 'compress';
  data: Uint8Array;
};

type DecompressMsg = {
  id: string;
  op: 'decompress';
  data: Uint8Array;
};

type CompressBlobMsg = {
  id: string;
  op: 'compressBlob';
  blob: Blob;
  chunkSize?: number;
};

type Msg = CompressMsg | DecompressMsg | CompressBlobMsg;

const post = (payload: unknown, transfer?: Transferable[]) => {
  (self as unknown as Worker).postMessage(payload, transfer ?? []);
};

self.onmessage = async (ev: MessageEvent<Msg>) => {
  const msg = ev.data;

  try {
    if (msg.op === 'compress') {
      const res = await compressNDC(msg.data, x => post({ id: msg.id, type: 'log', message: x }));
      post({ id: msg.id, type: 'result', file: res.file, hash: res.hash }, [res.file.buffer, res.hash.buffer]);
      return;
    }

    if (msg.op === 'decompress') {
      const out = await decompressNDC(msg.data);
      post({ id: msg.id, type: 'result', data: out }, [out.buffer]);
      return;
    }

    if (msg.op === 'compressBlob') {
      const res = await compressNDCBlobChunked(
        msg.blob,
        x => post({ id: msg.id, type: 'log', message: x }),
        msg.chunkSize
      );
      post({ id: msg.id, type: 'result', file: res.file, hash: res.hash }, [res.file.buffer, res.hash.buffer]);
      return;
    }

    throw new Error('unknown worker operation');
  } catch (err) {
    post({
      id: msg.id,
      type: 'error',
      error: err instanceof Error ? err.message : String(err),
    });
  }
};
