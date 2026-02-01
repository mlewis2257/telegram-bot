// raydiumSwapParser.js (ESM)
const WSOL_MINT = "So11111111111111111111111111111111111111112";

// Raydium often emits base64 `ray_log:` lines on swaps
export function isRaydiumSwapLogs(logs) {
  return logs.some((l) => l.includes("ray_log:"));
}

function extractTokenDeltas(parsedTx) {
  const pre = parsedTx?.meta?.preTokenBalances || [];
  const post = parsedTx?.meta?.postTokenBalances || [];

  // Map: accountIndex -> { mint, owner, uiAmount }
  const preMap = new Map();
  for (const b of pre) {
    preMap.set(b.accountIndex, {
      mint: b.mint,
      owner: b.owner,
      ui: b.uiTokenAmount?.uiAmount ?? null,
    });
  }

  const deltas = []; // [{ mint, owner, deltaUi }]
  for (const b of post) {
    const before = preMap.get(b.accountIndex);
    const mint = b.mint;
    const owner = b.owner;
    const afterUi = b.uiTokenAmount?.uiAmount ?? null;
    const beforeUi = before?.ui ?? 0;

    if (afterUi === null) continue;
    const delta = afterUi - beforeUi;
    if (!delta) continue;

    deltas.push({ mint, owner, deltaUi: delta });
  }

  // Remove wSOL if you want the other token(s)
  const filtered = deltas.filter((d) => d.mint !== WSOL_MINT);

  // Pick the two biggest absolute deltas (often the swap legs)
  filtered.sort((a, b) => Math.abs(b.deltaUi) - Math.abs(a.deltaUi));
  const top = filtered.slice(0, 2);

  const mints = [...new Set(top.map((t) => t.mint))];
  return { deltas: top, mints };
}

function deriveImpliedPrice(deltas) {
  // heuristic: if we have two legs, price = |quote| / |base|
  if (!deltas || deltas.length < 2) return null;
  const [a, b] = deltas;

  const base = Math.abs(a.deltaUi);
  const quote = Math.abs(b.deltaUi);

  if (!base || !quote) return null;
  return quote / base;
}

export async function parseRaydiumSwapEvent({
  connection,
  signature,
  slot,
  logs,
}) {
  const parsedTx = await connection.getParsedTransaction(signature, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0, // supports v0 tx
  });

  if (!parsedTx?.meta) return null;

  const { deltas, mints } = extractTokenDeltas(parsedTx);
  if (!mints.length) return null;

  return {
    signature,
    slot,
    mints,
    deltas,
    price: deriveImpliedPrice(deltas),
    // keep raw logs if you want:
    // logs,
  };
}
