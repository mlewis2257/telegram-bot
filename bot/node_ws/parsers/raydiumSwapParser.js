// parsers/raydiumSwapParser.js
import { PublicKey } from "@solana/web3.js";
import { getMetaplexMetadata } from "../utils/metaplexMetadata.js";
import { getTokenSupplyUi } from "../utils/onChain.js"; // you likely already have something similar

const WSOL_MINT = "So11111111111111111111111111111111111111112";

function toUiAmount(balanceObj) {
  if (!balanceObj) return null;
  const ui = balanceObj.uiTokenAmount;
  if (!ui) return null;
  return {
    amount: Number(ui.amount), // raw integer
    decimals: ui.decimals,
    uiAmount: ui.uiAmount ?? null,
    uiAmountString: ui.uiAmountString ?? null,
  };
}

function indexBalancesByAccount(tokenBalances = []) {
  const map = new Map();
  for (const b of tokenBalances) {
    if (!b?.accountIndex) continue;
    map.set(b.accountIndex, b);
  }
  return map;
}

/**
 * Returns token deltas per accountIndex:
 * deltaRaw = post.amount - pre.amount (raw integer)
 */
function computeTokenDeltas(parsedTx) {
  const pre = parsedTx?.meta?.preTokenBalances || [];
  const post = parsedTx?.meta?.postTokenBalances || [];

  const preMap = indexBalancesByAccount(pre);
  const postMap = indexBalancesByAccount(post);

  const deltas = [];

  for (const [idx, postB] of postMap.entries()) {
    const preB = preMap.get(idx);
    if (!preB) continue;

    const preAmt = BigInt(preB.uiTokenAmount.amount);
    const postAmt = BigInt(postB.uiTokenAmount.amount);

    const delta = postAmt - preAmt;
    if (delta === 0n) continue;

    deltas.push({
      accountIndex: idx,
      mint: postB.mint,
      owner: postB.owner || null,
      decimals: postB.uiTokenAmount.decimals,
      deltaRaw: delta.toString(),
    });
  }

  return deltas;
}

export async function parseRaydiumSwapFromSignature(
  connection,
  signature,
  slot,
) {
  const parsedTx = await connection.getParsedTransaction(signature, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });

  if (!parsedTx?.meta) return null;

  // Quick reject: no token balances changed
  const deltas = computeTokenDeltas(parsedTx);
  if (!deltas.length) return null;

  // Identify the two “main” mints involved (ignore WSOL if desired)
  const mintCounts = new Map();
  for (const d of deltas) {
    const m = d.mint;
    if (m === WSOL_MINT) continue;
    mintCounts.set(m, (mintCounts.get(m) || 0) + 1);
  }
  const mints = Array.from(mintCounts.entries())
    .sort((a, b) => b[1] - a[1])
    .map(([m]) => m)
    .slice(0, 2);

  // If we can’t find 2, still return what we have
  const [mintA, mintB] = mints;

  // Heuristic: pick biggest negative delta as “amount in”, biggest positive as “amount out”
  const sorted = [...deltas].sort((a, b) => {
    const da = BigInt(a.deltaRaw);
    const db = BigInt(b.deltaRaw);
    // sort by absolute value desc
    const aa = da < 0n ? -da : da;
    const ab = db < 0n ? -db : db;
    return ab > aa ? 1 : ab < aa ? -1 : 0;
  });

  const neg = sorted.find((d) => BigInt(d.deltaRaw) < 0n) || null;
  const pos = sorted.find((d) => BigInt(d.deltaRaw) > 0n) || null;

  // If no clear in/out, skip
  if (!neg || !pos) return null;

  // Enrich metadata (optional but useful)
  const [mdIn, mdOut] = await Promise.all([
    getMetaplexMetadata(connection, neg.mint).catch(() => null),
    getMetaplexMetadata(connection, pos.mint).catch(() => null),
  ]);

  return {
    signature,
    slot,
    kind: "swap",
    in: {
      mint: neg.mint,
      symbol: mdIn?.symbol ?? null,
      name: mdIn?.name ?? null,
      deltaRaw: neg.deltaRaw, // negative
      decimals: neg.decimals,
      owner: neg.owner,
    },
    out: {
      mint: pos.mint,
      symbol: mdOut?.symbol ?? null,
      name: mdOut?.name ?? null,
      deltaRaw: pos.deltaRaw, // positive
      decimals: pos.decimals,
      owner: pos.owner,
    },
    mintsDetected: mints,
    // You can add: price, pool reserves, marketCap here next
  };
}
