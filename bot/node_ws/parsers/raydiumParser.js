// parsers/raydiumParser.js (replace your current version)
import { PublicKey } from "@solana/web3.js";
import { getMetaplexMetadata } from "../utils/metaplexMetadata.js";
import { getTokenSupplyUi } from "../utils/onChain.js";
import { rpcQueue } from "../utils/rpcQueue.js";

export const RAYDIUM_PROGRAM_IDS = [
  "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8", // AMM
  "RVKd61ztZW9GUwhRbbuY4VXKKHmHeUEhg8samq36cVy", // CPMM
  "27haf8L6oxUeXrHrgEgsexjSY5hbVdEmiUF4b2FhwzSQ", // CLMM
];

export function isRaydiumLog(logs) {
  return logs && logs.some((log) => log.includes("ray_log:"));
}

function computeTokenDeltas(preBalances = [], postBalances = []) {
  const deltas = [];
  const balanceMap = new Map();

  // Create map by account index + mint
  for (const balance of preBalances) {
    if (!balance.accountIndex && balance.accountIndex !== 0) continue;
    const key = `${balance.accountIndex}:${balance.mint}`;
    balanceMap.set(key, { pre: balance, post: null });
  }

  for (const balance of postBalances) {
    if (!balance.accountIndex && balance.accountIndex !== 0) continue;
    const key = `${balance.accountIndex}:${balance.mint}`;
    const existing = balanceMap.get(key);
    if (existing) {
      existing.post = balance;
    } else {
      balanceMap.set(key, { pre: null, post: balance });
    }
  }

  // Calculate deltas
  for (const [key, { pre, post }] of balanceMap.entries()) {
    if (!post) continue; // Skip if no post balance

    const preAmount = pre ? BigInt(pre.uiTokenAmount?.amount || "0") : 0n;
    const postAmount = BigInt(post.uiTokenAmount?.amount || "0");
    const delta = postAmount - preAmount;

    if (delta !== 0n) {
      const preUi = pre ? Number(pre.uiTokenAmount?.uiAmount || 0) : 0;
      const postUi = Number(post.uiTokenAmount?.uiAmount || 0);

      deltas.push({
        mint: post.mint,
        owner: post.owner,
        decimals: post.uiTokenAmount?.decimals || 0,
        deltaRaw: delta.toString(),
        uiAmount: postUi - preUi,
        accountIndex: post.accountIndex,
      });
    }
  }

  return deltas;
}

function identifySwapTokens(deltas) {
  if (deltas.length < 2) return null;

  // Group by mint to find net changes
  const mintChanges = new Map();
  for (const delta of deltas) {
    const current = mintChanges.get(delta.mint) || 0;
    mintChanges.set(delta.mint, current + delta.uiAmount);
  }

  // Find significant changes only
  const significant = Array.from(mintChanges.entries()).filter(
    ([, change]) => Math.abs(change) > 0.001,
  ); // Filter noise

  if (significant.length < 2) return null;

  // Sort by absolute value
  const sorted = significant.sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]));

  // Input token has negative net change, output has positive
  const input = sorted.find(([, change]) => change < 0);
  const output = sorted.find(([, change]) => change > 0);

  // Safety check: ensure we have one of each
  if (!input || !output) {
    // Fallback: use top 2 by absolute value
    if (sorted.length >= 2) {
      return {
        inputMint: sorted[0][0],
        inputAmount: Math.abs(sorted[0][1]),
        outputMint: sorted[1][0],
        outputAmount: Math.abs(sorted[1][1]),
      };
    }
    return null;
  }

  return {
    inputMint: input[0],
    inputAmount: Math.abs(input[1]),
    outputMint: output[0],
    outputAmount: Math.abs(output[1]),
  };
}

export async function parseRaydiumSwap(connection, signature, logs, slot) {
  return rpcQueue.request(
    async () => {
      const parsedTx = await connection.getParsedTransaction(signature, {
        commitment: "confirmed",
        maxSupportedTransactionVersion: 0,
      });

      if (!parsedTx?.meta) {
        console.log(`No meta for tx: ${signature}`);
        return null;
      }

      const deltas = computeTokenDeltas(
        parsedTx.meta.preTokenBalances || [],
        parsedTx.meta.postTokenBalances || [],
      );

      if (deltas.length < 2) {
        console.log(`Insufficient deltas for tx: ${signature}`);
        return null;
      }

      const swapTokens = identifySwapTokens(deltas);
      if (!swapTokens) {
        console.log(`Could not identify swap tokens for: ${signature}`);
        return null;
      }

      // Prevent same mint bug
      if (swapTokens.inputMint === swapTokens.outputMint) {
        console.log(`Same mint detected, skipping: ${swapTokens.inputMint}`);
        return null;
      }

      // Parallel enrichment with caching
      const [inputMetadata, outputMetadata, supply] = await Promise.all([
        getMetaplexMetadata(connection, swapTokens.inputMint),
        getMetaplexMetadata(connection, swapTokens.outputMint),
        getTokenSupplyUi(connection, swapTokens.outputMint),
      ]);

      const price =
        swapTokens.inputAmount > 0
          ? swapTokens.outputAmount / swapTokens.inputAmount
          : null;

      const marketCap = price && supply ? price * supply : null;

      return {
        signature,
        slot: slot || parsedTx.slot,
        kind: "swap",
        input: {
          mint: swapTokens.inputMint,
          amount: swapTokens.inputAmount,
          symbol: inputMetadata?.symbol || null,
          name: inputMetadata?.name || null,
        },
        output: {
          mint: swapTokens.outputMint,
          amount: swapTokens.outputAmount,
          symbol: outputMetadata?.symbol || null,
          name: outputMetadata?.name || null,
          supply: supply,
        },
        price,
        marketCap,
        hasRayLog: isRaydiumLog(logs),
        timestamp: new Date().toISOString(),
      };
    },
    `parse-${signature}`,
    30000,
  ); // Cache parsed results for 30 seconds
}
