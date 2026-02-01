// parsers/raydiumParser.js
import { PublicKey } from "@solana/web3.js";
import { rpcQueue } from "../utils/queue.js";
import {
  chooseBaseAndQuote,
  computePoolPricingAndMcap,
} from "../utils/onChain.js";
import { getMetaplexMetadata } from "../utils/metaplexMetadata.js";

export const RAYDIUM_AMM_PROGRAM_ID = new PublicKey(
  "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
);

export function isRaydiumInitLogs(logs) {
  // Important: keep this strict, otherwise you'll match swaps constantly.
  return logs?.some((l) => l.includes("initialize2")) ?? false;
}

function extractMintsFromParsedTx(parsedTx) {
  const mints = new Set();

  const post = parsedTx?.meta?.postTokenBalances || [];
  const pre = parsedTx?.meta?.preTokenBalances || [];

  for (const b of [...pre, ...post]) {
    if (b?.mint) mints.add(b.mint);
  }

  // Optional: remove wSOL if you want, but keep for pricing if it's the quote
  // mints.delete("So11111111111111111111111111111111111111112");

  return [...mints];
}

function getAllAccountKeys(parsedTx) {
  // Parsed tx gives accountKeys as objects with pubkey fields
  const keys = parsedTx?.transaction?.message?.accountKeys || [];
  return keys.map((k) =>
    typeof k === "string" ? k : (k.pubkey?.toBase58?.() ?? k.pubkey),
  );
}

function findRaydiumInstruction(parsedTx) {
  const ixs = parsedTx?.transaction?.message?.instructions || [];
  // Each ix can be "parsed" or "partiallyDecoded"
  for (const ix of ixs) {
    const programId =
      ix?.programId?.toBase58?.() ?? ix?.programId ?? ix?.programIdIndex; // last resort
    if (programId === RAYDIUM_AMM_PROGRAM_ID.toBase58()) return ix;
  }
  return null;
}

async function findVaultCandidates(connection, pubkeys, mintsSet) {
  // Fetch account infos (queued) and return token accounts whose mint is in mintsSet
  const candidates = [];

  const infos = await Promise.all(
    pubkeys.map((pk) =>
      rpcQueue.add(async () => {
        try {
          const res = await connection.getParsedAccountInfo(
            new PublicKey(pk),
            "confirmed",
          );
          return { pk, info: res.value };
        } catch {
          return { pk, info: null };
        }
      }),
    ),
  );

  for (const { pk, info } of infos) {
    const data = info?.data;
    if (
      !data ||
      data.program !== "spl-token" ||
      data.parsed?.type !== "account"
    )
      continue;

    const mint = data.parsed?.info?.mint;
    if (!mint || !mintsSet.has(mint)) continue;

    const uiAmount = Number(
      data.parsed?.info?.tokenAmount?.uiAmountString ?? 0,
    );
    candidates.push({ pubkey: pk, mint, uiAmount });
  }

  // Sort by largest balances first — pool vaults tend to be big after init/add liquidity
  candidates.sort((a, b) => b.uiAmount - a.uiAmount);
  return candidates;
}

function pickVaultsForBaseQuote(candidates, baseMint, quoteMint) {
  const baseVault = candidates.find((c) => c.mint === baseMint)?.pubkey ?? null;
  const quoteVault =
    candidates.find((c) => c.mint === quoteMint)?.pubkey ?? null;
  return { baseVault, quoteVault };
}

export async function parseRaydiumInitFromSignature(
  connection,
  signature,
  slot = null,
) {
  const parsedTx = await connection.getParsedTransaction(signature, {
    commitment: "confirmed",
    maxSupportedTransactionVersion: 0,
  });

  if (!parsedTx) return null;

  const logs = parsedTx.meta?.logMessages ?? [];
  if (!isRaydiumInitLogs(logs)) return null;

  const mints = extractMintsFromParsedTx(parsedTx);
  if (mints.length < 2) {
    return {
      signature,
      slot: slot ?? parsedTx.slot,
      kind: "raydium_initialize2",
      mints,
      debug: "Not enough mints found in token balances.",
    };
  }

  const { baseMint, quoteMint } = chooseBaseAndQuote(mints);

  // Find Raydium ix and scan its account list for vault candidates
  const rayIx = findRaydiumInstruction(parsedTx);

  // If we can't find the raydium instruction for some reason, fallback to scanning all keys
  const allKeys = getAllAccountKeys(parsedTx);

  const ixAccounts =
    rayIx?.accounts?.map((a) =>
      typeof a === "string" ? a : (a.toBase58?.() ?? a),
    ) ?? allKeys;

  const mintsSet = new Set([baseMint, quoteMint]);
  const candidates = await findVaultCandidates(
    connection,
    ixAccounts,
    mintsSet,
  );

  const { baseVault, quoteVault } = pickVaultsForBaseQuote(
    candidates,
    baseMint,
    quoteMint,
  );

  if (!baseVault || !quoteVault) {
    return {
      signature,
      slot: slot ?? parsedTx.slot,
      kind: "raydium_initialize2",
      baseMint,
      quoteMint,
      mints,
      debug:
        "Could not confidently identify pool vaults from instruction accounts.",
      vaultCandidates: candidates.slice(0, 10),
    };
  }

  const [pricing, metadata] = await Promise.all([
    computePoolPricingAndMcap({
      connection,
      baseMint,
      quoteMint,
      baseVault,
      quoteVault,
    }),
    getMetaplexMetadata(connection, baseMint),
  ]);

  return {
    signature,
    slot: slot ?? parsedTx.slot,
    kind: "raydium_initialize2",
    ...pricing,
    token: {
      mint: baseMint,
      name: metadata?.name ?? null,
      symbol: metadata?.symbol ?? null,
      uri: metadata?.uri ?? null,
      updateAuthority: metadata?.updateAuthority ?? null,
    },
  };
}
