// utils/onchain.js
import { PublicKey } from "@solana/web3.js";

// Common quote mints (optional heuristics)
export const WSOL_MINT = "So11111111111111111111111111111111111111112";
export const USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

export async function getTokenSupplyUi(connection, mint) {
  const res = await connection.getTokenSupply(new PublicKey(mint));
  // uiAmount can be null on some responses; uiAmountString is safer
  return Number(res.value.uiAmountString ?? 0);
}

export async function getTokenAccountBalanceUi(connection, tokenAccount) {
  const res = await connection.getTokenAccountBalance(
    new PublicKey(tokenAccount),
  );
  return {
    uiAmount: Number(res.value.uiAmountString ?? 0),
    decimals: res.value.decimals,
  };
}

export function chooseBaseAndQuote(mints) {
  // Prefer SOL/USDC as quote if present.
  if (mints.includes(WSOL_MINT)) {
    const base = mints.find((m) => m !== WSOL_MINT);
    return { baseMint: base ?? mints[0], quoteMint: WSOL_MINT };
  }
  if (mints.includes(USDC_MINT)) {
    const base = mints.find((m) => m !== USDC_MINT);
    return { baseMint: base ?? mints[0], quoteMint: USDC_MINT };
  }
  // fallback: first is base, second is quote
  return { baseMint: mints[0], quoteMint: mints[1] ?? null };
}

export function calcPriceFromReserves({ baseReserve, quoteReserve }) {
  if (!baseReserve || !quoteReserve) return null;
  if (baseReserve <= 0) return null;
  return quoteReserve / baseReserve;
}

export async function computePoolPricingAndMcap({
  connection,
  baseMint,
  quoteMint,
  baseVault,
  quoteVault,
}) {
  const [baseBal, quoteBal, supply] = await Promise.all([
    getTokenAccountBalanceUi(connection, baseVault),
    getTokenAccountBalanceUi(connection, quoteVault),
    getTokenSupplyUi(connection, baseMint),
  ]);

  const baseReserve = baseBal.uiAmount;
  const quoteReserve = quoteBal.uiAmount;

  const priceInQuote = calcPriceFromReserves({ baseReserve, quoteReserve });
  const marketCapInQuote = priceInQuote ? supply * priceInQuote : null;

  return {
    baseMint,
    quoteMint,
    baseVault,
    quoteVault,
    reserves: {
      base: baseReserve,
      quote: quoteReserve,
    },
    priceInQuote,
    supply,
    marketCapInQuote,
  };
}
