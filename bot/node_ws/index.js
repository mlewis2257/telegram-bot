import { isRaydiumLogs, parseRaydiumLogs } from "./parsers/raydiumParser.js";

// Simulated logs from Solana
const sampleLogs = [
  "Program log: initialize2",
  "Program log: market=3gEy...4HL3",
  "Program log: poolAuthority=Fsd9...7Adf",
  "Program log: mint=6iT9...P45Y",
  "Program log: AddLiquidity completed",
];

if (isRaydiumLogs(sampleLogs)) {
  const parsedData = parseRaydiumLogs(sampleLogs);
  console.log("✅ Raydium Token Launch Detected:");
  console.log(parsedData);
} else {
  console.log("⛔ Not a Raydium launch.");
}
