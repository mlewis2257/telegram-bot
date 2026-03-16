// index.js
import "dotenv/config";
import { Connection, PublicKey } from "@solana/web3.js";
import { rpcQueue } from "./utils/rpcQueue.js";
import {
  parseRaydiumSwap,
  RAYDIUM_PROGRAM_IDS as PARSER_PROGRAM_IDS,
} from "./parsers/raydiumParser.js";

const RPC_URL = process.env.RPC_URL || "https://api.mainnet-beta.solana.com";
const RPC_WSS =
  process.env.RPC_WSS ||
  RPC_URL.replace("https://", "wss://").replace("http://", "ws://");

const MODE = process.env.MODE || "swap"; // "debug" | "swap"

// Raydium programs you’ve observed in your logs:
const RAYDIUM_PROGRAM_IDS = PARSER_PROGRAM_IDS.map((s) => new PublicKey(s));

const connection = new Connection(RPC_URL, {
  commitment: "confirmed",
  wsEndpoint: RPC_WSS,
  disableRetryOnRateLimit: true,
});

// ---- batching / throttling ----
const pending = new Map(); // signature -> slot
let draining = false;

const BATCH_SIZE = 1; // keep low on public RPC
const DRAIN_EVERY_MS = 2000; // slower = fewer 429s
const MAX_PENDING = 100;

let seen = 0;
let rayLogSeen = 0;

function hasRayLog(logs) {
  return (logs || []).some((l) => l.includes("ray_log:"));
}

function enqueue(signature, slot, logs) {
  if (!signature) return;
  if (pending.has(signature)) return; // de-dupe
  if (pending.size >= MAX_PENDING) {
    const firstKey = pending.keys().next().value;
    pending.delete(firstKey);
  } // drop when overloaded
  pending.set(signature, { slot, logs });
}

async function drainBatch() {
  if (draining || pending.size === 0) return;

  draining = true;
  try {
    // Process ONE transaction at a time to avoid queue blocking
    const batchEntry = Array.from(pending.entries())[0];
    if (!batchEntry) return;

    const [signature, { slot, logs }] = batchEntry;
    pending.delete(signature);

    console.log(`🔄 Processing transaction: ${signature.substring(0, 16)}...`);

    try {
      const result = await rpcQueue.request(
        () => parseRaydiumSwap(connection, signature, logs, slot),
        `swap-${signature}`,
        30000,
      );

      if (result) {
        console.log(`\n💱 Raydium Swap Detected (${result.slot}):`);
        console.log(`Signature: ${result.signature}`);
        console.log(
          `Input: ${result.input.amount} ${result.input.symbol ||
            result.input.mint}`,
        );
        console.log(
          `Output: ${result.output.amount} ${result.output.symbol ||
            result.output.mint}`,
        );
        if (result.price) console.log(`Price: ${result.price.toFixed(6)}`);
        if (result.marketCap)
          console.log(`Market Cap: ${result.marketCap.toFixed(0)}`);
        console.log("─".repeat(50));
      } else {
        console.log(
          `➖ No swap detected for: ${signature.substring(0, 16)}...`,
        );
      }
    } catch (error) {
      console.error(
        `❌ Error processing ${signature.substring(0, 16)}:`,
        error.message,
      );
    }

    // Log queue stats periodically
    if (seen % 10 === 0) {
      const stats = rpcQueue.getStats();
      console.log(
        `📊 Queue Stats: ${stats.pending} pending, ${stats.size} queued, cache: ${stats.cacheSize}`,
      );
    }
  } catch (e) {
    console.error("❌ Drain batch error:", e?.message || e);
  } finally {
    draining = false;
  }
}

async function main() {
  console.log("🚀 Starting Solana Trading Intelligence System");
  console.log("RPC:", RPC_URL);
  console.log("WSS:", RPC_WSS);
  console.log("MODE:", MODE);
  console.log("🔁 Listening to Raydium program logs (swap-driven)...");

  // ✅ The key fix:
  // Pass a *PublicKey* as the filter (NOT a string, NOT {mentions:...})
  for (const programPk of RAYDIUM_PROGRAM_IDS) {
    connection.onLogs(
      programPk,
      (logInfo) => {
        seen++;
        if (hasRayLog(logInfo.logs)) {
          rayLogSeen++;

          // Only enqueue if it has ray_log (more selective)
          enqueue(logInfo.signature, logInfo.slot, logInfo.logs);
        }

        if (MODE === "debug" && seen % 200 === 0) {
          console.log(`📡 logs seen: ${seen} | 🟣 ray_log seen: ${rayLogSeen}`);
          console.log(`📊 Pending queue: ${pending.size}`);
        }

        // Swap-driven: enqueue everything that mentions Raydium program;
        // parser decides if it’s a swap we care about.
      },
      "confirmed",
    );
    console.log(`✅ Subscribed to program: ${programPk.toBase58()}`);
  }

  // Drain queue every 2 seconds (slower = fewer 429s)
  setInterval(() => {
    drainBatch().catch(console.error);
  }, DRAIN_EVERY_MS);

  // Monitor queue health
  setInterval(() => {
    const stats = rpcQueue.getStats();
    if (stats.pending > 10) {
      console.warn(
        `⚠️  High queue pressure: ${stats.pending} pending requests`,
      );
    }
  }, 2000);
}

// Graceful shutdown
process.on("SIGINT", () => {
  console.log("\n🛑 Shutting down gracefully...");
  process.exit(0);
});

main().catch(console.error);
