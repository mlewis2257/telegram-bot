// index.js
import "dotenv/config";
import { Connection } from "@solana/web3.js";
import {
  RAYDIUM_AMM_PROGRAM_ID,
  isRaydiumInitLogs,
  parseRaydiumInitFromSignature,
} from "./parsers/raydiumParser.js";
import { rpcQueue } from "./utils/queue.js";

const RPC_URL = process.env.RPC_URL || "https://api.devnet.solana.com";
const RPC_WSS = process.env.RPC_WSS; // strongly recommended (wss://...)
const MODE = process.env.MODE || "init"; // "init" | "debug"

const connection = new Connection(RPC_URL, {
  commitment: "confirmed",
  wsEndpoint: RPC_WSS,
});

const pending = new Map(); // signature -> { signature, slot }
let draining = false;

let raydiumLogsSeen = 0;
let raydiumInitSeen = 0;

function pushPending({ signature, slot }) {
  pending.set(signature, { signature, slot });
}

async function drainBatch() {
  if (draining) return;
  draining = true;

  try {
    const batch = Array.from(pending.values()).slice(0, 15);
    for (const item of batch) pending.delete(item.signature);
    if (batch.length === 0) return;

    const results = await Promise.all(
      batch.map((item) =>
        rpcQueue.add(() =>
          parseRaydiumInitFromSignature(connection, item.signature, item.slot),
        ),
      ),
    );

    for (const r of results) {
      if (!r) continue;
      console.log("\n🧠 Raydium init parsed:");
      console.dir(r, { depth: null });
      if (r.debug) console.log("⚠️ debug:", r.debug);
    }
  } finally {
    draining = false;
  }
}

function pickRaydiumOnlyLines(allLogs, raydiumProgramIdStr) {
  if (!Array.isArray(allLogs)) return [];

  // Keep only lines that clearly belong to Raydium program execution
  // (invoke/success/consumed lines for Raydium program id, plus raydium-specific markers)
  return allLogs.filter((l) => {
    if (!l) return false;
    return (
      l.includes(`Program ${raydiumProgramIdStr} `) || // invoke/success/consumed
      l.includes("ray_log:") || // Raydium emits ray_log
      l.includes("initialize2") || // init pools
      l.includes("AddLiquidity") || // add liquidity
      l.includes("swap") // swaps (if/when you add swap parsing)
    );
  });
}

async function main() {
  console.log("RPC:", RPC_URL);
  console.log("WSS:", RPC_WSS || "(inferred by web3.js)");
  console.log(`MODE: ${MODE}`);
  console.log("🔁 Listening to Raydium program logs...");

  connection.onLogs(
    RAYDIUM_AMM_PROGRAM_ID,
    async (logInfo) => {
      raydiumLogsSeen++;
      if (raydiumLogsSeen % 50 === 0) {
        console.log(`📡 Raydium logs seen: ${raydiumLogsSeen}`);
      }

      // DEBUG mode: print occasional samples so you KNOW it's alive
      if (MODE === "debug" && raydiumLogsSeen % 200 === 0) {
        const rayOnly = pickRaydiumOnlyLines(
          logInfo.logs,
          RAYDIUM_AMM_PROGRAM_ID.toBase58(),
        );

        console.log("🧾 SAMPLE SIG:", logInfo.signature);

        if (rayOnly.length) {
          console.log("🟣 Raydium-only logs:");
          console.log(rayOnly.slice(0, 50).join("\n"));
        } else {
          // Fallback if tx had Raydium but logs didn't match patterns (rare)
          console.log(
            "🟡 No Raydium-only lines matched; showing first 25 raw lines:",
          );
          console.log(logInfo.logs.slice(0, 25).join("\n"));
        }

        console.log("—".repeat(70));
      }
      const rayOnly = pickRaydiumOnlyLines(
        logInfo.logs,
        RAYDIUM_AMM_PROGRAM_ID.toBase58(),
      );

      // Strict init filter (use rayOnly to avoid false context noise)
      if (!isRaydiumInitLogs(rayOnly)) return;

      raydiumInitSeen++;
      console.log(
        `🆕 initialize2 detected: ${raydiumInitSeen} | sig=${logInfo.signature}`,
      );

      pushPending(logInfo);
    },
    "confirmed",
  );

  setInterval(() => {
    drainBatch().catch((e) => console.error("drainBatch error:", e));
  }, 250);
}

main().catch(console.error);
