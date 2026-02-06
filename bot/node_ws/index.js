// index.js
import "dotenv/config";
import { Connection } from "@solana/web3.js";
import { rpcQueue } from "./utils/queue.js";
import { parseRaydiumSwapFromSignature } from "./parsers/raydiumSwapParser.js";

const RPC_URL = process.env.RPC_URL || "https://api.mainnet-beta.solana.com";
const RPC_WSS = process.env.RPC_WSS;
const MODE = process.env.MODE || "debug";

const RAYDIUM_AMM_PROGRAM_ID = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";

const connection = new Connection(RPC_URL, {
  commitment: "confirmed",
  wsEndpoint: RPC_WSS,
});

const pending = new Map(); // signature -> slot
let draining = false;

const BATCH_SIZE = Number(process.env.BATCH_SIZE || 20);
const DRAIN_EVERY_MS = Number(process.env.DRAIN_EVERY_MS || 250);

let seen = 0;

function extractProgramLogBlocks(logs, programIdStr) {
  const blocks = [];
  let current = null;

  for (const line of logs || []) {
    if (line.startsWith(`Program ${programIdStr} invoke`)) {
      if (current?.length) blocks.push(current);
      current = [line];
      continue;
    }
    if (current) {
      current.push(line);
      if (
        line === `Program ${programIdStr} success` ||
        line.startsWith(`Program ${programIdStr} failed:`)
      ) {
        blocks.push(current);
        current = null;
      }
    }
  }
  if (current?.length) blocks.push(current);
  return blocks;
}

function pickBiggestBlock(blocks) {
  if (!blocks.length) return null;
  return blocks.reduce((a, b) => (b.length > a.length ? b : a), blocks[0]);
}

function enqueue(sig, slot) {
  pending.set(sig, slot);
}

async function drainBatch() {
  if (draining) return;
  draining = true;

  try {
    const batch = Array.from(pending.entries()).slice(0, BATCH_SIZE);
    for (const [sig] of batch) pending.delete(sig);
    if (!batch.length) return;

    const results = await Promise.all(
      batch.map(([signature, slot]) =>
        rpcQueue.add(() =>
          parseRaydiumSwapFromSignature(connection, signature, slot),
        ),
      ),
    );

    for (const r of results) {
      if (!r) continue;
      console.log("\n💱 Raydium Swap Parsed:");
      console.dir(r, { depth: null });
    }
  } catch (e) {
    console.error("drainBatch error:", e?.message || e);
  } finally {
    draining = false;
  }
}

async function main() {
  console.log("RPC:", RPC_URL);
  console.log("WSS:", RPC_WSS || "(inferred)");
  console.log("MODE:", MODE);
  console.log("🔁 Listening to Raydium program logs (swap-driven)...");

  connection.onLogs(
    RAYDIUM_AMM_PROGRAM_ID,
    async (logInfo) => {
      seen++;
      if (seen % 50 === 0) console.log(`📡 Raydium logs seen: ${seen}`);

      // Debug: show Raydium-only block every 200 hits
      if (MODE === "debug" && seen % 200 === 0) {
        const blocks = extractProgramLogBlocks(
          logInfo.logs,
          RAYDIUM_AMM_PROGRAM_ID,
        );
        const biggest = pickBiggestBlock(blocks);
        console.log("🧾 SAMPLE SIG:", logInfo.signature);
        if (biggest) {
          console.log("🟣 Raydium Block:");
          console.log(biggest.slice(0, 80).join("\n"));
        } else {
          console.log("🟡 No Raydium-only block found");
        }
        console.log("—".repeat(70));
      }

      // ✅ For swaps we don’t rely on log keywords;
      // we just enqueue signatures and let the parser decide if it’s a swap.
      enqueue(logInfo.signature, logInfo.slot);
    },
    "confirmed",
  );

  setInterval(() => drainBatch().catch(console.error), DRAIN_EVERY_MS);
}

main().catch(console.error);
