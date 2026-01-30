const { Connection, clusterApiUrl, PublicKey } = require("@solana/web3.js");

// Use raydium PubK to pull data from
const RAYDIUM_PROGRAM = new PublicKey(
  "5quBipSFXkKxja9gTJeWxJ3KHrrD2vWYLMtWHzq5G7nG",
);
const RPC_URL = process.env.RPC_URL || clusterApiUrl("devnet");

const connection = new Connection(RPC_URL, "confirmed");

// Listens to Raydium for logs
function listenToRaydium() {
  connection.onLogs(RAYDIUM_PROGRAM, async (logInfo) => {
    const { logs, signature } = logInfo;
    // filters through logs for new liquidity
    if (
      logs.some(
        (log) => log.includes("initialize") || log.includes("add_liquidity"),
      )
    ) {
      console.log("🔔 Raydium token event detected!");
      console.log("Tx Signature:", signature);
      //   Custom parser for data
      // const tokenData = await parseRaydiumLogs(logs);
      // sendToTelegram(tokenData);
    }
  });
  console.log("🧲 Listening for new Raydium tokens...");
}

console.log(listenToRaydium());

module.exports = { listenToRaydium };
