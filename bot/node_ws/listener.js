require("dotenv").config();
const web3 = require("@solana/web3.js");

const RPC_URL = process.env.RPC_URL;

(async () => {
  const connection = new web3.Connection(RPC_URL, "confirmed");

  console.log("🟢 Listening for logs...");

  connection.onLogs(
    "all",
    (log) => {
      console.log("🔔 Log Update:");
      console.log("Signature", log.signature);
      console.log("Logs:", log.logs);
    },
    "confirmed",
  );
})();
