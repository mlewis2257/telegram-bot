require("dotenv").config();
const web3 = require("@solana/web3.js");
// Uses url for direct blockchain access
const RPC_URL = process.env.RPC_URL;

// Establish connection via websocket for blockcahin logs
(async () => {
  const connection = new web3.Connection(RPC_URL, "confirmed");

  console.log("🟢 Listening for logs...");
  // Returns unparsed data
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
