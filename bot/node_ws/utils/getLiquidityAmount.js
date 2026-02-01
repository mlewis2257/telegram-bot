// Liquidity estimation

import { Connection, PublicKey } from "@solana/web3.js";
import { liquidityQueue } from "./queue";
const axios = require("axios");
require("dotenv").config();

export async function getTokenBalance(poolAuthority) {
  return await liquidityQueue.add(async () => {
    try {
      const url = `https://api.solanatracker.io/pool-liquidity/${poolAuthority}`;
      const response = await axios.get(url);
      return response.data?.liquidity || 0;
    } catch (error) {
      console.error(
        `Error fetching liquidity for ${poolAuthority}:`,
        error.message,
      );
      return null;
    }
  });
}

module.exports = { getTokenBalance };
