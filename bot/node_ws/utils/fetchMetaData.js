const axios = require("axios");
const { metadataQueue } = require("./queue");

const metadataCache = new Map(); // mint -> { data, ts }
const TTL_MS = 1000 * 60 * 30; // 30 minutes

async function getTokenMetadata(mint) {
  const cached = metadataCache.get(mint);
  if (cached && Date.now() - cached.ts < TTL_MS) return cached.data;

  return metadataQueue.add(async () => {
    try {
      const url = `https://api.solanatracker.io/token-metadata/${mint}`;
      const res = await axios.get(url);

      const data = {
        name: res.data?.name ?? null,
        symbol: res.data?.symbol ?? null,
      };

      metadataCache.set(mint, { data, ts: Date.now() });
      return data;
    } catch (err) {
      return { name: null, symbol: null };
    }
  });
}

module.exports = { getTokenMetadata };
