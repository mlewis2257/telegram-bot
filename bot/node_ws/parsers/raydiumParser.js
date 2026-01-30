function isRaydiumLogs(logs) {
  return logs.some(
    (log) =>
      log.includes("initialize2") ||
      log.includes("initialize") ||
      log.includes("AddLiquidity") ||
      log.includes("swap"),
  );
}

function parseRaydiumLogs(logs) {
  data = {
    tokenAddress: null,
    market: null,
    poolAuthority: null,
    interactionType: null,
    txHints: [],
  };

  for (const log of logs) {
    // Basic type identification
    if (log.includes("initialize2")) data.interactionType = "initialize2";
    if (log.includes("initialize")) data.interactionType = "initialize";
    if (log.includes("AddLiquidity")) data.interactionType = "AddLiquidity";
    if (log.includes("swap")) data.interactionType = "swap";

    // LP pair detection
    if (log.includes("market")) {
      const match = log.match("/market=([A-Za-z0-9]+)/");
      if (match) data.market = match[1];
    }
    if (log.includes("poolAuthority=")) {
      const match = log.match(/poolAuthority=([A-Za-z0-9]+)/);
      if (match) data.poolAuthority = match[1];
    }

    if (log.includes("mint=")) {
      const match = log.match(/mint=([A-Za-z0-9]+)/);
      if (match) data.tokenAddress = match[1];
    }

    // Save interesting lines
    data.txHints.push(log);
  }

  return data;
}

module.exports = { isRaydiumLogs, parseRaydiumLogs };
