import { getTokenMetadata } from "./fetchMetaData";

const metadataCache = new Map();

export async function getCachedTokenMetadata(tokenAddress) {
  if (metadataCache.has(tokenAddress)) return metadataCache.get(tokenAddress);
  const metadata = await getTokenMetadata(tokenAddress);
  metadataCache.set(tokenAddress, metadata);
  return metadata;
}

module.exports = { getCachedTokenMetadata };
