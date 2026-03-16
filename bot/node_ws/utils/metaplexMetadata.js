// utils/metaplexMetadata.js
import * as mpl from "@metaplex-foundation/mpl-token-metadata";
// utils/metaplexMetadata.js (completely replace)
import { PublicKey } from "@solana/web3.js";
import { rpcQueue } from "./rpcQueue.js";

// Metaplex Token Metadata Program
export const TOKEN_METADATA_PROGRAM_ID = new PublicKey(
  "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
);

const cache = new Map();

export function getMetadataPda(mint) {
  const mintPk = new PublicKey(mint);
  const [pda] = PublicKey.findProgramAddressSync(
    [
      Buffer.from("metadata"),
      TOKEN_METADATA_PROGRAM_ID.toBuffer(),
      mintPk.toBuffer(),
    ],
    TOKEN_METADATA_PROGRAM_ID,
  );
  return pda;
}

function cleanStr(s) {
  if (!s) return null;
  return (
    String(s)
      .replace(/\0/g, "")
      .trim() || null
  );
}

// Manual metadata deserialization - works regardless of library version
function deserializeMetadataManual(buffer) {
  if (!buffer || buffer.length < 1) return null;

  try {
    // Skip key byte (0x4 for Metadata)
    let offset = 1;

    // Update authority (32 bytes)
    const updateAuthority = new PublicKey(buffer.slice(offset, offset + 32));
    offset += 32;

    // Mint (32 bytes)
    const mint = new PublicKey(buffer.slice(offset, offset + 32));
    offset += 32;

    // Data structure starts here
    const dataLen = buffer.readUInt32LE(offset);
    offset += 4;

    // Name (32 bytes)
    const nameBytes = buffer.slice(offset, offset + 32);
    const name = cleanStr(nameBytes.toString("utf8"));
    offset += 32;

    // Symbol (10 bytes)
    const symbolBytes = buffer.slice(offset, offset + 10);
    const symbol = cleanStr(symbolBytes.toString("utf8"));
    offset += 10;

    // URI (200 bytes)
    const uriBytes = buffer.slice(offset, offset + 100); // Often shorter
    const uri = cleanStr(uriBytes.toString("utf8"));

    return {
      mint: mint.toString(),
      updateAuthority: updateAuthority.toString(),
      data: { name, symbol, uri },
    };
  } catch (error) {
    console.error("Manual metadata deserialization failed:", error);
    return null;
  }
}

// Try library deserialization first, fallback to manual
function deserializeMetadata(buffer) {
  // Try mpl-token-metadata library first
  try {
    // Dynamic import to avoid version issues
    const mpl = require("@metaplex-foundation/mpl-token-metadata");

    if (typeof mpl.deserializeMetadata === "function") {
      const result = mpl.deserializeMetadata(buffer);
      return Array.isArray(result) ? result[0] : result;
    }
  } catch (libraryError) {
    console.warn("Library deserialization failed, using manual method");
  }

  // Fallback to manual parsing
  return deserializeMetadataManual(buffer);
}

export async function getMetaplexMetadata(connection, mint) {
  if (!mint) return null;

  const cacheKey = `metadata-${mint}`;
  if (cache.has(cacheKey)) {
    return cache.get(cacheKey);
  }

  const promise = rpcQueue.request(
    async () => {
      try {
        const pda = getMetadataPda(mint);

        const account = await connection.getAccountInfo(pda, "confirmed");
        if (!account?.data) {
          return null;
        }

        const metadata = deserializeMetadata(account.data);
        if (!metadata) {
          return null;
        }

        const result = {
          name: cleanStr(metadata.data?.name || metadata.name),
          symbol: cleanStr(metadata.data?.symbol || metadata.symbol),
          uri: cleanStr(metadata.data?.uri || metadata.uri),
          updateAuthority:
            metadata.updateAuthority?.toString?.() ||
            String(metadata.updateAuthority),
          mint: metadata.mint?.toString?.() || mint,
        };

        // Cache for 10 minutes
        setTimeout(() => cache.delete(cacheKey), 10 * 60 * 1000);

        return result;
      } catch (error) {
        console.error(`Metadata fetch error for ${mint}:`, error.message);
        // Cache failures briefly to avoid hammering
        setTimeout(() => cache.delete(cacheKey), 30000);
        return null;
      }
    },
    cacheKey,
    10 * 60 * 1000,
  ); // 10 minute cache

  cache.set(cacheKey, promise);
  return promise;
}
