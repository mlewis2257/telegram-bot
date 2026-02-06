// utils/metaplexMetadata.js
import { PublicKey } from "@solana/web3.js";
import * as mpl from "@metaplex-foundation/mpl-token-metadata";
import "./queue.js";

// Metaplex Token Metadata Program
export const TOKEN_METADATA_PROGRAM_ID = new PublicKey(
  "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s",
);

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

// Metaplex strings often contain null padding
function cleanStr(s) {
  if (!s) return null;
  return String(s).replace(/\0/g, "").trim() || null;
}
/**
 * Decode Metaplex metadata account buffer into a JS object.
 * Handles version differences in mpl-token-metadata package.
 */

export async function getMetaplexMetadata(connection, mint) {
  return rpcQueue.add(async () => {
    try {
      const pda = getMetadataPda(mint);
      // Reads and decodes Metadata account (borsh)
      const account = await connection.getAccountInfo(pda, "confirmed");
      if (!account?.data) return null;
      // IMPORTANT: deserializeMetadata expects the raw account buffer
      const md = mpl.deserializeMetadata(account.data);
      if (md) console.log(md.name, md.symbol, md.uri);

      // In this version, the "data" field typically contains name/symbol/uri
      const name = cleanStr(md?.data?.name);
      const symbol = cleanStr(md?.data?.symbol);
      const uri = cleanStr(md?.data?.uri);

      const updateAuthority = md?.updateAuthority
        ? (new PublicKey(md?.updateAuthority).toBase58() ??
          String(md?.updateAuthority))
        : null;
      return {
        mint,
        metadataPda: pda.toBase58(),
        name,
        symbol,
        uri,
        updateAuthority,
        raw: md,
      };
    } catch (error) {
      return null;
    }
  });
}
