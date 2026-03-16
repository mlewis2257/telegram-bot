// utils/rpcQueue.js (replace your current queue.js)
import PQueue from "p-queue";

class UnifiedRPCQueue {
  constructor() {
    // Single queue for ALL RPC calls with aggressive rate limiting
    this.mainQueue = new PQueue({
      concurrency: 1, // Critical: Only 1 concurrent request
      intervalCap: 2, // Max 2 requests per interval
      interval: 1000, // Per second
      carryoverConcurrencyCount: true,
    });

    this.cache = new Map();
    this.cacheTtl = 300000; // 5 minutes default

    // Track recent calls for debugging
    this.callCount = 0;
    this.lastReset = Date.now();
  }

  async request(operation, cacheKey = null, ttl = null) {
    return this.mainQueue.add(async () => {
      // Check cache first
      if (cacheKey && this.cache.has(cacheKey)) {
        const cached = this.cache.get(cacheKey);
        if (Date.now() - cached.timestamp < (ttl || this.cacheTtl)) {
          return cached.data;
        }
      }

      try {
        // Track rate limiting
        this.callCount++;
        const now = Date.now();
        if (now - this.lastReset > 1000) {
          this.callCount = 1;
          this.lastReset = now;
        }

        if (this.callCount > 10) {
          // Safety valve
          await new Promise((r) => setTimeout(r, 100));
        }

        const result = await operation();

        // Cache successful result
        if (cacheKey) {
          this.cache.set(cacheKey, {
            data: result,
            timestamp: Date.now(),
          });
        }

        return result;
      } catch (error) {
        // Don't cache errors
        if (
          error.message?.includes("429") ||
          error.message?.includes("Too Many Requests")
        ) {
          console.warn("RPC Rate limited, waiting...");
          await new Promise((r) => setTimeout(r, 2000)); // Wait 2 seconds on 429
        }
        throw error;
      }
    });
  }

  clearCache(key = null) {
    if (key) {
      this.cache.delete(key);
    } else {
      this.cache.clear();
    }
  }

  getStats() {
    return {
      pending: this.mainQueue.pending,
      size: this.mainQueue.size,
      callCount: this.callCount,
      cacheSize: this.cache.size,
    };
  }
}

// Global instance - use this for ALL RPC calls
export const rpcQueue = new UnifiedRPCQueue();

// Optional: Keep your existing queues for non-RPC operations
export const metadataQueue = new PQueue({ concurrency: 3 });
export const liquidityQueue = new PQueue({ concurrency: 2 });
export const enrichQueue = new PQueue({ concurrency: 10 });
