import PQueue from "p-queue";

// Rate limit queue to prevent bottlenecks from happening
export const metadataQueue = new PQueue({
  concurrency: 3,
  intervalCap: 5,
  interval: 1000,
});

export const liquidityQueue = new PQueue({
  concurrency: 2,
  intervalCap: 3,
  interval: 1000,
});

export const enrichQueue = new PQueue({ concurrency: 10 });

// Controls *HTTP* RPC calls you make after detecting logs
export const rpcQueue = new PQueue({
  concurrency: 4, // how many RPC calls at once
  interval: 1000, // 1 second windows
  intervalCap: 8, // max 8 tasks per second window
});

// module.exports = { metadataQueue, liquidityQueue, enrichQueue, rpcQueue };
