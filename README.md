What improved system performance
1. (Most important) Tune batcher per service: sometimes batcher timeout constitutes most of a request's latency
2. Use pprof and OpenTelemtry to find aggregate CPU/block times + break down latency per request
3. Keep exec's serial runCoordinator light
4. Convert HTTP to TCP: network cost is nontrivial especially for a replication protocol
5. Disabling OpenTelemtry/lots of logging during test time
6. Having middle service fan out to only the primary batcher of the backend service
7. If you have a mac, have it be charging