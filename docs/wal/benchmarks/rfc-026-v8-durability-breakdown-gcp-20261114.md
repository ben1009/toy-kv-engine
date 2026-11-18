# V8 Point durability breakdown

Single GCP C4-16 diagnostic; 16 writers, 100,000 puts, 1 KiB values, Direct FRONTIER, fixed 400 us coalescing, sliding fallocate 4 MiB. Sample every 64 completed advances. Not throughput qualification.

Source: b2b275c62af13f6a8132329d1aa478f3dd90b5ec plus validated instrumentation patch f3416e4f0b79ccb9035698c106b5d2767b99f8214d9c2eb2d5e667fcf09754a5.

Result: 32,410 puts/s; sampled p50 0.484 ms, p99 1.534 ms. 6,329 completed advances; 98 samples; zero dropped. Archive transfer and result validation passed.

Median sampled phase times, microseconds:

| Phase | Wall | Thread CPU |
|---|---:|---:|
| Driver |371.51|121.46|
| Coalescing total |142.98|44.92|
| Coalescing active work |3.55|4.36|
| Condvar wait/reacquire interval |139.88|39.10|
| Retirement before Publish |16.78|16.64|
| Retirement after Publish |27.07|21.51|

Coalescing median: four Condvar returns/reacquisitions, five loop checks. Initial state-lock wait 0.35 us. Retirement ledger-lock waits were about 0.35 us each; lock hold medians were 15.35 us before Publish and 25.89 us after Publish.

The coalescing CPU is concentrated in Condvar wait/reacquire intervals, rather than active prefix checks. This includes synchronization, wake handling, and measurement overhead; it is not pure sleeping CPU. Retirement work is substantial while lock acquisition waits are small. Distinct phase medians must not be summed into a cycle. One run and 98 samples do not establish stable tail behavior. Durable-to-Put/MVCC tail remains unmeasured.

Initial setup failed due an external-diff formatted patch. A validated unified patch was generated with --no-ext-diff. The first benchmark was rejected because the temporary validator compared changing extent counters; its handles remained stable. The successful repeat compared handle identity/mode and used a fresh database. Previous artifacts were preserved.

No cloud cleanup or PR merge was performed. VM retains its original 60-minute automatic deletion deadline. Archives remain local and are not uploaded to GitHub.
