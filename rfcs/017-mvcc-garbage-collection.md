# RFC 017: MVCC Garbage Collection

**Status**: Proposed  
**Author**: kv-engine Contributors  
**Created**: 2026-07-10  
**Target Version**: Post-RFC 016  
**Tracking Issue**: N/A (design RFC)

---

## Summary

This RFC proposes kv-engine's MVCC garbage-collection design.

The target design is a standalone scheduler-driven MVCC-GC subsystem, not
merely an opportunistic side effect of ordinary compaction. The scheduler:

1. periodically computes an effective GC cutoff from MVCC reader state,
2. inspects SST metadata and runtime stats,
3. selects GC-worthy SSTs even when normal size-based compaction is idle,
4. submits targeted compaction work to rewrite those SSTs,
5. lets the compaction rewrite path perform the actual keep/drop decisions.

The compaction-time GC rules are:

1. for each user key, keep all versions above the cutoff,
2. keep the newest version at or below the cutoff,
3. drop older below-cutoff versions,
4. drop point tombstones, range-covered values, and range-tombstone fragments
   only when bottom-level safety conditions hold,
5. drop TTL-expired retained boundary versions only under a stronger whole-key
   deletion gate.

In short:

1. scheduling is standalone,
2. execution is compaction,
3. safety is still enforced by the existing MVCC/tombstone/range/TTL rules.

---

## Motivation

MVCC solves snapshot visibility, but it creates a storage-liveness problem:

1. overwrites create older historical versions,
2. deletes create tombstones,
3. range deletes accumulate fragment metadata,
4. TTL expiry can leave expired entries physically present,
5. obsolete pointer-bearing versions can keep vLog space live indirectly.

If GC only runs when normal compaction happens to run, reclaim quality becomes
an accident of size-growth heuristics instead of an intentional storage policy.

That is not enough for an MVCC engine. The system needs to reclaim obsolete
versions even when:

1. write traffic is light,
2. level sizes are balanced,
3. no ordinary compaction trigger fires,
4. old versions or tombstones have accumulated for policy reasons rather than
   space-ratio reasons.

So MVCC GC needs its own trigger path. Compaction remains the physical
mechanism for rewriting SSTs, but it is no longer the scheduler of record.

---

## Goals

1. Make MVCC GC independently triggerable from ordinary compaction heuristics.
2. Use a safepoint/watermark-style cutoff that remains safe for active readers.
3. Use SST metadata and runtime stats to pick GC-worthy files when normal
   compaction is idle.
4. Reuse the compaction rewrite path as the physical execution mechanism.
5. Preserve current MVCC GC semantics for per-key version retention.
6. Preserve current bottom-level safety requirements for tombstone and
   range-tombstone cleanup.
7. Integrate TTL cleanup and vLog interaction into the same design.

## Non-Goals

1. Deleting versions directly out of memtables or WALs.
2. Reclaiming data by metadata-only edits without SST rewrite.
3. Full TiKV-style distributed GC coordination.
4. Replacing the current compaction pipeline with a separate SST rewrite engine.
5. vLog file reclamation itself; this RFC only covers how MVCC GC affects it.

---

## Design Overview

The design has two layers.

### 1. Standalone MVCC-GC scheduler

A background scheduler periodically runs and:

1. computes the effective GC cutoff,
2. examines SST metadata/stats,
3. generates targeted GC compaction tasks,
4. enqueues them even if ordinary size-based compaction has no work.

### 2. Compaction-time execution

Once a GC compaction task runs, the existing compaction rewrite path decides
what to keep and drop. The core rule remains:

```text
for each user key, in version order newest -> oldest:
  keep all versions with ts > cutoff
  keep the first version with ts <= cutoff
  drop remaining versions with ts <= cutoff
```

Additional physical-deletion rules remain stricter:

1. point tombstones: bottom-level only,
2. range-covered point entries: bottom-level only,
3. range-tombstone fragments: bottom-level only,
4. TTL-deletion of the retained boundary version: bottom-level plus no active
   readers.

So the system is:

```text
GC scheduler
   │
   ├── compute cutoff
   ├── inspect SST metadata/stats
   └── choose GC compaction candidates
            │
            ▼
      targeted compaction tasks
            │
            ▼
   existing compaction rewrite path
            │
            ▼
   MVCC/tombstone/range/TTL keep-drop logic
```

---

## Cutoff Model

The scheduler needs a cutoff equivalent to a safepoint/watermark boundary.

In the current single-node engine, the natural source is the MVCC watermark:

1. every active `ReadGuard` registers its `read_ts`,
2. `watermark()` returns the smallest active reader timestamp,
3. when no readers exist, it falls back to `latest_commit_ts`.

That means:

1. versions strictly newer than the cutoff are always kept,
2. versions at or below the cutoff are candidates for pruning,
3. active readers still define the lower bound of visible history.

The design intentionally leaves room for a future explicit GC safepoint, but
the current design does not require one. Today, the scheduler can use the MVCC
watermark directly as its effective cutoff.

---

## SST Metadata and Stats for Candidate Picking

The scheduler does not reclaim data directly. It uses metadata and stats to
decide which SSTs are worth rewriting for GC.

Useful signals already present or naturally derivable in this engine include:

1. `sst.max_ts()`
2. TTL metadata:
   - `min_ttl_expire_ts`
   - `max_ttl_expire_ts`
   - `has_non_ttl_entries`
   - `ttl_entry_count`
   - `total_entry_count`
3. tombstone/range-tombstone density
4. future redundant-version estimates per SST

### What `max_ts` is good for

`max_ts` is a strong candidate-selection signal:

1. if `sst.max_ts() <= cutoff`, then every version in that SST is old enough to
   be examined for GC,
2. such an SST is a strong GC compaction candidate even when size-based
   compaction would ignore it.

But `max_ts` is not enough to reclaim data by itself:

1. it does not prove which version is the retained boundary version per key,
2. it does not prove whether a tombstone is safe to delete,
3. it does not prove overlap relationships with deeper levels,
4. it does not reveal how much reclaimable data actually exists.

So metadata drives scheduling, not deletion.

### TTL-specific candidate picking

TTL metadata is especially useful because it can identify high-value GC
opportunities:

1. SSTs where many TTL entries are expired,
2. SSTs where all TTL entries are expired,
3. SSTs with no non-TTL entries and old expiry windows.

Those are ideal GC compaction candidates even when ordinary compaction is idle.

---

## Standalone Scheduler

The standalone MVCC-GC scheduler should:

1. wake periodically,
2. compute the effective cutoff,
3. ask the normal compaction controller for ordinary work first,
4. if ordinary compaction is idle or insufficient, run a GC-specific candidate
   picker,
5. submit targeted compaction tasks for selected SSTs.

This gives GC a separate trigger path without inventing a new rewrite engine.

### Why compaction is still the execution mechanism

GC needs the full compaction context to decide what can be deleted:

1. per-key version order,
2. bottom-level status,
3. range-tombstone coverage,
4. TTL interpretation,
5. vLog reference rewriting/propagation.

That is why the scheduler should schedule compaction, not attempt to mutate
SST metadata or LSM state directly.

---

## Compaction-Time Point-Version GC

Once a GC compaction task is chosen, the compaction rewrite path applies the
existing per-key logic.

Entries are processed in internal-key order:

1. user key ascending,
2. timestamp descending within each user key.

This means compaction only needs transient per-key stream state:

1. `prev_user_key`
2. `seen_below_cutoff`

For each user key:

1. keep all versions with `ts > cutoff`,
2. keep the first version with `ts <= cutoff`,
3. drop later older versions with `ts <= cutoff`.

This is ordinary MVCC historical-version GC. It is valid even in non-bottom
compactions.

### Important distinction

A non-bottom GC compaction may already perform real physical deletion:

1. older below-cutoff point versions are removed from the rewritten SSTs,
2. but safety-sensitive deletion markers are still preserved until stronger
   conditions hold.

So “non-bottom compaction = only merge” is incorrect. It is:

1. merge,
2. plus safe per-key historical-version pruning.

---

## Point Tombstone GC

A point tombstone is not handled like an ordinary below-cutoff historical
version.

The retained boundary version for a key may be a tombstone. Deleting that
tombstone is only safe when:

1. the compaction is bottom-level, and
2. no older lower-level point value can reappear.

So the rule is:

1. non-bottom GC compaction may delete older historical values,
2. non-bottom GC compaction must still keep a boundary tombstone if it is
   needed to hide deeper data.

This is why bottom-level proof is a deletion-safety rule, not a trigger rule.

---

## Range-Tombstone Fragment GC

Range tombstones have two GC surfaces.

### 1. Covered point-entry dropping

If a bottom-level GC compaction proves that a merged range-tombstone fragment
covers a point version at or below the cutoff, the point version may be dropped
from output SSTs.

### 2. Fragment removal

Once bottom-level compaction proves a fragment is permanently visible to all
current and future readers, the fragment itself may be removed from output SSTs.

So again:

1. the scheduler decides when to run GC compaction,
2. bottom-level proof decides whether these stronger deletions are legal.

---

## TTL Compaction

TTL cleanup belongs in the same GC design, but it is stronger than ordinary
historical-version pruning.

Ordinary MVCC GC already reclaims expired versions that lie below the retained
boundary version for a key. TTL-specific deletion only matters when the
retained boundary version itself is expired and compaction wants to delete that
version too.

That makes TTL deletion a whole-key physical-deletion rule, not an ordinary
below-cutoff pruning rule.

The safety gate is therefore stronger:

1. bottom-level compaction, and
2. no active readers.

This is semantically closer to compaction-filter deletion than to plain
historical-version GC.

---

## Compaction Filters

Compaction filters fit this design, but only for whole-key deletion.

They are appropriate for:

1. prefix-based cleanup after a cutoff,
2. policy-driven logical namespace cleanup,
3. deleting the retained boundary version under stronger visibility rules.

They are **not** sufficient to express ordinary MVCC version GC, because
ordinary MVCC GC needs per-key streaming state:

1. keep all versions above cutoff,
2. keep the first version at or below cutoff,
3. drop later older ones.

So the division of responsibility is:

1. scheduler + metadata/stats decide which SSTs to rewrite,
2. compaction core performs ordinary MVCC version GC,
3. compaction filters handle whole-key deletion cases layered on top.

---

## Interaction With vLog GC

MVCC GC and vLog GC operate at different layers.

1. MVCC GC decides which internal-key versions survive SST rewrite.
2. vLog GC later reclaims value-log file space once those surviving SSTs no
   longer reference old pointer-bearing versions.

This means:

1. a standalone MVCC-GC scheduler may create reclaim opportunities for vLog GC
   even when ordinary compaction is idle,
2. but MVCC GC does not reclaim vLog bytes directly,
3. vLog GC still needs its own liveness and file-rewrite logic afterward.

The retained below-cutoff boundary version is especially important here: it may
still carry the live `ValuePointer` that prevents vLog reclamation. TTL or
compaction-filter deletion can remove that boundary version under stronger
safety gates, which can unlock later vLog GC reclaim.

---

## Why This Matches TiKV Better

This design matches the useful part of the TiKV model:

1. standalone GC scheduling,
2. safepoint/watermark-based cutoff,
3. metadata/stats-driven manual compaction triggers,
4. compaction as the execution mechanism,
5. optional compaction-filter-style acceleration for whole-key deletion.

It does **not** require copying TiKV's full distributed machinery. It only
adopts the architectural split between:

1. GC scheduling/policy
2. compaction-time physical execution

---

## Testing Implications

The existing compaction tests remain relevant for execution correctness:

1. dropping old versions,
2. preserving versions for active readers,
3. bottom-level-only tombstone deletion,
4. range-tombstone cleanup semantics.

But the standalone scheduler design adds new tests that should exist:

1. GC scheduler triggers compaction even when ordinary compaction is idle,
2. `max_ts`/TTL metadata candidate picking selects expected SSTs,
3. ordinary compaction and GC compaction coexist without conflicting picks,
4. TTL-heavy SSTs are selected for GC compaction without needing size-pressure,
5. GC compaction improves vLog reclaim opportunity by removing old
   pointer-bearing versions.

---

## Conclusion

MVCC GC should not be defined as “whatever compaction happens to clean up.”

The current design is:

1. a standalone scheduler computes a cutoff,
2. metadata/stats pick GC-worthy SSTs,
3. targeted compaction executes the rewrite,
4. existing MVCC/tombstone/range/TTL rules determine what is actually deleted.

That keeps the engine’s safety logic where it already belongs, while giving GC
its own scheduling and policy surface instead of tying reclaim quality to
ordinary compaction luck.
