# V7/V8 Point 生命周期 CPU 归因：GCP Profile 诊断

日期：2026-11-17 UTC
源码提交：3b13b453e06e81c34acb7347370cf8a2634927c2

## 结论

在这次带 Profile 的单轮对照中，V8 的 Point 成本差距随并发增加：1 writer 时 V8 吞吐较高；4、8、16 writers 时逐渐落后，16 writers 的诊断吞吐低 15.5%，进程 CPU/Put 高 39.4%。这是诊断运行，不是无插桩性能成绩，也没有重复运行，不能作为稳定性能估计。

剖析没有发现单个 MVCC 函数能够解释差距。V8 在 16 writers 时进入 MVCC Out-of-order 路径的比例较高，但该路径自身的 CPU p50 并不比 V7 高。V8 的 Durability Driver CPU 随 writer 数增长；在 16 writers 时，资源回收的采样 CPU p50 合计约 42.7 μs/advance。同期 fdatasync 的采样 p50 仅 1.78 μs、p95 为 114.34 μs，说明本轮 Direct FRONTIER + Lookahead 的同步中位数已很低，但仍有长尾。

最明显的用户态 Profile 信号是 V8 的 RawMutex 慢路径占比高于 V7；该符号覆盖多个锁，当前 Profile 不能把它归到具体 WAL 或 MVCC 锁。Point Lookahead 后台初始化共写零约 397 MiB（16 writers），但没有单独采集准备线程的累计 Thread CPU（Hotpath 只有采样 CPU% 快照），因此进程 CPU/Put 的增量不能全归因到 Driver、Retirement 或 MVCC。

## 实验身份与验证

- GCP：C4，16 vCPU、8 个物理核心、每核 2 个硬件线程；Intel Xeon 6985P-C；Ubuntu 24.04，内核 7.0.0-1017-gcp。
- 存储：两块 Local SSD 组成的 ext4 RAID0，挂载于 /mnt/gcp-wal-bench；挂载选项 rw,noatime,stripe=256。
- 负载：V7 Parallel 对 V8 Parallel；Point Put，1 KiB value，每次 100,000 puts；writer 数 1、4、8、16，各配置一轮。
- V8：Direct FRONTIER、1 MiB sliding-point-lookahead、Dynamic coalescing、Point frontier credit batching 关闭；DATA 继续使用 O_DIRECT 和 io_uring。Lookahead 配置由结果中的 WAL I/O 起止快照验证，且每次运行都有初始化进度。
- 诊断：perf record 199 Hz、DWARF call graph；HOTPATH_TIME_SAMPLING_RATE=0.1；Put latency 每 100 次采样；MVCC Publication 每 64 次采样；V8 Durability 每 256 次（1 writer）或每 64 次（4/8/16 writers）采样。
- 每个 run 的 MVCC 样本为 1,562 条，未丢样；V8 Durability 样本分别为 390、394、204、158 条，均未丢样；8/8 perf captures 成功。
- 源码提交：3b13b453e06e81c34acb7347370cf8a2634927c2；write-perf SHA-256：9cf7d9bf1f6482cf2929ba5775a1e78ea8512632366fce0821303b50a65f324a。
- GCP VM bootstrap/orchestration runner SHA-256：5f479b524e03d995905d17b0a3f31fca07882fd308cc7133b4649774c236edf9。完整重跑使用的隔离 Profile Runner SHA-256：b6ca8b366ec66bf4708e06618bf79b9c337eaecde5384ccfa6f4c1ed09788e6d。它只修复汇总 TSV 列定义；重跑直接在保留 VM 上执行，WAL 源码和 benchmark binary 未改变。

首次启动在完成一个 1-writer run 后，因汇总 TSV 缺少新元数据字段而退出。该次产物和日志保留但不计入结果。修复 Runner 后，在同一 VM、同一 benchmark binary 上重跑了完整 8 组。Profile summary 报告 complete=true、runs_completed=8；remote-exit 报告 failed=false、runs_started=8。归档内每个计划 run ID 都有成功的 result.json、run-metadata.json、perf.data 和 perf-report.txt。

本地完整归档：bench-results/gcp-point-lifecycle-profile-20261117T154123Z/gcp-point-lifecycle-profile-retry-20261117.tar.gz
归档 SHA-256：54460f3c50caf93f9870401fbd46545930fcd8f80521a39e94895f4adac11c6d
校验 sidecar 与远端 SHA-256 一致。VM、隔离 VPC、子网、SSH 防火墙规则和 Terraform 状态均保留；本轮没有执行云资源清理。

## 吞吐与全进程 CPU

CPU/Put 按进程 user+system CPU 除以 100,000 puts 计算。全部数字来自带 perf、Hotpath 和 WAL Timeline 插桩的单次诊断 run。

| Writers | V7 k puts/s | V8 k puts/s | V8 吞吐变化 | V7 CPU/Put | V8 CPU/Put | V7 / V8 sync | V7 / V8 puts/sync |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 6.08 | 7.01 | +15.3% | 77.3 μs | 68.5 μs | 100,000 / 100,000 | 1.00 / 1.00 |
| 4 | 21.57 | 20.16 | −6.5% | 41.6 μs | 50.0 μs | 25,089 / 25,260 | 3.99 / 3.96 |
| 8 | 33.48 | 29.90 | −10.7% | 43.1 μs | 60.3 μs | 13,033 / 13,115 | 7.67 / 7.62 |
| 16 | 46.29 | 39.11 | −15.5% | 63.5 μs | 88.5 μs | 8,373 / 10,174 | 11.94 / 9.83 |

16 writers 的 sampled Put latency：V7 p50/p95/p99 为 0.312/0.582/0.851 ms；V8 为 0.373/0.822/1.254 ms。每个 run 约有 1,000 个 latency 样本。该尾延迟结果同样只有一轮诊断。

吞吐差距随着 writers 增加而扩大，CPU/Put 也从 4 writers 起显著升高。4 和 8 writers 的 puts/sync 基本相同，16 writers 下 V8 每次 sync 覆盖的 puts 较少。因此，小组形成可能解释 16-writer 差距的一部分，但不能解释 4/8 writers 的全部 CPU 差异。

## V8 Durability 阶段采样

每行是成功 Advance 样本的分位数。Ticket 数量为 p50/p95；其它时间单位为 μs，除特别说明外为 p50。不同阶段的 p50 不能相加成完整关键路径。

| Writers | Tickets/Advance p50/p95 | Driver Wall / CPU p50 | Coalescing CPU p50 | Retirement 前 / 后 CPU p50 | fdatasync Wall p50/p95 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1 / 1 | 51.39 / 23.75 | 0.00 | 2.70 / 2.56 | 0.95 / 1.45 |
| 4 | 4 / 4 | 99.59 / 47.28 | 0.00 | 9.41 / 8.48 | 1.18 / 103.29 |
| 8 | 8 / 8 | 168.76 / 83.12 | 23.71 | 18.07 / 13.25 | 1.28 / 128.94 |
| 16 | 10 / 16 | 241.38 / 116.65 | 28.54 | 24.19 / 18.69 | 1.78 / 114.34 |

16 writers 下，按同一 Advance 样本相加计算的 pre/post retirement CPU p50/p95 为 42.65/62.99 μs；按覆盖 Ticket 归一化后 p50/p95 为 4.19/7.01 μs/Ticket。Driver CPU 的 p50/Advance 为 116.65 μs，约 11.08 μs/Ticket。它们说明回收路径消耗可观，但不证明回收是剩余吞吐差距的唯一来源。

16 writers 的聚合 syscall 计数为：V7 fdatasync 总计 136.6 ms / 8,373 次（均值 16.3 μs）；V8 为 206.6 ms / 10,174 次（均值 20.3 μs）。采样 V8 p50 很低、p95 较长，均值受长尾影响。不要将本次 Profile 的均值和先前未插桩报告直接比较。

Lookahead 在 16 writers run 内初始化 416,284,672 bytes，分 3,176 个 128 KiB chunks；431 次 fallocate。初始化与 fallocate 的累计墙钟计数分别约 259.5 ms 和 17.0 ms，Producer 累计等待约 61.2 ms。这些是跨线程累计值，后台初始化耗时与 Producer 等待存在重叠；当前没有初始化线程的 Thread CPU 计数。

## MVCC 与 perf 热点

MVCC 每 64 个 Commit Timestamp 采一个样本。Out-of-order 占比以及该路径 CPU p50：

| Writers | V7 OOO | V8 OOO | V7 OOO CPU p50 | V8 OOO CPU p50 | V7 / V8 entry distance p50 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.0% | 0.0% | — | — | — |
| 4 | 9.0% | 17.0% | 6.03 μs | 10.72 μs | 1 / 2 |
| 8 | 20.8% | 38.5% | 7.52 μs | 8.32 μs | 2 / 3 |
| 16 | 52.4% | 61.5% | 18.47 μs | 16.07 μs | 4 / 5 |

V8 更频繁地进入 Out-of-order 路径，但在 16 writers 下该路径 CPU p50 反而低于 V7。因而当前数据支持“到达顺序不同”，不支持“V8 的 MVCC Out-of-order 函数本身更慢”。

16 writers 的 perf report 约有 V7 951 个、V8 1,371 个 task-clock 样本：

| 符号 | V7 Children / Self | V8 Children / Self |
| --- | ---: | ---: |
| publish_commit_ts_impl | 18.2% / 17.4% | 14.7% / 14.5% |
| parking_lot RawMutex lock_slow | 10.9% / 3.8% | 15.2% / 5.2% |

RawMutex 是跨模块的通用符号，不能据此确定是哪把锁在争用。采样数量和单轮 Profile 也限制了几个百分点差异的解释力。Admission、frontier advance 和 V8 coordinator acquisition 均可见，但没有一个被本次采样证明为唯一热点。

## 判断与下一步

1. 本轮的 Direct FRONTIER + Point Lookahead 下，fdatasync p50 不再像早期 Fallocate-only 配置那样普遍接近 100–170 μs；但 p95 仍约 100–129 μs。它不是唯一剩余瓶颈。
2. MVCC Out-of-order 到达比例提高，但 16-writer 的单样本路径 CPU 没有更差。暂不改 MVCC sequencer 或 spin/condvar 策略。
3. V8 Driver CPU 与 Retirement 总 CPU 随 group size 增长。此前将 Map 工作移出锁持有区没有提高吞吐，Credit Batching 的增益也只有约 1.29%；当前结果不足以支持再次做大范围 retirement 重构。
4. Lookahead 的初始化线程 CPU 尚未单独测量，是全进程 CPU/Put 对比的主要归因盲点之一。下一次定向 Profile 应补这项 CPU 计数，并将 RawMutex 样本按调用栈归属到具体锁，再决定是否做单变量优化。

本报告只记录 Profile 诊断，不修改或建议变更 WAL 默认配置。