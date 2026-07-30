# 第十六阶段：链重组与 canonical 修正

## 目标

标准区块采集不再假设同一高度永远只有一个区块。系统使用 Base RPC 返回的
`block_hash` 和 `parent_hash` 维护 canonical header 历史，在短链重组发生时：

1. 检测新块父哈希与本地 canonical 父块不一致；
2. 向后寻找双方最近的共同祖先；
3. 发布携带完整孤块列表的新分支首块；
4. PostgreSQL checkpoint 回退到共同祖先；
5. 从共同祖先后的第一个区块重新顺序采集；
6. ClickHouse 中旧分支事实和标准化事件被标记为非 canonical。

## 数据流

```text
Base RPC
  -> block-ingestor
       -> canonical_block_headers (PostgreSQL)
       -> parent hash mismatch
       -> common ancestor search
       -> RawBlockEnvelope.reorganization
  -> Redpanda
       -> block-writer
            -> raw_* canonical correction
            -> chain_reorganizations audit
       -> event-parser
            -> erc20_transfers / dex_pool_swaps canonical correction
```

两个 Kafka consumer 独立执行 canonical 修正，因此不依赖 block-writer 与
event-parser 的消费先后顺序。修正条件使用孤块 hash，而不是高度范围，消息重放时
不会误伤已经写入的新分支。

## PostgreSQL

`canonical_block_headers` 按 `(pipeline, chain_id, block_number)` 保存采集器已经确认的
canonical 区块头。保存 header 与推进 `ingestion_checkpoints` 在同一个事务内完成。

发生重组后，checkpoint 回退和删除共同祖先之后的旧 header 也在同一个事务内完成。
迁移会用现有 checkpoint 初始化一条 header；升级后才逐块积累更深的可回退历史。

## ClickHouse

以下表按孤块 `block_hash` 同步执行 `is_canonical = 0`：

- `raw_blocks`
- `raw_transactions`
- `raw_receipts`
- `raw_logs`
- `erc20_transfers`
- `dex_pool_swaps`

修正使用同步 mutation，Kafka offset 只有在 mutation 和新分支数据写入成功后才提交。
`chain_reorganizations` 保存共同祖先、旧 head、新分支首块、孤块高度和 hash，支持
运行审计和数据质量告警。

## 配置

```dotenv
REORG_MAX_DEPTH=128
```

如果在限制内找不到共同祖先，采集器返回错误并保持 checkpoint，不会猜测或跳过。
提高该值会增加极端重组时的 RPC 调用数量。

## 验证

```sql
SELECT *
FROM chain_reorganizations
ORDER BY detected_at DESC
LIMIT 20;
```

```sql
SELECT block_number, block_hash, parent_hash, is_canonical
FROM raw_blocks FINAL
WHERE chain_id = 8453
  AND block_number BETWEEN 100 AND 110
ORDER BY block_number, block_hash;
```

业务查询必须继续显式使用 `is_canonical = 1`。估值、钱包快照和告警等下游派生结果
应由 canonical 标准化事件重新计算；本阶段不直接改写已经生成的历史聚合快照。
