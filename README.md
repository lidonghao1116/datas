# Base Analytics

Base 链实时交易与聪明钱分析系统。当前代码实现第一阶段的原始事实数据链路：

```text
Base HTTP RPC / WSS
  → 顺序区块采集与缺口补齐
  → 区块与交易回执
  → Redpanda base.block.raw.v1
  → ClickHouse base.raw_blocks/raw_transactions/raw_receipts/raw_logs
```

PostgreSQL 保存采集 checkpoint，使采集器重启后能从上次成功发布的区块继续。

## 运行机制

- 优先调用 `eth_getBlockReceipts`；RPC 不支持时，降级为批量调用
  `eth_getTransactionReceipt`。
- 公共 Base RPC 每批最多请求 10 笔回执。客户端会限制 batch 大小、控制请求
  速率，并对限流错误做有限退避重试。
- `RPC_REQUEST_TIMEOUT` 作用于每次 RPC 请求，而不是整个区块的全部回执处理。
- WSS 不可用或返回 `405 Method Not Allowed` 时，采集器自动使用 HTTP 轮询。
- 区块成功发布到 Redpanda 后才推进 PostgreSQL checkpoint。
- ClickHouse 写入成功后才提交 Kafka offset，因此链路支持 at-least-once 重放。
- 每个区块的 `parent_hash` 会与 PostgreSQL canonical header 历史核对。发生短分叉时，
  采集器寻找共同祖先、发布带重组元数据的新分支首块，并从共同祖先后重新采集。
- 原始事实表和 ERC20/DEX 标准化表会按孤块 hash 将 `is_canonical` 修正为 `0`；
  重组记录保存在 ClickHouse `chain_reorganizations`。

## 目录

```text
cmd/block-ingestor       Base 区块和 Receipt 采集
cmd/block-writer         Redpanda 到 ClickHouse
internal/chain/base      Base JSON-RPC/WSS 客户端
internal/ingest          顺序采集、补块和 checkpoint
internal/messaging       Redpanda producer
internal/storage         ClickHouse 写入
internal/checkpoint      PostgreSQL checkpoint
migrations               PostgreSQL/ClickHouse 表结构
```

## 本地启动

### 1. 准备配置

复制环境变量模板：

```powershell
Copy-Item .env.example .env
```

默认使用 Base 公共 RPC。生产或历史回补场景建议在 `.env` 中配置支持更高请求
额度的 HTTP/WSS RPC。

### 2. 启动基础设施

```powershell
docker compose up -d redpanda redpanda-console clickhouse postgres redis minio
```

### 3. 执行迁移

PostgreSQL 和 ClickHouse 首次创建数据卷时会执行初始化脚本。仍建议显式运行一次
幂等迁移，以验证两个数据库均已就绪：

```powershell
docker compose run --rm migrate
```

ClickHouse 迁移采用“一文件一条 DDL”，并显式写入 `base` 数据库。任何 PostgreSQL
或 ClickHouse 迁移失败都会使命令返回非零状态。

### 4. 启动数据管线

```powershell
docker compose up -d --build block-writer block-ingestor
```

启动应用服务时，`redpanda-init` 会自动创建 `base.block.raw.v1` topic：

- 3 个 partition；
- 1 个 replica，适合本地单节点环境；
- 单条消息最大 16 MiB。

### 5. 查看状态和日志

```powershell
docker compose ps
docker compose logs -f block-ingestor block-writer
```

正常日志会依次出现：

```text
block published
block persisted
```

### 6. 验证数据

```powershell
Invoke-RestMethod "http://localhost:8123/?database=base&query=SELECT%20count()%20FROM%20raw_blocks"
```

查看最新已落库区块：

```powershell
Invoke-RestMethod "http://localhost:8123/?database=base&query=SELECT%20max(block_number)%20FROM%20raw_blocks"
```

查看 PostgreSQL checkpoint：

```powershell
docker exec base-analytics-postgres-1 psql -U base -d base -c `
  "SELECT pipeline, chain_id, block_number, updated_at FROM ingestion_checkpoints"
```

Redpanda Console：

```text
http://localhost:8082
```

## 配置说明

- `START_BLOCK=0`：没有 checkpoint 时从当前最新区块开始。
- 设置具体 `START_BLOCK`：从指定高度开始历史回补。
- `RPC_REQUEST_TIMEOUT=15s`：单次 HTTP RPC 调用的超时。
- `RPC_RECONNECT_DELAY=3s`：RPC、订阅或区块处理失败后的重试间隔。
- `REORG_MAX_DEPTH=128`：允许自动寻找共同祖先的最大深度；超过限制时停止推进
  checkpoint，避免把无法验证的分支写成 canonical。
- `BASE_WSS_URL` 不可用时不会中断采集，服务会回退到 HTTP 轮询。
- 所有写入均使用稳定链上键和 `ReplacingMergeTree`，允许 at-least-once 重放。

## 常见问题

### ClickHouse 返回 `Database base does not exist`

确认 ClickHouse 已通过健康检查，然后重新执行迁移：

```powershell
docker compose up -d clickhouse
docker compose run --rm migrate
```

### ClickHouse 返回 `UNKNOWN_TABLE raw_blocks`

说明 `base` 数据库存在但表迁移尚未完成。运行：

```powershell
docker compose run --rm migrate
```

迁移成功后，`base` 中应包含四张表：

```powershell
docker exec base-analytics-clickhouse-1 clickhouse-client --database base `
  --query "SHOW TABLES"
```

### RPC batch 解码失败

旧版本会发送最多 50 个 JSON-RPC 调用，而 Base 公共 RPC 每批最多允许 10 个，
服务端因此返回单个错误对象。当前实现已将 batch 限制为 10。

### `context deadline exceeded`

当前超时按单次 RPC 请求计算。较大区块可以经过多个 batch 完成，不会因为整个区块
处理超过 15 秒而失败。若单次调用仍超时，可在 `.env` 中适当提高：

```dotenv
RPC_REQUEST_TIMEOUT=30s
```

### `MESSAGE_TOO_LARGE`

Redpanda topic、producer 和 consumer 均已配置为支持最大 16 MiB 的原始区块消息。
重新创建或更新应用服务会由 `redpanda-init` 自动应用 topic 配置：

```powershell
docker compose up -d redpanda-init
docker compose up -d --build block-ingestor block-writer
```

### 完全重置本地数据

以下命令会删除 PostgreSQL、ClickHouse、Redpanda、Redis 和 MinIO 的本地数据卷，
仅应在确认数据不需要保留时使用：

```powershell
docker compose down -v
```

## 构建和测试

本机无需安装 Go：

```powershell
docker run --rm -v "${PWD}:/workspace" -w /workspace golang:1.24 go test ./...
docker run --rm -v "${PWD}:/workspace" -w /workspace golang:1.24 go build ./...
```

## 当前边界

本次提交实现原始事实数据管线。下一批代码将增加：

1. ERC-20 Transfer 标准化解析器；
2. DEX Decoder 注册机制；
3. Aerodrome、Uniswap V2/V3 Swap；
4. parent hash 重组检测与 canonical 修正；
5. Prometheus/OpenTelemetry 指标。
