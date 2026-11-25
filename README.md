# 本机 HTTP 对象存储（local-object-store）

为“构建进程反复发布素材包、渲染进程按版本下载（可暂停接收）”这一场景从零实现的本机 HTTP
对象存储。纯 TypeScript + Node.js，无任何运行时第三方依赖，数据全部持久化到本地磁盘，
不依赖外部数据库或对象存储服务。

## 能力对照

| 需求 | 实现方式 |
| --- | --- |
| 固定位置分片，首次全量、后续差量 | 会话中声明 `positions`；差量会话只上传替换位置的新分片 |
| 分片乱序上传、查询确认状态 | `GET 会话` 返回 `confirmed` / `pending` |
| 原样重试成功、已确认字节不可改写 | 内容寻址 + 会话元数据原子确认；重试幂等，不同内容写同一位置返回 409 |
| 传齐才可发布 | 发布时校验所有 `positions` 均已确认，否则 409 |
| 发布版本 = 基版未替换分片 + 本次新分片 | 创建差量会话时**固化基版清单快照**，发布时确定性组装 |
| 其他构建可并发发布 | 按对象/会话分锁；内容寻址 blob 全局去重，互不阻塞 |
| 强制终止后已确认分片可续传 | 所有元数据 `tmp + fsync + rename` 原子写；启动扫描恢复 |
| 当前版本只能是完整旧版或完整新版 | 对象索引单文件原子切换，不存在半成品版本 |
| 重复发布返回第一次的版本标识（即使已回收） | 会话持久化 `publishedVid`，发布接口对已发布会话幂等 |
| 当前版本与未回收旧版可流式下载 | blob 只读流按需拼接，支持 HTTP Range（含跨分片区间） |
| 慢下载暂停期间发布/回收仍能完成 | 下载不持有对象锁；显式租约或连接级隐式租约钉住在用 blob |
| 回收撤下下载入口并释放无主分片与残留 | 按“版本 + 开放会话 + 租约”计算 blob 活集合后删除 |
| 差量会话在基版撤下后仍可续传发布 | 基版快照在会话内自包含；被明确替换的基版分片允许释放 |
| 磁盘预算（128MiB 包替换 1 片，峰值增量 ≤ 20MiB） | 单分片临时文件 + 原子 rename，无整包拷贝；实测约 **8 MiB** |

## 快速开始

```bash
npm install        # 仅安装 tsx / typescript / 类型定义（开发依赖）
npm start          # 默认 127.0.0.1:8080，数据目录 ./data
# 自定义：
npm start -- --port 9000 --host 127.0.0.1 --data /var/lib/objstore
# 或环境变量 PORT / HOST / DATA_DIR
```

健康检查：`GET http://127.0.0.1:8080/health`

## 一条命令运行测试

```bash
npm test
```

17 个用例（含真实 SIGKILL 子进程的故障注入测试）在本机约 **5–7 秒**完成，远低于 80 秒；
使用 node:test 的 TAP 风格可读输出，结尾汇总 `# pass / # fail`。

生成（或重新生成）本地数据夹具：

```bash
npm run fixtures   # 输出 fixtures/demo-package/manifest.json
```

类型检查与编译：

```bash
npx tsc -p tsconfig.json --noEmit   # 仅检查
npm run build                        # 输出到 dist/
```

## HTTP 接口

所有请求/响应体为 JSON（分片上传与下载为原始字节流）。错误统一为
`{ "error": { "code", "message" } }`。

### 1. 创建上传会话

`POST /objects/:name/sessions`

全量（首次发布，上传全部分片）：

```json
{ "mode": "full", "shardSize": 8388608, "shardCount": 16 }
```

差量（指定仍可下载的基版与要替换的位置，只上传这些位置的新内容）：

```json
{
  "mode": "delta",
  "baseVid": "v-2f3a...",
  "shardSize": 8388608,
  "shardCount": 16,
  "positions": [7]
}
```

返回会话视图，含 `sid`、`positions`、`confirmed`、`pending`、`baseVid`。

### 2. 上传分片（可乱序、可重试）

`PUT /sessions/:sid/shards/:position`

- 请求体为分片原始字节；可选头 `X-Shard-Hash: <sha256 十六进制>` 做端到端校验。
- 首次确认返回 `201`；原样重试返回 `200` 且 `idempotent: true`。
- 已确认位置写入不同字节返回 `409`（已确认字节不可改写）。
- 位置不在会话 `positions` 内返回 `400`；会话已发布后上传返回 `409`。

### 3. 查询会话确认状态

`GET /objects/:name/sessions/:sid` → `confirmed` / `pending` / `status` / `publishedVid`

### 4. 发布（传齐才可发布）

`POST /sessions/:sid/publish`

- 成功返回 `201` 与确定性版本标识 `vid`；对已发布会话重复调用返回 `200`、
  `idempotent: true` 和**第一次发布的同一个 vid**（即使该版本后来已被回收）。
- 分片未传齐返回 `409`。

### 5. 对象与版本查询

- `GET /objects`：对象列表与各自当前版本。
- `GET /objects/:name`：全部版本、状态（`current` / `superseded` / `reclaimed`）、
  是否仍可下载。

### 6. 流式下载

- 整包：`GET /objects/:name/versions/:vid/content`
- 单片：`GET /objects/:name/versions/:vid/shards/:position`
- 支持标准 `Range: bytes=start-end`（可跨分片边界），返回 `206` 与 `Content-Range`。
- 当前版本与未回收旧版直接可下；已回收版本只有持有效租约才可继续读（`410`）。

### 7. 下载租约（暂停/慢下载保护）

- 创建：`POST /objects/:name/versions/:vid/leases`， body
  `{ "ttlSeconds": 120 }`（`null` 或省略表示不过期），返回 `lid`。
- 下载时带 `?lease=<lid>`，即使期间版本被回收，blob 也会被保留到租约释放/过期。
- 释放：`DELETE /leases/:lid`。
- 不显式建租约时，每条下载连接自动持有**连接级隐式租约**：连接关闭即释放，
  因此“暂停接收但保持连接”期间发布与回收都能完成且数据不丢，中止连接后空间可回收。

### 8. 回收

`POST /reclaim`，body 可选 `{ "object": "name" }`（省略则回收全部对象）。

行为：把所有 `superseded` 版本标记为 `reclaimed`（撤下下载入口），然后按活集合
删除无引用 blob 与 `tmp/` 残留，返回释放字节数。当前版本永不回收。

## 端到端示例（curl 风格）

```bash
# 1) 全量会话 + 乱序上传 16 片
SID=$(curl -s -XPOST localhost:8080/objects/asset/sessions \
  -H 'content-type: application/json' \
  -d '{"mode":"full","shardSize":8388608,"shardCount":16}' | jq -r .sid)
curl -s -XPUT localhost:8080/sessions/$SID/shards/3 --data-binary @shard3.bin
curl -s -XPUT localhost:8080/sessions/$SID/shards/0 --data-binary @shard0.bin
# ...
V1=$(curl -s -XPOST localhost:8080/sessions/$SID/publish | jq -r .vid)

# 2) 差量会话：基版 V1，只替换第 7 片
SID2=$(curl -s -XPOST localhost:8080/objects/asset/sessions \
  -H 'content-type: application/json' \
  -d "{\"mode\":\"delta\",\"baseVid\":\"$V1\",\"shardSize\":8388608,\"shardCount\":16,\"positions\":[7]}" | jq -r .sid)
curl -s -XPUT localhost:8080/sessions/$SID2/shards/7 --data-binary @shard7-new.bin
V2=$(curl -s -XPOST localhost:8080/sessions/$SID2/publish | jq -r .vid)

# 3) 渲染进程：建租约 → 下载 → 暂停后用 Range 续传
LID=$(curl -s -XPOST localhost:8080/objects/asset/versions/$V1/leases \
  -H 'content-type: application/json' -d '{"ttlSeconds":300}' | jq -r .lid)
curl -s -H 'Range: bytes=0-1048575' \
  "localhost:8080/objects/asset/versions/$V1/content?lease=$LID" -o part0
curl -s -H 'Range: bytes=1048576-' \
  "localhost:8080/objects/asset/versions/$V1/content?lease=$LID" -o part1

# 4) 发布 V2 后回收 V1（租约内的慢下载仍可读完）
curl -s -XPOST localhost:8080/reclaim -H 'content-type: application/json' -d '{"object":"asset"}'
curl -s -XDELETE localhost:8080/leases/$LID
```

## 磁盘布局

```
<data-dir>/
├── blobs/<hash前2位>/<sha256>   # 内容寻址、不可变分片，天然去重
├── sessions/<sid>.json          # 会话：positions、已确认分片、基版快照、publishedVid
├── versions/<vid>.json          # 版本清单：每位置分片的 hash/size 与状态
├── objects/<name>.json          # 对象索引：currentVid 与版本列表（原子切换）
├── leases/<lid>.json            # 显式下载租约
└── tmp/                         # 上传临时文件，rename 发布；启动时清空残留
```

## 崩溃安全与恢复语义

- 分片写入走“临时文件 → fsync → 原子 rename → 会话元数据原子确认”。崩溃在确认前：
  blob 可能成为孤儿（回收清理），会话未确认，客户端重试即可，不产生重复占盘。
- 发布顺序：版本清单落盘 → 对象索引单文件原子切换 → 会话标记已发布。
  启动时扫描已发布会话：清单已存在则信任磁盘，清单缺失才按会话确定性重建
  （vid 仅由对象名与分片内容派生，重放结果恒定）。因此当前版本永远只可能是
  完整旧版或完整新版。
- 故障注入：子进程设置环境变量 `FAULT_CRASH` 为
  `after_blob_write | before_session_update | before_index_swap |
  before_session_publish | after_session_publish`，可在对应写盘点模拟 SIGKILL，
  见 `test/crash.test.ts`。

## 回收活集合规则

一个 blob 被保留，当且仅当它满足以下任一条件：

1. 属于未回收版本（`current` / `superseded`）；
2. 属于已回收但仍被有效租约钉住的版本；
3. 属于开放会话已确认上传的新分片；
4. 属于开放差量会话基版中**未被该会话替换**的分片。

因此差量会话明确要替换的基版分片，在没有其他使用者时即可释放，而会话仍能依靠
固化快照完成续传与发布。

## 测试结构

| 文件 | 覆盖内容 |
| --- | --- |
| `test/core.test.ts` | 乱序上传、确认查询、幂等/不可改写、并发构建、连续版本 |
| `test/delta.test.ts` | 差量组装、基版撤下后续传发布、替换分片释放、重复发布同 vid |
| `test/download-reclaim.test.ts` | 暂停续传、显式/隐式租约、Range、回收安全 |
| `test/crash.test.ts` | 真实 SIGKILL 子进程 + 五个故障注入点的崩溃恢复 |
| `test/disk-budget.test.ts` | 128MiB / 16×8MiB 替换 1 片，`du` 实测峰值增量 ≤ 20 MiB |
| `fixtures/` | 确定性 16 片夹具清单（`npm run fixtures` 再生） |
