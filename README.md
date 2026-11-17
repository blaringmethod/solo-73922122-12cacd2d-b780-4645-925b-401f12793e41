# 科研数据归档与独立见证系统

研究数据归档员交付采样 CSV 后，需要凭**独立第三方回执**发现原件被替换或漏失；
长期保留的批次还要压缩进冷包以节省磁盘。本系统由两个互不信任的服务组成：

| 服务 | 职责 | 持久化 | 端口 |
|---|---|---|---|
| **归档端 archive**（网页） | 按批号上传 ≤ 2 MiB 原件、热件本地保存、固定公钥验签、目录登记、下载、**批次转冷（ZIP 冷包）**、独立核查 | 独立卷 `archive_data`（热件 + 冷包 + 目录 SQLite） | 8000 |
| **见证端 witness** | 独立重算 SHA-256/长度并持久化，签发 Ed25519 回执；同号同文复用、异文拒绝 | 独立卷 `witness_data`（回执 SQLite + 私钥） | 8001 |

见证端持有 Ed25519 **私钥**；归档端只持有配套**公钥**（`keys/witness_ed25519.pub.pem`），
因此归档端无法伪造回执，只能验证。

## 1. 快速启动（Docker Compose）

```bash
# 1) 生成 Ed25519 密钥（仓库已附带一套演示密钥，生产环境务必重新生成）
./scripts/generate-keys.sh

# 2) 构建并启动两个服务（各自独立持久卷）
docker compose up --build -d
docker compose ps          # 两个 healthcheck 均为 healthy

# 3) 浏览器访问
#    归档网页:        http://localhost:8000
#    独立核查页:      http://localhost:8000/verify
#    见证回执清单:    http://localhost:8001/receipts
```

停止与清理：

```bash
docker compose down            # 停止，保留两个持久卷
docker compose down -v         # 同时删除持久卷（清空全部数据）
```

## 2. 核心流程与需求对应

### 上传（`POST /api/batches/{batch_id}/files`，网页首页）
1. 归档端流式读取，超过 **2 MiB 立即 413 拒绝**，本地计算长度与 SHA-256；
2. 原件先以原子替换（`.part` → 目标名）写入本地卷，**绝不覆盖已存在原件**；
3. 转发给见证端；见证端**自己重新**哈希（不信任归档端自报摘要），
   持久化 `批号、文件名、长度、SHA-256` 并签发 Ed25519 回执；
4. 归档端用**固定公钥**验签并核对批号/文件名/长度/摘要全部一致；
5. 验签通过才写入本地目录（状态 `registered`）。
   **本地原件、目录登记、已验签回执三者齐备**才把批次报为 `complete` 并开放下载。

### 同号同文复用、异文拒绝
- 见证端对 `(batch_id, filename)` 建唯一约束：同键同摘要 → 返回 `reused=true`
  的既有回执；同键异摘要/异长度 → **409 content_conflict**，历史记录不可覆盖；
- 归档端对同键异文同样 409，且保留已见证原件不动。

### 断连 / 崩溃后的接续（不重复归档）
- 见证不可达：原件保留在本地，目录记为 `pending`（网页显示“待核”），
  API 返回 **202**；**不会当作“未受理”盲目重投**。
- 见证受理后断连，或本地登记前进程退出（演示用故障注入）：
  重启时归档端同时扫描 ① 目录中的 pending 行 ② 磁盘上无目录记录的“孤儿原件”，
  先向见证端 `GET /batches/{id}` **查明**：有回执则验签后补登（复用，不重传）；
  见证明确 404 才重传本地原件；见证不可达则继续 pending。
- 每个批号一把进程内锁，双窗口并发重试在服务端串行化；
  见证端唯一约束是第二道防线，任何情况下都不会产生重复归档。
- 失败清理只移除临时 `.part` 文件，已见证原件永不被覆盖或删除。

### 独立核查（网页 `/verify`，`GET /api/verify`）
以**见证端已签发清单**为准（不是只扫本地目录），对每个回执逐项：
1. 固定公钥**验签**；
2. 重算本地原件 SHA-256/长度，与回执比对 → 报告 `tampered_original`（篡改）
   或 `missing_original`（缺件）；
3. 检查本地目录是否登记 → 报告 `catalog_missing`（目录漏登）。

见证不可达时接口返回 **503 且 `passed=false`**，绝不报核查通过。

### 下载
仅 `registered` 文件可下载；下载前再次重算摘要并验签，损坏原件返回 409 拒绝。
冷件直接从 ZIP 冷包做**单件随机读取**（无需解包整包），冷包缺失/损坏返回 503 且
不会被残留热件掩盖。切换取件位置**之前**已经开始的热件下载，即使热件随后被删除，
也凭已打开的文件描述符读完完整原字节。

### 批次转冷（网页批次页“转冷”，`POST /api/batches/{batch_id}/cold`）
长期保留的批次压入每批一个 ZIP 冷包（DEFLATE 压缩，支持 `zipfile` 单件读取），
由后台守护线程驱动，慢压缩/删除不阻塞同批上传、下载与状态查询。状态机：

```
queued → compressing → verifying → switching → cleaning → completed
       └→ failed（保留全部热件，可重试；重启自动接续）
```

关键安全规则（均有自测覆盖）：

1. **首次受理固定名单**：受理时把当前所有 `registered` 文件的
   `文件名/长度/SHA-256` 快照进 `cold_jobs.manifest`；有待核（pending）文件的批次
   返回 409，不受理转冷。
2. **后续上传照常接收、保留热件**：转冷后同批新文件仍正常见证登记并以 `hot` 保存，
   绝不会被自动加入或删除。
3. **重复转冷沿用原名单**：对已完成/失败的任务再次受理，只重新排队、保留最初冻结的
   manifest；名单外的后上传热件不动。
4. **先持久化核验、再切换、最后删热件**：冷包以唯一 `.tmp` 构建并原子 `os.replace`
   发布，绝不开半包；发布后逐成员从 ZIP 内重算 SHA-256/长度并核对回执，全通过才在
   **一个事务内**把目录 `storage` 翻为 `cold`（取件位置原子切换）；此后逐个删除热件，
   且删除前再次确认磁盘字节仍等于冻结摘要，**热件清净才报 `completed`**。
5. **写盘/登记失败或途中退出可接续**：任何一步崩溃，重启后都保全可读原件——
   切换前以热件为准重建冷包，切换后以已核验冷包为准继续清理；启动时清理残留 `.tmp`，
   绝不开放半包，也不会误删后来上传的文件。
6. **回执与见证记录不变**：转冷全程不调用见证端，回执、签名与见证清单保持原样。

进度与失败原因可在批次页或 `GET /api/batches/{id}/cold`、`GET /api/cold-jobs` 查看；
冷件/热件位置在批次页与下载链路中标注。

### 转冷后的独立核查（网页 `/verify`，`GET /api/verify`）
仍以**见证端已签发清单**为准，对每个回执逐项重算，且冷热分别处理：

- 冷件：从冷包单件读出重算 SHA-256/长度并验签；
- 热件：从热目录重算；目录漏登的冷件也会在冷包中被找到（报告 `catalog_missing`
  但本地来源标为 cold）；
- **冷包缺失**报 `cold_package_missing`、**冷包损坏/CRC 失败**报
  `cold_package_damaged`、冷包里出现**见证清单之外的成员**报
  `cold_undeclared_member`；这些包级异常独立成立，即使磁盘上还残留同内容热件也照样
  报警，不会被掩盖。


## 3. 故障演示

### 3.1 断连/崩溃接续 + 原件损坏（脚本，需 compose 运行中）

```bash
./scripts/demo.sh
```

脚本自动完成：武装崩溃标记 → 上传（见证受理后归档容器退出）→
等待 Docker 重启策略拉起 → 启动自动接续、批次恢复 complete 且见证只有 1 条记录 →
篡改卷内原件 → 核查页报告 `tampered_original` 且下载被拒。

手动版（网页亦可）：

```bash
# 武装：下一次上传在“见证已受理、本地未登记”处退出
curl -X POST http://localhost:8000/api/admin/arm-crash
curl -X POST http://localhost:8000/api/batches/BATCH-DEMO/files \
     -F filename=readings.csv -F file=@demo-samples/readings.csv
# 连接被切断；docker compose ps 可见 archive 容器被 restart 自动拉起
# 重启后打开 http://localhost:8000/batches/BATCH-DEMO，状态自动恢复 complete

# 模拟见证不可达：停掉见证后上传，网页/接口显示“待核”
docker compose stop witness
curl -X POST http://localhost:8000/api/batches/BATCH-OFFLINE/files \
     -F filename=x.csv -F file=@demo-samples/readings.csv   # 202 pending
curl http://localhost:8000/api/verify                       # 503, passed=false
docker compose start witness
curl -X POST http://localhost:8000/api/reconcile            # 查明并接续
```

### 3.2 原件损坏演示

```bash
docker exec research-archive sh -c \
  "printf 'CORRUPTED' >> /data/originals/BATCH-DEMO/readings.csv"
# 打开 http://localhost:8000/verify 点“开始核查” → 篡改，红色告警
```

### 3.3 批次转冷与崩溃接续演示（脚本/手动）

```bash
BID=BATCH-COLD-$(date +%H%M%S)
# 上传若干已见证原件
for f in a b c; do
  printf '%s,row\n' "$f" > "$f.csv"
  curl -f -X POST http://localhost:8000/api/batches/$BID/files \
       -F filename=$f.csv -F file=@$f.csv
done
# 受理转冷（首次固定名单），随后轮询进度直至 completed
curl -f -X POST http://localhost:8000/api/batches/$BID/cold
watch -n1 "curl -s http://localhost:8000/api/batches/$BID/cold"
# 热件已清净、冷包位于归档卷：/data/cold/$BID.zip；下载仍逐字节一致
curl -f http://localhost:8000/api/batches/$BID/download/a.csv -o a.dl
cmp a.csv a.dl

# 故障注入：让转冷 worker 在崩溃点退出（Docker 自动重启后接续）
curl -f -X POST http://localhost:8000/api/admin/arm-cold-crash
# …受理一个新批次转冷，容器退出；重启后自动接续，热件始终可读、无半包
curl -f -X POST http://localhost:8000/api/admin/clear-cold-crash   # 解除武装
```

> 管理端点 `/api/admin/arm-crash`、`/api/admin/arm-cold-crash`、
> `/api/admin/clear-cold-crash` 仅用于演示/测试，生产应经反向代理屏蔽或加鉴权。


## 4. 本地自测（无需 Docker）

`scripts/self_test.py` 会在临时目录中拉起**真实的 uvicorn 双进程**
（临时目录充当两个独立卷），覆盖 79 项检查：

```bash
pip install -r requirements.txt
python3 scripts/self_test.py
```

覆盖点：健康检查、空清单不误报通过、正常上传与验签下载、幂等复用、异文 409、
>2 MiB 拒绝、同批双窗口并发不重复归档、异批并行各自归档一次（无全局锁）、
见证后崩溃→重启自动接续、见证卷重启后回执与签名完整保持、
见证不可达 pending 且不重投、核查发现篡改/缺件/目录漏登、
伪造回执（他方 Ed25519 私钥）被固定公钥拒绝、见证停服时核查 503 不通过，
以及完整的**批次转冷**：冻结名单、ZIP 冷包单件读取、冷件下载字节一致、
冷包持久化后才删热件、后上传文件保留热件、重复转冷沿用原名单、
慢压缩不阻塞同批查询与异批上传、核查重算冷/热原件、冷包损坏/缺失/未见证成员报警且
不被热件掩盖、目录漏登冷件仍可在冷包找到、转冷途中崩溃→重启接续且无半包。

> 注：CI 沙箱禁用 `kill(2)`，测试进程通过应用内 `POST /__selftest_shutdown`
> 优雅退出；该端点仅用于测试，生产可经反向代理屏蔽。

## 5. 目录结构

```
common/receipt.py          Ed25519 回执载荷、签名、验签（两端共享）
witness/                   独立见证服务（FastAPI + SQLite + 私钥）
archive/                   归档网页（FastAPI + SQLite + 固定公钥 + 原件/冷包存储）
  cold.py                  冷包 ZIP 构建/核验/单件读取 + 后台转冷状态机线程
  templates/               上传/批次/核查三个页面（含转冷进度与冷热位置）
keys/                      Ed25519 密钥对（演示用，生产请重新生成）
scripts/generate-keys.sh   密钥生成
scripts/self_test.py       79 项端到端自测（含转冷全流程与崩溃接续）
scripts/demo.sh            断连接续 + 原件损坏演示
docker-compose.yml         双服务 + 两个独立持久卷 + 健康检查 + 自动重启
Dockerfile                 单镜像双命令
```

## 6. 安全说明与边界

- 回执载荷为确定性 JSON（排序键、紧凑分隔），签名覆盖
  `version/batch_id/filename/length/sha256/issued_at`，并含公钥指纹防错钥；
- 演示密钥随仓库提供仅为开箱即用；真实部署应在受信环境运行
  `generate-keys.sh` 重新生成，私钥只挂载进见证端；
- 当前为单节点 SQLite、HTTP 明文，生产部署应置于 TLS 反向代理之后，
  并按需给见证/归档管理接口加鉴权。
