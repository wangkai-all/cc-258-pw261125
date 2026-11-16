# 跨来源分页查询：三种“口径”的承诺与代价

两个本地数据服务，一个只有快照读取（A：accounts），一个只有修订号（B：activity）。
查询把两者按 account_id 关联，分页返回。这里**没有全局事务**：A 和 B 之间不存在共享事务边界，
任何“一致”都只能是在一个**可重读的读对**（A 快照 token + B 修订号）意义上的、可审计的一致。

页面：`http://127.0.0.1:8080/`。它显示每个来源实际读取的版本、已输出范围、失败位置，
可以先把来源更新/推进修订，再选择重试口径，并校验结果究竟来自哪一组版本。
取消、恢复、缓存命中都走同一条承诺，不允许“悄悄换版本”。

## 快速开始

```bash
npm install
npm start                 # A:4100  B:4101  查询页:8080（数据在 ./data）
```

```bash
npm test                  # 34 个契约测试：来源、协调器、HTTP 取消/恢复/缓存
npm run compare           # 同一故障下 pin / restart / mixed 的实测对比
npm run test:e2e          # Playwright 驱动真实页面（需要 Chromium）
```

## 三种口径

| 口径 | 承诺 | 代价 | 何时使用 |
| --- | --- | --- | --- |
| `pin` 固定来源版本 | 已输出的每个分页都来自同一组 (A 快照, B 修订号)。失败停在失败位置，不跳号、不重复、不换版本 | B 修订必须留在保留窗口内；版本过期后查询无法前进，只能选 restart 或 mixed | 需要一份可复现、可校验的结果 |
| `restart` 整次重试 | 最终结果只来自一组版本 | 重试作废此前所有已输出分页，从第 1 页重来，成本随规模线性增长 | 消费方能丢弃已收到的分页 |
| `mixed` 显式不一致 | 不承诺单一版本；每页自报版本，跨版本漂移逐页记录并在结果中汇总 | 前后分页可能来自不同时刻，跨行比较会漂移，消费方必须接受 | 前缀价值高、且必须继续前进 |

三者都不能把两个来源放进同一事务。选择不是“哪个更正确”，而是**愿意付哪种代价**。
`npm run compare` 会用真实来源复现同一故障并输出实测差异：

```
策略/状态          查询能否继续   最终状态   交付页数  交付行数  作废页数  版本段  不同版本组  校验结论
pin（版本仍可读）  是             completed  6/6       240       0         1       1           verified
pin（修订已过期）  否             failed     2/6       80        0         1       1           —
restart            是             completed  6/6       240       2         1       1           verified
mixed              是             completed  6/6       240       0         2       2           verified_mixed
```

## 关键机制

**读对绑定。** 新建运行先探测 A 的最新快照与 B 的最新修订，写入 `binding`（含 `binding_digest`）。
之后每页都带着这组版本读取；每页的 `versions` 记录 A token 与 B revision，页摘要（`page_digest`）
可由页体单独重算，因此“这页来自哪组版本”不是页面自述，而是可校验的。

**失败位置。** 一页要么整页提交（页体 + 游标 + 缓存 + 运行状态在同一 `BEGIN IMMEDIATE` 事务里），
要么整页丢弃。失败时持久化 `failure`：页号、阶段、来源、失败前的游标、已输出页数。
失败绝不推进游标，也不会留下半页。

**重试前重新校验。** `pin` 重试会实时探测两个固定版本：仍可读且内容摘要未变才继续；
B 修订掉出保留窗口或 A 快照被清理时拒绝（`pinned_version_unavailable`），并记录 `version_loss`
（哪个来源、丢了哪个版本、floor 是多少）。拒绝本身不改变已输出前缀。

**口径变化必须显式。** 已有输出时，`restart` 需要 `acknowledge_invalidation`，明确列出将被作废的页范围；
`mixed` 需要 `acknowledge_mixed`，并只重新绑定失败/丢失的那个来源，在下一页写入 `version_boundary` 警告。
两者都会在 API 返回里给出 `contract_change`，页面也会提示“此变化对消费方可见”。

**取消与恢复。** 取消排在正在进行的页读取之后，在页边界生效；已提交的页保持不变。
恢复只允许从 `cancelled` 状态，并且必须先验证两个固定版本仍可重读；否则拒绝而不是静默换版本。

**缓存。** 键 = 完整 binding 摘要 + 口径 + 页码 + 游标。命中前还会校验版本信封与页摘要；
不匹配的条目会被删除并计入 `mismatch_rejections`，绝不把别的版本组的数据拼进来。

**校验分两层。** `verify` 把“已交付结果是否自洽”（integrity，可离线从页体重算）与
“来源现在是否还保留这些版本”（availability）分开。历史结果不会因为来源后来清理了旧版本
而被判为错误；页面会分别显示这两组结论。

## 目录

```
src/sources/          来源 A（快照）与 B（修订号），含故障注入与读取日志
src/coordinator/      协调器、SQLite 持久化、REST API
src/web/              页面（原生 HTML/CSS/JS）
scripts/dev.js        A + B + 查询页
scripts/compare.js    三种口径的实测对比
test/                 契约测试（source / coordinator / HTTP）与 Playwright 端到端
```

## HTTP 接口（页面同源）

```
GET    /api/health                    承诺、口径、上限
GET    /api/runs                      最近运行 + 缓存统计
POST   /api/runs                      {policy, filters, limit, join}
GET    /api/runs/:id                  描述：版本、已输出范围、失败位置、分页、事件
POST   /api/runs/:id/step             {pages}   按页读取
POST   /api/runs/:id/retry            {policy, acknowledge_invalidation?, acknowledge_mixed?}
POST   /api/runs/:id/retry-guidance   重新探测固定版本，仅刷新重试建议，不改运行
POST   /api/runs/:id/resume           仅 cancelled，且版本仍可重读
POST   /api/runs/:id/cancel           {reason}
GET    /api/runs/:id/verify           integrity + availability 两组检查
GET    /api/runs/:id/pages/:pageNo    当前 epoch 的页体
GET    /api/sources/:a|b/admin/state  来源版本、快照/修订、读取记录
POST   /api/sources/:a|b/admin/:act   update / snapshot / bump / settings / faults / reset / prune_snapshots
```

来源本身：`/v1/snapshot[/:token]`、`/v1/accounts?snapshot=&after=&limit=`（A）；
`/v1/revision[/:rev]`、`/v1/activity?revision=&ids=`（B，未知修订 404，过期 410）。

## 端到端测试的浏览器依赖

Playwright 的 Chromium 需要系统库。若容器里没有，可在不安装系统包的情况下解出 Debian 包：

```bash
mkdir -p /tmp/deps /tmp/rootfs
cd /tmp/deps
apt-get download libglib2.0-0 libnss3 libnspr4 libdbus-1-3 libatk1.0-0 \
  libatk-bridge2.0-0 libatspi2.0-0 libx11-6 libx11-xcb1 libxcomposite1 libxdamage1 \
  libxext6 libxfixes3 libxrandr2 libgbm1 libxcb1 libxkbcommon0 libasound2t64 \
  libpango-1.0-0 libcairo2 libcups2 libdrm2 libexpat1 libxrender1 libxi6 libxtst6 \
  libfontconfig1 libfreetype6 libpng16-16 libwayland-client0 libwayland-server0 \
  libxshmfence1 libxau6 libxdmcp6 fonts-dejavu-core fonts-noto-cjk 2>/dev/null || true
for f in *.deb; do dpkg-deb -x "$f" /tmp/rootfs; done
printf '<fontconfig><dir>/tmp/rootfs/usr/share/fonts</dir></fontconfig>\n' > /tmp/fonts.conf

npx playwright install chromium
LD_LIBRARY_PATH=/tmp/rootfs/usr/lib/x86_64-linux-gnu FONTCONFIG_FILE=/tmp/fonts.conf npm run test:e2e
```

浏览器无法启动时，e2e 测试会 skip（而不是伪造通过）；其余契约测试不依赖浏览器。

## 已知边界

- “一致”是读对意义上的：A 快照创建与 B 修订提交之间仍有时间差，页面显示两者的 `created_at`。
- `pin` 的可继续性取决于 B 的保留窗口（默认 10 个修订）；这是来源本身的能力约束，不是协调器能消除的。
- 结果不承诺跨来源的因果一致性，只承诺“每个分页来自哪组版本”可被独立校验。
