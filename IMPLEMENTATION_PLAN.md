# Stock Watch Current Implementation Plan

**Current plan date:** 2026-09-29
**Authoritative local target:** `C:\Users\DIO\.codex\worktrees\171e\astrbot_stock_watch`  
**Scope:** local implementation planning and dated, read-only production evidence. This plan does not authorize further production deployment, reload, credential access, messages, trades, commits, pushes, or Desktop-checkout changes.

This worktree is the single local source baseline. The Desktop checkout and other detached worktrees are not alternate release templates; do not merge or publish from them implicitly. The baseline remains uncommitted and is not a production release. The 2026-09-23 production deployment used a separately verified, four-file, hunk-selected package, not this whole worktree; the 2026-09-24 checkpoint found only `research_selector.py` matched that production byte hash. This is not a fresh production comparison. Obsolete manual-install archives and generated root-level quote artifacts are not part of the source baseline.

## 当前执行计划：前日推荐、盘中确认、区间提示与网页（2026-09-28）

本节是当前执行顺序和状态总表；后文保留截至 2026-09-24 的技术约束与历史记录。旧章节中“尚未安装”“候选数据源”“首次采集未执行”等描述仅代表对应日期，不得覆盖本节已找到的后续证据；旧 Tushare 专用假设不能阻止本节已授权的多源方案设计。未修改运行代码前，不能把计划中的新路由描述为已经接入生产。

### 目标、范围与状态口径

- 最终流程：前一交易日收盘后生成下一交易日候选及理由 → 盘中持续检查并确认推荐 → 给出有条件的参考入场区间、退出目标区间及失效条件 → 网页展示同一条推荐的状态与后续结果。
- 范围：沪市主板、深市主板、创业板 A 股；不含科创板、北交所、B 股、ETF、可转债。旧研究池曾含科创板样本，保留原记录，不把它们纳入新范围或回写历史。
- 短线主评价：实际确认/模拟入场日记 0，之后第 3 个交易日为主，第 1/5 日辅助；20 日仅研究延伸。原有 T+1/3/5/10 推荐复盘接口继续兼容，但须区分推荐日、确认日与模拟入场日。
- 名单匹配率、触发率、可执行率、扣费盈利命中率、实际账户交易胜率分别统计。评估同时保留净收益、平均盈亏、回撤、机会数、费用和资金使用率，不以单一胜率定案。
- `[x]` 已完成且注明证据层级；`[ ]` 缺失/待接入/未验收；`✗` 明确失败及原因。源码存在、本地测试、历史部署记录、当前生产验收是四个不同层级。不报告无依据的整体完成百分比。
- 初版计划更新仅读取本地源码、既有部署记录和任务回报。后续委派的9月28日现场核验及协调审核见下一节，必须区别执行者、检查时间和授权边界；协调方本次仍仅审查本地证据，没有重新访问主机或读取账号文件。

### 当前状态快照与下一步（2026-09-29，协调核对更新）

本节是当前唯一执行快照，覆盖下方历史派发、初审和待交付描述。171e本地schema23整合已按`accepted_local_bounded`接收；独立47项相关测试、14/14源码哈希、四个反例通过。两项初审缺陷已修复：最终滑点成交超限会未成交，历史超限事件保留但无数值估值；新冻结未入场候选进入正确原始分母。生产最近已验收记录为schema20，不据此推断当前线上版本。本轮未访问生产。

| 工作 | 最新状态 | 责任及下一步 |
| --- | --- | --- |
| 既有AKShare/BaoStock及采集器 | [x] 有部署和成功采集记录；[ ] 连续新鲜度、全部字段验收 | 复用已有组件；Tushare 200积分按实测接口能力分工 |
| 本地主链、Web及收盘估值 | [x] 官5本地合成B链复审通过；[ ] A链、真实来源和线上完整展示 | 官5原任务待命，继续作为唯一业务整合写者；四位小数模拟价不代表真实可执行价位 |
| 前瞻协议与核算准备包 | [x] 官4限定窗口/收盘估值准备包通过；[ ] 真实A/B执行、卖出、共享资金核算 | 收盘估值不代表已实现收益 |
| 历史分钟调查 | [x] 有界研究收尾；✗ 231/480日仍unknown | 保留旧结论，不重复同一批量探源 |
| 日线短线策略研究 | [x] 研究报告收尾；✗ 三类方案D3均值为负 | 尚无通过盈利验证的选股策略 |
| 有效价限规则及字段契约 | [x] 官3深交所/上交所规则研究补正收录；[ ] 正式accepted来源场景仍0 | 上交所20份哈希/3测试及482/38段重抽通过；供应商字段、特殊状态、当前时点资格仍unknown；官3待命 |
| 完整分钟来源 | [x] 官4研究包补正复审收录；[ ] 完整来源未通过，一分钟accepted仍0 | 34哈希/10文本响应对及离线复算通过；5分钟partial，三者低价、两者高价与同上游日线有差异，单位unknown。已补全OHLC校验/证据层级，官4收尾待命；审核见桌面minute_source_contract_official4_review_20260929 |
| 发布及真实前瞻 | [ ] 待对应来源、发布包和适用生产授权 | 冻结规则与费用后观察D1/D3/D5，D3为主 |

### 本批执行与后续顺序（2026-09-29）

本批初审历史（已由下方“最终补正复核”覆盖，以下缺陷不再作为当前未修项）：官4实时专项已独立按accepted_with_limitations收录并收尾待命，12哈希/腾讯1523字节/3股09:31快照重算及离线18检查通过（完整1min项明确未过）；仅snapshot_only，应用read后时间不等于网络首字节，单次记录不证明连续新鲜度。官3上交所路线18哈希/3响应/3测试通过，但检查时间早于请求且tests仍pending、误将终端乱码归为DOCX缺口，已发原任务离线重抽全文/核对暂缓条款/补正报告。对应审核均在桌面`.local_records/realtime_minute_source_official4_review_20260929/`、`sse_risk_route_official3_review_20260929/`。官5继续默认拒绝未验来源及Web打包漏依赖修复。本批仍无正式风险或完整1分钟来源放行。

最新收尾：官3、官4均已完成补正复审，按accepted_with_limitations收录研究证据并通知待命；官5本地主链已接收并待命。审核分别见桌面`.local_records/limit_rule_contract_official3_review_20260929/REVIEW.md`与`minute_source_contract_official4_review_20260929/REVIEW.md`。两专项完成不代表来源验收通过，正式风险场景和完整一分钟来源均为0，因此第4项正式来源接入仍受阻。本轮未新派取证或部署。后续应先明确新的可验证来源路线及字段/时点依据；如考虑改用五分钟确认，须另冻策略并重新验证，当前五分钟OHLC差异未解释，不能直接替换一分钟规则。展示改进可独立准备，但不解除正式推荐和盈利验证门。

最终补正复核（覆盖本节早期待补正/待命描述）：官3上交所路线按`accepted_with_limitations`收录；20份长度/哈希、3测试及原DOCX重抽482/38段一致，规则不解锁供应商字段。官5来源默认关闭、ST四字段贯通及Web闭包修复按`accepted_local_bounded`接收；独立400 passed in49.67s、20份长度/哈希、旧库迁移/重启及ST三态反例通过；当前源码新构建Web包28哈希、隔离导入及合成GET200/POST405通过。官3/4/5本批均收尾待命。正式风险来源及完整一分钟accepted仍0，尚无盈利验证通过的策略，本轮未部署。审核见桌面`.local_records/sse_risk_route_official3_review_20260929/COORDINATOR_REVIEW.md`和`source_gate_release_review_official5_review_20260929/REVIEW.md`末节。未来来源启用仍需来源场景版本的持久化和消费验收，不能仅添加许可集合条目。

本次“检查计划，分发任务”（2026-09-29）：已并行复用原官4、官3、官5任务。官4负责当前交易时段的新浪/腾讯等公开实时快照/分钟路由小样本；官3负责上交所规则和沪主板当前风险字段路线；官5负责171e来源门控、记录身份/Web一致性及最小发布审查准备。三者均只写各自约定目录或官5必要的171e业务范围，禁止生产、凭据、部署、长期采集和交易；交付必须回本协调任务独立审核。正式风险场景和完整一分钟来源仍为0。

用户“确认计划，继续计划”后，已向原官3和原官4发送以下两项；不新开任务。官5已确认本地收口并待命。两个取证任务保持各自Sol模型，允许同provider前缀Luna子代理，主任务负责整合。交付回本协调任务审核，回传不等于验收。

| 顺序 | 负责人/状态 | 操作和验收条件 | 依赖及停止条件 |
| --- | --- | --- | --- |
| 1. 本地主链审核 | [x] 协调完成；官5待命 | 47相关测试、14哈希、四独立反例通过；见桌面`.local_records/main_chain_integration_official5_review_20260928/REVIEW.md`和`round2_verification.json` | 仅合成B链和收盘估值，不解除真实来源门 |
| 2. 有效价限规则与字段契约 | [x] 官3上交所路线补正复审收尾，按`accepted_with_limitations`收录；[ ] 供应商字段/当前风险场景验收；正式风险来源未过 | 规则正文、暂缓附件和生效日期已独立核对；后续只补字段契约、单位/参考价/舍入/特殊状态的可验证证据，输出逐场景矩阵 | 不依赖官5；规则研究不能解锁正式风险；过期样本只用于研究 |
| 3. 完整分钟来源 | [x] 官4历史/实时专项收尾；[ ] 完整一分钟来源未过 | 5分钟partial、实时snapshot_only；单次快照不证明连续新鲜度，完整1min仍0 | 不重复既有失败批量探源；需新的可验证来源或另冻规则，不能直接把5分钟替代1分钟 |
| 4. 选择性接入及发布准备 | [x] 官5默认关闭/ST门控/Web依赖闭包本地复审通过；[ ] 真实来源启用、最终发布准备 | 独立400测试、20交付哈希、ST旧库迁移/重启反例及新构建Web隔离包28文件通过；官5待命 | 下一步为来源场景及版本持久证据；生产精确版本/最小差异/回滚和适用部署授权仍缺 |
| 5. 真实前瞻与选股有效性 | [ ] 后续明确派发 | 冻结选股、入场、退出、双边费用、分母与停止条件；采集D-1/D0/D1/3/5，对比直接入场/确认后入场/基准；保留缺失、未成交及亏损 | 来源/执行证据支撑对应结论；还需A链和退出/账户规则，收盘估值不能充当实盘收益 |

官3唯一新产物目录为桌面`.local_records/limit_rule_contract_official3_20260929/`；官4为`.local_records/minute_source_contract_official4_20260929/`。旧交付包只读，不改171e业务源码和主计划；不访问生产/凭据，不登录购买，不启动定时或长期采集，不部署、交易或提交推送。协调维护本主计划并审查返回证据。

生产准备另有一项待处理：官5在任务回报中披露既往工具输出出现连接口令/令牌样式内容，尚无轮换证据。已提醒用户若有效需轮换；本轮未读取、复述或验证凭据，公开数据取证可独立继续。

### 第1、2步现场核验记录（2026-09-28，保留历史时点）

最新来源取证复审：官3`risk_source_acceptance_official3_20260928`按accepted_with_limitations收录，来源验收not_passed。新40/40与旧28/28文件清单、独立5测试通过，13条HTTP台账/10字节响应/3断连复核一致。600032原始报价与Bao收盘一致，只为供应商上界相等；规则正文/生效版本、端点字段定义、特殊状态等仍缺，accepted场景0。实际选样按带交易所前缀标识排序，不是六位数字排序（后者首个000011）；不重选、不扩大预登记证明。审核桌面`.local_records/risk_source_acceptance_official3_review_20260928/REVIEW.md`及verification.json。官3收尾待命，官5主链整合继续，禁止用本包解除真实风险门；当前未另派规则/分钟探源。此条覆盖下方旧“官3正在来源验收”状态。

最新官3风险证据复审补充：原型异常退出已修，7种无效收盘独立返回unknown；晚时点复核仍明确原业务日的历史研究，不产生当前风险资格。28/28清单、14/14原始数据不变、12独立测试及四样本重算通过，按accepted_with_limitations收录。审核见桌面`.local_records/risk_evidence_official3_review_20260928/REVIEW.md`末节及round2_verification.json。官3已通知待命，覆盖此前“正在补风险证据”派发快照；官4也已收尾，官5后续主链整合尚未派发。以上不提升正式风险源或真实分钟验收状态。

执行任务 `01a0e600-d7fd-7c60-8f78-a0084368931b`（cu6sol）在北京时间11:15—12:06形成首轮证据，第二/三轮分别为14:22—14:28和14:45—14:58。根目录为桌面 `.local_records/source_chain_audit_20260928_111500`；协调审核为 `.local_records/source_chain_review_20260928/REVIEW.md`。以下为合并后的最新结论，日期时间仍限定各项证据的适用范围。

- [x] 首轮已核对生产schema20、插件相关哈希、网页实际release/三文件哈希和两处本地版本差异。当时网页三文件与171e一致；其后本地网页已修改而未部署，不再宣称当前源码/生产网页一致。插件仍非整套一致。
- [x] 已现场确认AKShare1.18.97、BaoStock0.9.4在同一独立venv，风险timer有效；9/24风险侧车27条、失败0，批次匹配。不是没有数据源，也不是已证实有两套独立采集服务。
- [x] 已取得BaoStock日线、日历、指数、因子样本。200积分Tushare高权限接口未重复探测，不能编造一次实际权限拒绝。
- ✗ 正式候选链路尚未修复：9/24研究冻结存在，但formal active run为空、网页正式候选0。第三轮已定位当前代码的缓存风险字段缺失→零筛选目标→覆盖率门失败路径，历史逐次内存/版本仍不可还原。
- [x] 网页快照首轮为11:23生成，业务日9/24符合当时最近已完成交易日。研究池只读展示其后已在171e本地实现并验收，生产接入仍未执行。
- ✗ 历史分钟231/480差异仍未解决；6个分层样本BaoStock日线与独立Tushare日线极值一致。AKShare独立分钟一次请求网络断开，不能写成权限拒绝或验证成功。
- [x] 第二轮14:22定向核对未发现旧quick_check标记进程，只证明该检查时点，不能倒推历史退出时间。
- [ ] 完整现场SQLite检查未通过验收；快照自身`integrity=ok`不能替代。生产真实页面渲染、9/28晚间采集均无本计划中的新验收证据；本地合成页面已有单独验证。
- [x] 报告补正已复审，按`accepted_with_limitations`收录；原27份及附录2份manifest条目哈希/长度通过。用户在连接询问后指向本地连接资料，可视为必要认证使用的适用授权，不因缺少“允许读密码”字样判越界；原报告未说清读取方式的问题已披露。协调方未读凭据或重新连接主机。
- [ ] 历史执行证据局限保留：对话有两次inventory调用，第一次原JSON已删除；第二次11:58:44开始后超时，确切退出时间仍unknown。后续无存活进程的时点证据不补造历史记录，此项不作为所有本地开发的前置条件。

27份原证据清单哈希/长度一致；附录复审见协调审核文件末节。原检查计数仍为总20、executed17、pass12、fail4、blocked1、skip3，不把补正文档核验计入生产通过数。步骤1、2均维持`partial`；报告审核完成。上述已核实事实可用于下一步设计，不代表正式推荐或上线获验收。

上述现场核验后的风险证据接入/缓存复用、独立资格事件及模拟成交持久化、网页与复盘关联，已有后续本地修正验收，详见当前快照及官5复审记录，不再列为尚未开始。剩余为契约整合、真实数据门、完整本地链路和生产前瞻验收。分钟数据与策略研究仅阻塞相应有效性结论。18:30侧车不能倒灌18:15原冻结，但可供更晚独立记时/版本的资格判断使用，次日仍须新鲜风险与行情确认。

### 已有多源采集：复用、核验、接线，不从零重建

第三轮根因链核验（2026-09-28，14:45—14:58 CST）：13/13证据哈希/长度通过，8项检查6pass/2blocked，报告按accepted_with_limitations收录。当前生产终态快照走cache-only，raw Quote重建没有三项风险字段且未补全；is_screenable先要求停牌/涨停/跌停显式False，导致指标目标为0、indicator_coverage=0.0，报告门阻断正式写入。9/24日线5557行三项风险全NULL，与此一致。已定位当前实现缺口，尚未修复；不是缺日线或已实测的Tushare权限拒绝。历史逐次内存/版本未记录，18:53 started_at不能当作首次启动时刻。分钟231/480仍未解。后续要补风险证据接入、独立资格版本和分阶段诊断，保留unknown/cache-only/历史不可倒填约束。

第二轮核验更新（2026-09-28，14:22—14:28 CST）：原任务已定位正式冻结缺失的直接门控原因。9/17、18、21—24的自动收盘任务均6次尝试后missed，最终原因码indicator_coverage；生产实际源码在研究池冻结之后、正式_record_screen之前fail_closed。覆盖率具体数值、配置门槛及上游成因尚无完整证据，不能归因于Tushare积分或降低门槛。14:22未见旧quick_check标记进程，历史退出时间及完整数据库检查仍未证；分钟231/480差异未新增解释。第二轮清单8/8哈希/长度一致、检查9项（5通过/2失败/2受阻），报告accepted_with_limitations，系统步骤仍partial。详见桌面审计目录round2_20260928_1411及协调REVIEW.md末节。

并行分工：原任务01a0e600-d7fd-7c60-8f78-a0084368931b负责候选缺失与数据核验；新任务01a0e6b7-9240-7b21-a90f-55428a427690（官3/gpt-6-sol，可用同前缀子代理）负责171e本地网页研究池接线、盘中/区间规则缺口和复盘口径及模拟验收。具体依赖未通过只阻塞对应验收，不阻塞其他本地实现；本地通过不等于生产上线、真实通知或短线盈利。

早期并行包修正复审：初审发现的30秒轮询误清零、同日冲突close静默覆盖两项P1均已修复；最终8/8哈希一致，指定Python313独立重跑72 passed in 14.04s，状态`accepted_local_partial`。本地突破确认按不同已完成分钟bar计数，中间轮询等待、重复bar不累加；冲突/无效混杂close为unknown且不进成熟可评/盈利分母。当时尚缺资格事件与持久模拟入场桥，后续已由官5完成本轮本地关联修正，见最新48项复审；真实完整链路仍未验收。审核证据位于桌面`.local_records/parallel_local_review_20260928/REVIEW.md`，本地接收不等于生产上线或策略有效。

用户确认 Tushare 账户只有 **200 积分**。保留该账户实测可用的接口；权限不足的字段优先评估 **AKShare、BaoStock** 补充或替代，不要求购买更高积分来完成当前目标，也不假设 200 积分能访问高门槛接口。积分不等同于所有接口权限清单，仍按实际返回和字段契约判断。

| 已有组件/来源 | 已有证据 | 当前应该做什么 |
| --- | --- | --- |
| Pi 独立数据环境 `/home/pi/apps/stock-watch-data-probe/.venv` | 2026-09-23 安装并导入 `akshare==1.18.97`、`baostock==0.9.4`；独立于 AstrBot 和网页运行环境 | 复用已有部署，先读版本、最近成功时间和产物；不能把两套已装库写成仍待安装，也不据此猜测还有两个独立服务 |
| `stock-watch-research-risk.service` / `.timer` | 2026-09-24 部署记录：工作日北京时间 18:30—23:30 每小时尝试，按当日冻结批次采集 BaoStock/AKShare 证据 | 核验近期定时执行、日期/批次一致性、失败原因、当前调度与产物新鲜度；不重复建立同用途采集器 |
| `research_risk_evidence/YYYY-MM-DD.json` 与 `research_risk.py` | 27 条回溯证据曾通过批次/日期/收盘价核对并接入观察池显示；冻结的风险状态保持原值 | 从已有侧车文件读取和规范化证据，检查它是否在决策前可用；不能用冻结后采集倒填当时安全 |
| 后续实际定时采集回报 | 任务“审计计划缺口”（`01a0c810-c8e8-7cf2-908c-9c3ab3b6855d`）记录 2026-09-24 18:30 采集 27/27 成功、失败 0；另有 19:02 验收回报 | 已有实际成功采集记录，不能继续仅写“首轮 waiting”；这是历史任务回报，本次未独立读取服务器原件，连续稳定性及今天状态仍待核验 |
| Tushare 日线原始批次 | 既有 raw 数据、覆盖校验、日历及批次记录 | 继续使用确实可用的能力；权限拒绝不等于整项功能不可做 |
| BaoStock 历史研究数据 | 桌面独立研究记录已补取 4,703 只、663 个交易日、3,023,088 行日线 | 复用经校验的原始数据及单位/日期映射；历史研究成功不等于插件日线自动替代已接通 |
| 新浪、腾讯等实时行情 | 2026-09-28 独立影子实验取得 18 股 738 条有效报价；腾讯对两股近时点核对成功 | 保留可用实时来源；AKShare 是访问上游的工具集合，不代表所有接口实时或彼此独立 |
| 网页快照服务 | 既有只读 SQLite 快照与网页部署记录 | 这是展示层复制，不是另一个行情源；不得让插件绕过权限读取网页特权快照来扩展采集 |

来源证据：`tools/verification/SOURCE_PROBE_20260923.md`、`tools/verification/RESEARCH_RISK_RETROSPECTIVE_20260924.md`、`tools/verification/systemd/stock-watch-research-risk.{service,timer}`。以上足以确认 AKShare、BaoStock 已有部署和采集成果；若用户所称“两项目”还指其他独立服务，后续清点准确名称、地址和产物，不能擅自把它们等同于网页服务或新建副本。

### 按数据类型分工的多源路由（待逐项接入验收）

| 数据用途 | 来源分工与权限不足时的替代 | 验收要求 |
| --- | --- | --- |
| 沪深/创业板日线、历史回看、交易日历、历史股票范围 | Tushare 已可用能力继续保留；受限或缺口由 BaoStock 补齐，AKShare 合适接口交叉核对/补充 | 日期、证券类型、退市样本、未复权价格、成交量/额单位、完整性与新鲜度；一个批次内明确逐行来源，不拼接不兼容口径 |
| 每日交易状态、ST、涨跌停证据 | 优先复用已部署的 BaoStock `tradestatus/isST` 和 AKShare 池/相关接口；Tushare 高权限风险接口为可选 | 日频状态不冒充实时临停；命中涨跌停池是正证据，未命中不能自动写为安全；缺字段只标记受影响功能 |
| 除权除息、复权/收益可比性 | Tushare `adj_factor` 不可用时评估 BaoStock 因子/分红和 AKShare 对应资料，必要时用公开原始公告核对 | 有效日、登记/除权/到账/股份可交易日、修订及观察时间；与原始价格交叉核验。生产复盘目前仍有 Tushare 特定依赖，需要适配和测试后才算替代完成 |
| 盘中价格与分钟证据 | 使用已验证的新浪/腾讯路径及可用 AKShare 上游；BaoStock 历史分钟只用于经核对的历史研究 | 来源时间/接收时间、完整 bar、延迟、停牌/限价证据；不可把日线或历史分钟当实时服务 |
| 指数/基准 | Tushare 受限时评估 AKShare/BaoStock 的对应指数数据 | 同日期同收益口径；分红/价格指数差异明示，不用单日市场涨跌代替同期基准 |
| 公告与审计风险 | 原交易所/巨潮资料或有原文依据的适配器；AKShare 可辅助发现 | 报告有效期、发布时间、原文和更正关系；搜索未找到不能推断没有风险 |

替代规则：按 `permission_denied`、限流、网络失败、空响应、过期、字段缺失、口径冲突分别记录；权限拒绝不反复撞同一高权限接口，按固定路由选择可满足字段契约的来源。接口返回成功不等于数据验收通过；切源记录 provider、upstream、business_date、source_time/first_observed_at、basis、units、digest 和质量。AKShare 包装的相同上游不算独立双源验证。所有替代数据先标准化和校验，再进入命名明确的批次/证据适配层，不直接伪装为 Tushare raw。

只有经过替代查询与实际校验仍缺少的具体字段，才列为缺口。不得笼统写“因为 Tushare 200 积分，所以系统无法使用”。缺失/矛盾仍保留 `unknown`，未成熟 `pending`，顺序不明 `unknown_order`，发送不明 `unknown_delivery`；更换供应商不降低这些语义。

### 阶段 0：统一版本与当前部署清点

- [x] 本地版本核对：当前171e源码声明schema22并包含`webapp`；早期schema20是历史本地/最近生产证据，不代表当前本地版本。桌面旧代码曾核为schema14，近期独立研究只写其`.local_records`；不能以同一插件版本号推断字节一致。
- [x] 当前计划保留在本文件，不另建竞争主计划；历史记录和未提交改动保留。
- [x] 9月28日已登记插件/网页/数据采集环境/定时器/侧车的版本、哈希与日期证据；本项仅表示清单核实，审计授权边界补正及完整数据库检查仍见上述未验收项。
- [ ] 有界审查需要移植的代码差异，选定发布文件；不得整目录覆盖或覆盖其他 dirty changes。

### 阶段 1：复用数据源，补齐统一输入

- [x] AKShare、BaoStock 已有 Pi 隔离部署、侧车接入及历史成功采集证据（层级见上表）。
- [x] 日线多源研究和实时行情探针已有数据产物，不重复从零搭环境。
- [ ] 核对已有采集器近期持续产出与新鲜度，逐字段建立“已可用/权限受限/可替代/真正缺失”清单。
- [ ] 接入符合 200 积分条件的多源日线、风险、公司行动和基准适配，补齐来源切换及跨源一致性检查。
- [ ] 整理历史证券范围，统一排除科创板/北交所，保留退市历史样本。
- ✗ 第四轮分钟质量校验：480个候选入场日仍只有249个通过，231个原Bao极值差异保留unknown。官3补取215代码新浪响应覆盖231日，协调方从原始记录独立重算：124日OHLCV对齐、107日价格对齐但缺量；0.01与0.011元容差分类一致。全部新浪柱缺amount且接口复权口径/底层独立性未证，124不算原协议完整通过，不能改写成373个有效样本。47个对齐日争议极值不在09:35，不能统一归为开盘竞价。原始文件246/246哈希通过；审核见桌面.local_records/minute_quality_review_20260928/REVIEW.md。下一轮先补成交额/口径证据及定位107日逐bar缺量。
- [x] 分钟有界补源及逐柱排查已收尾：第二轮8/8清单、231日原始逐柱差额独立复核一致。107缺量日3852柱量差，124总量相同日仍3814柱量差，散布全天；腾讯2次SSL失败、雪球1次400/400016没有新柱。排查报告通过不等于分钟质量通过，上一项231仍unknown。后续需更完整历史来源或符合冻结契约的前瞻数据；低频报价聚合不冒充完整成交记录。本次没有启动新长期采集，cu6sol风险/记录链本地实现继续。

### 阶段 2：前一交易日推荐候选

- [x] 源码已有自动收盘任务、候选冻结、有效期、技术排序、发送队列，以及独立研究观察池/警戒池。
- [x] 已定位当前生产正式冻结缺失的具体代码路径：终态缓存行情风险字段未知→可筛目标0→覆盖率0→fail_closed；历史逐次行为只作有边界解释。
- [x] 风险证据阶段/缓存复用、独立资格版本及持久诊断本轮本地修正已复审通过，属于官5的48项检查覆盖范围。
- [ ] 以充分真实风险证据和自然收盘验收正式候选恢复；普通股票涨跌停阴性证据仍缺，不得放宽风险或指标门槛。
- [ ] 核验连续真实收盘自动完成，候选数据实际截至前一交易日且次日有效。
- [ ] 冻结最终短线方法和价位规则，输出入选原因、次日触发条件、计划版本；缺合格候选可为空。
- ✗ 现有多轮策略未通过稳健性门槛；第四轮 A3/R3/P3/T3 在目标期分别亏 1,878.13/1,942.15/4,623.48/3,028.17 元。旧 C 的小幅盈利在成本/延迟压力下转亏，不能当已证明有效。

### 阶段 3：盘中确认推荐

- [x] 源码已有候选/自选监控、行情新鲜度检查、连续确认、去重/冷却、持久状态和风险失效逻辑。
- [x] 2026-09-28 独立影子探针：18 股、41 次成功请求、738 条有效报价；0 触发、0 模拟买入，进程已停止。只证明该次采集/条件判断，不证明策略无效或有效。
- [x] 本地确认状态机复审：两根不同已完成分钟bar及新鲜现价共同满足突破区间，限制追价并要求eligible风险；默认30秒时序、重复bar、价格/风险失效和长间隔回归通过。未证明生产触发、实际送达或成交。
- [ ] 验收“前日晚间名单自动进入次日盯盘”，捕获自然触发、明确拒绝原因和实际通知送达。
- [ ] 在固定规则下采集多个交易时段的前瞻样本；补录/回看与实时样本隔离。长期采集调度另按实际授权实施，不因本计划自行新增定时任务。

### 阶段 4：参考买卖区间与持仓语义

- [x] `PricePlan` 已有观察区、确认价、目标区、失效价及来源/日期/价位顺序校验；盘中沿用收盘冻结计划。
- [ ] 将“观察区”与“确认后参考入场区”分开，明确追价上限、有效期、跳空放弃和持仓后退出条件；不可直接把旧观察区改名成买入指令。
- [ ] 新选股策略和价位计划使用同一版本规则，验证区间触达/失效及扣费后的实际执行代理。
- [ ] 区分未买入、模拟持仓和用户登记持仓；无买入日期/成本时不得假装知道可卖数量和持仓收益，保留 T+1。

### 阶段 5：接通现有网页

- [x] 171e 网页已有总览、候选、盘中/信号、个股、业绩、健康等页面；历史网页部署记录存在，不等于今天在线验收。
- [x] 本地网页已增加研究观察池/警戒池读取与独立展示，按run_id隔离，保留research_only、时点、过期和资格缺口；合成API/页面及回归有证据，未部署。
- [x] 本地资格判断、持久模拟成交及复盘已按同一记录身份关联网页；后续资格变化不抹去旧成交，官5复审通过。
- [ ] 最终推荐/区间/收益契约统一后，完成真实浏览器与生产同一状态链验收。
- [ ] 显示“前日推荐→待确认→已确认/不可执行→失效/评价中→已完成”，连同参考区间、数据时间、缺口及策略版本。
- [ ] 插件、通知和网页使用同一记录身份/状态，核验当前线上快照刷新和实时产物；数据采集器与网页快照职责分开。

### 阶段 6：复盘与跨交易日验收

- [x] 源码有不可变推荐记录、T+1/3/5/10 评价和 `pending/unknown/unknown_order` 分类；历史实验有账户费用、交易和候选分母记录。
- [x] paper_review.py离线模块已按显式模拟入场日0计算后续1/3/5交易日毛收益与费用扣减收益，未触发/未成交/未知/未成熟保留；同日冲突价格回归通过。仅离线算术，不代表真实成交、账户收益或生产复盘接通。
- [x] 本地持久模拟入场已关联1/3/5日离线复盘及网页，历史成交资格版本保留通过修正复审；不代表真实确认或收益口径最终验收。
- [x] 官4前瞻包v5/protocol0.3执行窗口及持仓收盘估值算术经独立复审接收（30测试、19额外检查、15哈希）。
- [ ] 将经审口径接入主链与网页，明确收盘估值仅扣买入费用，完整卖出成本/已实现收益另有证据要求，不把不同指标混为同一净收益。
- [ ] 新规则在未参与选择的真实前瞻数据上评价，保留未触发、未成交、未知、未成熟及亏损记录。
- ✗ 历史研究任务 `01a0e2ad-e22d-7e73-bbd9-31a9afe48917` 第四轮已生成报告，但最后核对阶段以 `429 Too Many Requests` 结束；需要补收尾核验，不能只因报告存在就标完成。
- [ ] 一次完整真实链路验收：收盘自动冻结→次日确认→区间提示→通知/网页一致→1/3/5日成熟复盘，再按部署范围完成新鲜上线验收。

### 执行顺序与已有研究证据

#### 最新审核结论（2026-09-28）

- 官4最新复审：manifest v5/protocol v0.3.0的窗口与收盘估值算术按`accepted_local_bounded`接收。独立30测试、19额外检查、15/15哈希及文件集合通过，正负CLI与保存结果一致，原五反例及B14:59/错误收益均被拒绝。限定单仓mark_to_close、扣买入费用，不代表扣卖出费用后的净收益、已实现交易、共享账户、完整来源或生产验收。官4已通知收尾待命，后续官5主链适配尚未派发。协调证据桌面`.local_records/forward_capture_official4_review_20260928/REVIEW.md`末节及round5_verification.json。

- 官4历史v4审核及后续派发（已由上条v5验收覆盖）：`round3_fixes_accepted / preparation_partial`。14/14哈希与独立11测试通过，上一轮5反例全部正确拒绝，真实柱/确认/收益证据关联和固定100股本轮修复认可。完整协议仍缺：B 14:59成交绕过14:57截止，任意有限return_pct不与成交/收盘/费用核算；因此不能作为完整A/B执行或收益验证器。已精确清除唯一生成pyc。用户最新授权“继续扔给官4”，已发原任务集中补齐执行窗口及收益核算：冻结协议作为窗口规则源，校验上海时区/D0/截止边界；区分收盘估值与模拟已实现收益，明确价格、费用、滑点、公司行动及舍入公式，用独立手算预期验证，证据不足保持pending/unknown。仅写原准备包目录，完成回传复审，不扩大为共享账户优化；暂不接主链，不采集/部署。现有审核证据见桌面`.local_records/forward_capture_official4_review_20260928/REVIEW.md`末节及round4_verification.json，新修正尚未验收。

- **最新模型授权（覆盖下文历史cu6sol称呼）**：用户指定原第1/2项任务`01a0e600-d7fd-7c60-8f78-a0084368931b`改为官5 `codey-official-account-f0bfed72-ec23-4487-849d-2029839fee99/gpt-6-sol`；允许按需使用同前缀`gpt-6-luna`子代理。仍复用原任务，主任务唯一整合写入171e，子代理职责/写入互斥，不跨账号静默替换。当前已接受修正包收尾状态保留，此次仅更新模型和子代理权限，不新增部署、采集或探源任务。

- [x] 第1/2项两处P1修正复审通过，按`accepted_local_partial`收录：23项最终哈希/17份已存基线一致，协调独立48项测试通过；新增资格版本保留历史模拟成交及复盘，近3小时前旧柱配新报价不再触发。当前资格与成交绑定资格分开展示，分钟年龄/时段/下一交易日门已加。真实涨跌停阴性来源和完整分钟质量仍缺，第1/2项总体partial，未上线。详见桌面`.local_records/risk_qualification_bridge_review_20260928/REVIEW.md`末节。
- 历史初审（已被后续复审覆盖，不是当前未修清单）：官4本地准备包初审`changes_requested`，13/13哈希、已有2测试通过，但独立反例证明缺A记录误算未触发、B无确认也可成交、未知状态统计丢失、无关联/时间倒序执行未拒绝，任意hash/科创板/缺字段分钟柱可通过，完全相同重放反而失败。后续修复状态见上方v4记录。证据桌面`.local_records/forward_capture_official4_review_20260928/REVIEW.md`。

- 第1/2项本地交付已收到，协调初审`changes_requested`并打回原任务`01a0e600-d7fd-7c60-8f78-a0084368931b`。23/23最终文件和17份已存基线哈希通过，独立重跑41项相关测试通过；新增合成反例复现两项P1：新资格版本导致旧模拟成交从网页/复盘隐藏；近3小时前旧分钟柱配新报价生成当前模拟成交。修正后再验收，真实风险阴性来源/分钟质量仍未解决。审核证据：桌面`.local_records/risk_qualification_bridge_review_20260928/REVIEW.md`。官4本地前瞻准备可继续并行，不碰业务代码。

- [ ] 用户已授权新建官4任务`01a0e729-9d61-7db2-8b6a-e57d1d5f895e`，模型`codey-official-account-6e2f4a32-12cb-4c16-a140-b5fd9335e1e3/gpt-6-sol`，负责前瞻采集与评价协议的本地准备。交付字段级复用清单、冻结协议、同候选直接入场/盘中确认配对评价契约、本地校验与台账脚本及验证报告；只写桌面`.local_records/forward_capture_official4_20260928`，171e与现有采集代码只读，不重复部署采集服务。此任务不启动长期采集、不操作生产，不与cu6sol业务写入竞争；回传后由协调方审核。

- [x] 官3日线研究v3修正复审完成，按`accepted_with_limitations`收录。32/32产物、4703原件+5索引哈希和1630笔费用/现金检查通过；候选逐字节未变，恰6个ST卖出标签改unknown_order，独立重算1/3/5日计数/均值/胜率一致，16账户及交易/日资产未变。开盘与盘后质量拆分验证通过，本批次对应质量冲突0。详见桌面`.local_records/daily_shortline_official3_review_20260928/REVIEW.md`末节。
- ✗ 本次三类短线盈利方案未通过：D3成熟候选平均净收益为突破-1.31%、缩量回踩-0.07%、超跌-0.56%；已见数据修订、公司行动现金流缺口及归档开盘执行未知仍在，不能按已验证盈利策略投放。研究任务完成不等于阶段2最终策略已冻结或阶段6前瞻验收完成；官3本包收尾，不自动追加调参/采集。

- 官3日线研究已提交，协调初审为`changes_requested`并打回原任务：22/22产物、4703原件+5索引哈希及1630笔费用/现金等式复核通过，但主板ST持仓仍套10%跌停代理，导致6个标签把约5%跌停开盘计为可卖，需修成熟分母和收益；另需分离开盘条件与当日收盘后质量信息。账户已成交暂未检出同类跌停卖出，不宣称所有账户数值因此错误。初报三族亏损尚不作最终策略验收；v2为见v1结果后的缩量条件纠正，历史全为探索。证据：桌面`.local_records/daily_shortline_official3_review_20260928/REVIEW.md`。

- [x] 原cu6sol、现官5任务 `01a0e600-d7fd-7c60-8f78-a0084368931b` 本轮风险证据/缓存、资格事件、持久模拟成交与网页复盘修正已协调复审接收；当前idle，后续继续作为171e唯一业务整合写者，不代表真实数据与生产门已通过。
- [x] 官3原任务 `01a0e6b7-9240-7b21-a90f-55428a427690` 的三类日线短线研究已复审收尾，当前idle。盈利有效性未通过；所用历史日期仅作探索，不能重新标为盲测。当前没有新调参或探源派发。
- 官3新增产物仅写桌面 `.local_records/daily_shortline_official3_20260928`，原始数据和171e代码只读；不依赖分钟质量通过，不改主代码或启动生产采集。交付冻结协议、可重跑脚本、数据哈希、结果/交易明细和报告后独立审核。派发不等于完成或策略通过。
- 分钟两轮有界排查已收尾，231个异常日仍unknown；未来补源或前瞻采集准备为后续独立事项，本次没有再次派发或启动长期采集。

以下五项保留原工作编号，按最新审核拆分“已完成本地实现”与“尚缺验收”。官5本轮本地修正已完成待命；官3旧分钟/日线研究收尾，新的风险证据补齐已派发；官4v5窗口/估值核算本轮已接收收尾。后续主链整合仍由官5唯一写入，尚未另行派发。未完成项不等于已有任务自动推进，生产部署仍未授权。

1. [ ] **P0：正式候选真实风险证据验收（本地修正已完成）。** 缓存证据复用、资格版本及诊断已通过本轮本地审核；剩余为普通股票停牌/涨跌停的充分真实证据、来源日期/时点/批次、新鲜度及自然收盘正式链路。现有27股侧车和涨跌停池未命中不证明全市场安全，不得把unknown写False。
2. [ ] **P0：统一资格→确认→模拟成交→复盘口径（本地记录关联已完成）。** 本轮版本化资格、持久模拟入场、Web及离线复盘关联已接收；剩余为官4窗口/收益契约审核通过后的主链适配、估值与已实现收益分栏、真实数据确认和完整验收。保留重启/幂等/历史成交，真实用户持仓仍需独立语义。
3. [ ] **并行：数据质量与策略有效性（本轮调查/研究均已收尾）。** 分钟231日仍unknown，需要新的完整来源/合格前瞻证据，停止重复已失败接口；三族日线方案未通过盈利门槛，不在已见样本无限试参。后续研究须重新预登记、固定费用/执行/指标，在未参与选规则的数据上评价，主看D3、辅看D1/D5。此项没有新派发，网页与确认工程通过不证明盈利。
4. [ ] **P1：完整本地贯通及发布包审核。** 自动冻结→新资格事件→实际确认状态机→持久模拟fill→网页状态→按交易日成熟复盘，覆盖空候选、拒绝、未知、冲突、重启和重复事件。使用脱敏/合成数据时明确标记。按本次变更前字节记录差异，核对生产版本，只准备必要文件及回滚方案，不能整套覆盖171e或桌面目录。
5. [ ] **最终：部署与真实前瞻验收。** 本地及发布检查通过且有适用部署授权后，按AstrBot操作要求部署/插件级reload并验证网页和记录一致。经历真实收盘及后续交易日，保留未触发/未成交/unknown/pending，待1/3/5日成熟后评价；未达到策略门槛继续展示为研究或模拟，不宣称可盈利推荐。

历史数据库超时的退出时刻不可还原，不再反复检查已知状态来替代以上实现。数据源不重复搭建；新一批跨main/storage/provider公共文件的变更应指定同一主写入者，其他任务使用互斥文件或只读研究。

桌面研究根目录：`C:\Users\DIO\Desktop\astrbot插件项目\astrbot_stock_watch\.local_records`。`independent_daily_20260927`、`optimization_daily_20260927_v2`、`optimization_daily_20260928_v3`、`shortline_20260928_v4` 为历史研究；`live_shadow_20260928_095657` 为独立实时探针。报告、源码和结果仅作各自层级证据，不直接作为生产发布模板；旧 schema 14 代码不得覆盖新 schema 20 数据库。

历史开发报告中的已见数据不能重新命名为未见验证集。保留下面既有字段安全语义与技术参考；其中旧的研究门槛属于对应研究协议，未来短线协议须在测试前显式登记，不能事后换门槛宣布通过。

## Plan and evidence boundaries

This is the repository's **only forward implementation plan**. The current execution checklist is the 2026-09-28 section above. The material below preserves the 2026-09-24 baseline and earlier dated technical/evidence records; it is not a new production acceptance or an instruction to repeat already-completed source setup.

| Material | Role | Authority boundary |
| --- | --- | --- |
| `IMPLEMENTATION_PLAN.md` | Current implementation status, open gates, compatibility rules, and verification procedure | The sole forward plan. |
| `tools/verification/DATA_EVIDENCE_REFERENCE.md` | Detailed data-evidence and local-Web contract | Technical reference only; it does not authorize provider or production access. |
| `webapp/README.md` | Loopback/read-only Web operation | Operational reference only; no deployment authority. |
| `tools/verification/remaining_plan_runtime_20260912.json` | Sanitized 2026-09-12 read-only runtime snapshot | Historical, immutable local evidence; not live-state proof. |
| `tools/verification/PRODUCTION_RESEARCH_POOLS_20260924.md` | Dated read-only production dual-pool acceptance and exact release identity | Observation only; not source acceptance, alert delivery, or strategy validation. |

The superseded `REMAINING_PLAN_AUDIT.md` has been merged into this document and removed. Its dated production facts and runnable read-only commands are retained below; no runtime code imports planning documents.

## Current local implementation status

### Engineering infrastructure completed locally

- **Close screening and raw data:** immutable raw batches, validated active generations, universe/coverage/calendar gates, fenced leases, and conservative fallback previews are implemented. Incomplete, mismatched, stale, or unprovable data never promotes an active batch.
- **Automatic close and delivery:** durable date jobs, bounded recovery, and per-destination outboxes are implemented. A timeout, partial send, expired in-flight owner, transport error, or ACK ambiguity becomes `unknown_delivery` and is never automatically resent.
- **Intraday:** session-scoped monitoring, persistent debounce/cooldown/FSM state, target provenance, risk invalidation, market-regime confirmation, and bounded outbox processing are implemented. Stale/missing prices, unknown suspension/limit state, invalid/expired plans, weak evidence, or unavailable market confirmation block opportunity signals.
- **Recommendation review:** immutable recommendation records and T+1/T+3/T+5/T+10 evaluation paths are implemented. Immature outcomes remain `pending`; missing comparability stays `unknown`; same-session target/invalidation ordering remains `unknown_order`.
- **LLM shadow review:** schema 17 adds one durable review batch per screen run/model/prompt version plus per-recommendation `keep`/`watch`/`veto` records. The model receives only the already-frozen candidate evidence, never outcome rows; timeout is `unknown`, invalid final JSON is `failed`, and neither state changes the rule candidate or blocks its later outcome evaluation. The read-only Web exposes the model decision and separate rule/AI performance groups.
- **Market comparison and evidence:** point-in-time market-comparison observations, data-evidence envelopes, official-document restrictions, and read-only audit tools are implemented. They never tune scores or infer a missing real observation.
- **Research dual pools (schema 20):** a complete same-date active unadjusted raw batch can produce one immutable `research_only` primary/radar freeze. `/观察选股` freezes after close; `/观察池` and `/警戒池` display it. The pools remain separate from formal `screen_candidates` and recommendation outcomes. Intraday targeting is bounded to the frozen lists, with radar observation requiring fresh consecutive touches; no alert delivery or trading safety is inferred from the local tests.
- **Read-only Web:** the local Web companion reads SQLite through bounded read-only snapshots and surfaces unavailable/partial evidence rather than manufacturing data. It does not initialize plugin storage, fetch providers, send messages, or mutate data.
- **2026-09-22 audit corrections:** Web snapshot/artifact failures render explicit `partial`/`unavailable` reasons; HEAD has no response body; package wiring is validated; schema 15/16 add only additive indexes and bounded cleanup. Terminal cleanup may delete only old `sent`/`cancelled` outbox rows—`pending`, `sending`, `failed`, and `unknown_delivery` remain durable.

### Rule selector prototype, not strategy validation

- The current selector has a complete engineering path (daily history -> indicators -> explicit risk gate -> fixed-score ranking -> persisted candidates), but its rules and weights remain a **research prototype**. `core.score_quote()` ranks setups; it does not predict return or establish a probability of success.
- The baseline uses hand-written indicator conditions such as trend, five-session momentum, RSI, volume ratio, and volatility. They have not been calibrated against a sufficiently long time-attested, held-out sample.
- Local tests prove data validation, risk blocking, persistence, deterministic ranking, and fail-closed handling. They do **not** prove accuracy, profit, excess return, liquidity, or deployability for trading.
- `strategy_quality_established` remains `false`. Do not label a result "validated accuracy", "correct rate", or "effective strategy" until the long-horizon evidence gate below has been met.

### Local research work implemented; evidence collection remains open

- `tools/verification/same_day_research.py` provides a standalone, file-only two-phase study: first freeze candidates from a declared cutoff and explicit point-in-time universe, then mark only that hashed freeze using a separately captured outcome file. Evaluation cannot rerank candidates.
- The tool retains source/date metadata, source-availability and capture timestamps, canonical parsed-payload SHA-256 (`raw_sha256`), optional exact raw-response SHA-256 (`raw_response_sha256`), parser/tool version, transformation identity/details, exclusions, fill assumptions, coverage, up/flat/down counts, gross/net marked performance, a frozen benchmark identity, equal-weight universe, fixed-seed random, and simple-momentum baselines.
- Every selection observation requires a `risk_evidence` object covering suspension/limit flags and name-risk status, plus method, source, `as_of`, and quality. Only `verified` evidence can produce `eligible`; a `reconstructed_assumption` is permitted only for an explicitly transformed post-close reconstruction and is retained as `research_assumed`, never silently promoted to eligible.
- Hypothetical fills include explicit entry/mark fields, per-side fees, and entry/exit slippage. They are marked valuations, not proof of an order, liquidity, same-day sale, or realized trading return. Empty valid samples use `null` metrics; missing result prices stay `unknown` rather than becoming zero.
- A broad market reconstruction always needs an explicit historical universe file. The tool never falls back to today's turnover, list ranking, or a provider's current universe because doing so would introduce lookahead/survivorship uncertainty.
- Remaining local research is to collect and attest real point-in-time inputs, then run rolling and held-out comparisons. The local tool makes that work reproducible; it does not manufacture the missing history.

### Compatibility retained deliberately

- Database migrations remain ordered and restart-safe from schema 7 through `LATEST_SCHEMA_VERSION = 20`; no persisted timestamp format or migration path is removed. The schema-20 research tables are additive. Rolling production back to schema-19 code alone would be incompatible with an upgraded database.
- Existing public configuration, command, provider, storage, and adapter aliases remain. A missing in-repository caller is not proof that an AstrBot integration or upgraded database no longer needs an alias.
- UTC persistence now uses one offset-free boundary in `storage.py`: `datetime.now(timezone.utc).replace(tzinfo=None)`. It deliberately preserves legacy naive UTC `isoformat()` text, comparison behavior, lease expiry arithmetic, and ordering; no existing SQLite timestamps are rewritten.
- Fail-closed meanings remain unchanged: `pending`, `unknown`, `unknown_order`, and `unknown_delivery` are never converted to success, return, probability, receipt, or fresh-data claims.

## Open local research evidence gates

These do not require production mutation, but they require real, reviewable public-data evidence rather than fixtures.

1. **Freeze a rule baseline:** retain the existing formal selector as `current_rule_prototype` and the new dual-pool policy as `technical-research-v1`; do not retune either while its first real forward baseline is being measured. The 2026-09-23 dual-pool batch is the first observed freeze, not a validated sample series.
2. **Point-in-time dataset:** collect a complete, dated explicit universe, raw source snapshots, source-availability evidence, daily bars, benchmark observations, and corporate-action comparability evidence. A later capture must be labeled a post-close reconstruction, not a historical prediction.
3. **Realistic marked-performance study:** keep entry/mark assumptions, fees, slippage, price-limit/suspension evidence, partial coverage, and benchmark definition fixed per run. Report same-day up share and marked performance, never "validated accuracy".
4. **Long-horizon holdout review:** require at least 252 decision dates, 60 held-out decision dates, 1,000 held-out samples, and 90% T+1/T+5/T+20 price/benchmark coverage before `ready_for_provenance_review`. Compare the frozen selector against the named index, equal-weight universe, fixed-seed random, and simple momentum baselines. This still does not establish strategy effectiveness or authorize tuning.
5. **Independent retest:** after any rule or weight change, rerun a time-separated test set that did not participate in the change. A single session, in-sample result, or local fixture can never close this gate.
6. **AI shadow comparison:** keep the AI review non-authoritative until forward samples separately report the rule universe, `keep`, `watch`, and `veto` groups. Model, prompt version, input hash, terminal status, and exact sample denominators must remain visible. A timeout, empty final response, model change, or missing review stays outside the completed AI denominator.

## Open production-only gates

None of these are closed by local fixtures, static checks, or this plan cleanup.

1. **Provider and production data:** a good 2026-09-23 Tushare unadjusted raw batch produced one research freeze. Continue checking successive genuine closes, exchange-wise coverage and permission, exact calendar/quote/factor evidence, and complete per-stock negative suspension/limit/ST/audit evidence; this observation does not accept the full source contract or unlock formal recommendations.
2. **Recommendation maturity:** observe verified forward sessions and matching persisted factor/bar evidence for each horizon. Do not calculate returns for immature, missing, or changed-factor samples.
3. **Natural intraday acceptance:** observe an eligible fresh-quote opportunity with exact event/FSM/payload/plan identity, sent-state ordering, explicit quote/market freshness at creation and send, and any separately supported downstream receipt evidence. Stored `sent` state alone is not a receipt.
4. **Web and future deployment:** the narrow four-file plugin release and first manual dual-pool freeze are observed below. The Web companion was not updated for these research pools; its real-data views, hosting/read access, authentication/HTTPS/remote exposure, any future plugin-only reload, and new-release acceptance each require their own scope and checks. Local loopback browser evidence is not production acceptance.
5. **Research dual-pool forward acceptance:** verify the next genuine close freezes once, the next session's bounded primary/radar monitoring and any actual outbox/delivery, and separately capture matured T+N outcomes against fixed baselines. The 2026-09-23 command/freeze and user-confirmed `/观察池` reply close only the first manual display checkpoint. No observed radar alert, accuracy, or automatic daily-repeat claim follows from them.

## 2026-09-23 selected data contract (source not yet accepted)

Select the data before selecting a vendor. The first release remains a post-close, paper-only selector; intraday signals have a separate contract. Preserve the current scoring rule during collection rather than tuning it against incomplete observations.

| Dataset | Required fields and timing | Missing or conflicting evidence |
| --- | --- | --- |
| Dated universe and calendar | Decision date, exchange, full security code, ordinary A-share membership/listing status, listing/delisting effective dates, trading-session date, and universe completeness by SH/SZ/BJ. Keep non-trading/suspended members distinguishable from absent securities. | Do not replace a historical universe with today's list; incomplete membership prevents a complete market freeze. |
| Decision-day raw market snapshot | Code/date; unadjusted open/high/low/close, previous close, volume, turnover amount, change, and provider/source/basis. Preserve the raw response/batch identity, first observation time, and response digest. Obtain roughly 120 trading sessions of unadjusted OHLCV for indicators; the current gate needs at least 20 valid bars, while 20-day momentum needs 21. | Reject stale, mixed-basis, malformed, duplicate, or truncated rows; no indicator or market-breadth inference from an incomplete batch. |
| Daily execution-risk state | Exact decision-day `suspended`, `limit_up`, and `limit_down`, including the source's underlying suspension record and limit prices where available. Record the market-specific rule, comparison basis, observation time, and complete-batch proof for any derived `False`. | Every candidate needs all three explicitly `False`; absence from an event list alone is not proof of `False`. Unknown/conflict blocks publication. |
| Dated issuer risk | Decision-time ST/risk-warning status and the latest *then-published* audit report/opinion, report period, publication time, revisions, and a documented mapping to `st_flag`/`audit_flag`. An audit report remains historical source evidence, not a new report on every trading day. | Each published candidate needs both flags explicitly `False` under a verified as-of policy. No report, ambiguous opinion, unknown revision/order, or missing historical status stays `unknown`; never fill `False` from silence. |
| Forward evaluation | Immutable candidate/cutoff/decision-input hashes; future raw OHLCV and execution-risk states at T+1/T+3/T+5/T+10 (plus T+20 for research); adjustment factors and corporate-action effective/observed times; one predeclared index identity, the same-day eligible-universe equal-weight baseline, fees/slippage/fill assumptions, and result coverage. | Immature is `pending`; missing or revised factor/price/benchmark evidence is `unknown`; unprovable same-session ordering is `unknown_order`. A later historical fetch is reconstruction, not a retroactive live recommendation. |

PE/PB, ROE, profit growth, cash quality, industry labels, and model reviews are optional scoring/annotation data with their own publication timestamps and units. They do not replace any of the five hard risk states. Real-time quote timestamp/freshness, intraday halts, real-time limit status, and delivery evidence belong to a later intraday acceptance, not this post-close data contract.

Before connecting any audit-opinion feed, resolve the local contract mismatch: `safe_factor_row()` currently requires `audit_flag` evidence with `business_date == decision date` and an announcement date. Define and test an as-of latest-effective-report assertion that retains the original report date and first-observed time; do not relabel an old publication as newly announced. Keep the existing `unknown` gate until that policy and a real, dated sample are verified. Source acceptance must check permissions/quota, SH/SZ/BJ coverage, complete negative states, timestamps, revisions, and successive genuine closes on the target host; an API name or open-source wrapper is not acceptance.

## 2026-09-23 zero-new-spend source route (not production accepted)

- **This replaces the earlier Tushare-only paid-entitlement choice.** Keep only the Tushare raw daily/calendar/universe calls already demonstrated by the existing account, with their real permission and completeness checks. Do not purchase or assume access to `suspend_d`, `stk_limit`, `stock_st`, `fina_audit`, `adj_factor`, `bak_basic`, or `index_daily`. Their documented starting levels include 2000 points for most of these risk/factor calls, 3000 for `stock_st`, and 5000 for historical `bak_basic`; they are optional future alternatives, not requirements for the free-first trial.
- **Free market-data candidates:** evaluate BaoStock's anonymous-login SH/SZ unadjusted daily bars, dated `tradestatus` and `isST` as a separately identified source; cross-check code/date/basis/price/units with the existing Tushare batch. AKShare's Eastmoney historical/spot interfaces can fill or cross-check a narrowly sampled gap, including BJ only after actual code/date/coverage testing. Neither is assumed to have a stable SLA, complete BJ coverage, corporate-action provenance, or exchange limit prices. A source failure never silently falls back to a seemingly safe `False`.
- **Free issuer evidence:** fetch dated original disclosures from CNINFO/SSE/SZSE/BSE only for bounded technical shortlists. Preserve the document URL, exact excerpt, issuer code, publication/effective times, first local observation, and digest. Search results and missing hits are discovery, not proof of a negative ST/audit/suspension state. The current `OfficialEvidenceAdapter.fetch()` rejects PDFs and its descriptor matching requires the decision date in the visible excerpt; support verified PDF extraction or an official text equivalent, and fix the latest-effective audit/as-of mismatch before any such report can qualify a stock.
- **Web search and LLM:** optionally use Tavily's published 1000 free credits/month (no card required) only for candidate-level document discovery after verifying its terms, account, quota, and search completeness. Budget/caching and a strict maximum of 20 candidates per close must fail closed on exhaustion. A configured DeepSeek-style chat API does not supply built-in web search here: DeepSeek's current Responses API ignores `web_search`; an external search/fetch adapter is required. Model calls consume tokens unless an already-authorized free grant/local model exists, so default to no paid model API calls under zero-spend. If available, LLMs may extract and compare cited facts from fetched original documents, never author prices, negative safety flags, or a probability of success.
- **Required code-path redesign before any free-source publication:** today's `is_screenable()` rejects unknown suspension/limits *before* the top-300 technical stage, so a candidate-only evidence budget cannot unblock it by configuration. Compute an unpublished technical research shortlist from raw data first; fetch per-code risk evidence for at most 20 likely candidates; publish only those whose three trading states and ST/audit state are independently verified under the frozen cutoff. Keep the public `unknown`/`pending` behavior, no forced number of picks, and preserve the broad full-snapshot/indicator gates. Until this is implemented and verified, the free route yields research observations, not approved recommendations.
- **Acceptance/stop conditions:** bounded public-source probes on the actual Pi, exchange-wise SH/SZ/BJ and positive/negative suspended/ST/limit/audit/corporate-action controls, repeatable genuine closes, quota and terms, raw/factor comparability, timestamps, revisions, and independent read-only audit. Missing BJ coverage limits the reported research scope explicitly; it does not become an all-A-share result. Any missing proof stays `unknown`. Future T+N observations start from real freezes; historical downloads cannot be backdated into live accuracy. Real-time monitoring remains a separate contract.
- This is a source and workflow proposal only. On the Windows host, the public CNINFO homepage and an Eastmoney single-stock quote returned HTTP 200 on 2026-09-23, but the exact Eastmoney risk companion request failed with an HTTP transport error; neither AKShare nor BaoStock is installed in the mandated Python 3.13 environment. Prior BaoStock probes covered SH/SZ only; its newly released package and BJ behavior have not been retested. No production Pi, secret, paid model call, account signup, installation, or runtime was changed in this review.
- The local 18:30 hard floor and changed defaults were rolled back. The automatic close remains configurable through `daily_scan_time` (default 15:10), and acceptance through `daily_acceptance_time` (default 15:40; never earlier than the scan). For an 18:30 close scan, set both fields in the AstrBot WebUI to 18:30 and 19:00 respectively; verify saved configuration and runtime behavior separately. No production configuration or plugin runtime was changed here.

### 2026-09-23 Raspberry Pi isolated free-source probe

- On confirmed host `my-pi` (aarch64, Python 3.13.5), installed `akshare==1.18.97` and `baostock==0.9.4` only in `/home/pi/apps/stock-watch-data-probe/.venv` (277 MB, owner `pi`, mode 700). `pip check` and both imports passed. System Python had neither package; AstrBot container and Web runtime were not modified or reloaded.
- BaoStock's anonymous query returned 7,404 SH/SZ universe rows dated 2026-09-22. Same-date `sh.600000` had `tradestatus=1,isST=0`, suspended `sh.601059` had `tradestatus=0,isST=0`, and `sh.600053` had `tradestatus=1,isST=1`. These distinguish trading status from ST but do not establish same-session intraday halts or price-limit safety. A `bj.920001` query returned error `10004011` (only `sh`/`sz` identifiers accepted).
- AKShare's current ST list returned 419 rows; its 2026-09-22 limit-up and limit-down pools returned 63 and 3 rows. Pool membership is positive evidence only, not an exhaustive proof of a per-stock negative state. SH `600000` raw closes matched BaoStock on 2026-09-21/22, but the providers' volume fields used different units. BJ spot and dated history (`920001`, `920002`) disconnected on this host; this is a failed probe, not proof that all BJ data is unavailable everywhere. The plugin's exact Eastmoney risk-companion query also disconnected on this Pi.
- BaoStock `query_adjust_factor` for `sh.600000` returned a factor dated 2026-07-16, without an API publication/first-observed timestamp. A CNINFO original PDF for `600053` (2025 annual report; URL path dated 2026-04-29, publication time not separately verified) was retrieved over HTTPS and parsed by the Pi's `pdftotext`; its opening pages identify the issuer and report a standard unqualified audit opinion. This does not negate the separate 2026-09-22 ST state, nor does a 2026-09-23 fetch prove that the response was first available by an older freeze cutoff. The plugin's current official-evidence adapter rejects PDF and requires exact decision-date matching, so this manual parse cannot be injected as `audit_flag=False`.
- These are bounded public, read-only source probes. No risk fields were injected into live quotes, no recommendations were frozen, and no accuracy, complete SH/SZ/BJ coverage, company-action chronology, or production-source acceptance was established. The `is_screenable()` early gate and the missing per-stock verified negative limit states still block automatic publication. Do not label the positive-pool counts as complete limit coverage.

## Preserved dated historical evidence

The following is retained for audit context only and must not be presented as current production state.

- The sanitized runtime snapshot at `tools/verification/remaining_plan_runtime_20260912.json` was captured on **2026-09-12T01:17:32.299271+00:00**. It has hashed origins and no raw message bodies or credentials.
- That dated read-only snapshot found 32 completed screen runs across five decision dates (**2026-08-28** through **2026-09-11**) and 30 recommendations dated **2026-09-11**. All T+1/T+3/T+5/T+10 horizons then had zero verified forward sessions and `unknown` stored outcomes; this is historical insufficiency evidence, not a current maturity result.
- It also found 32 sent-state risk invalidations (30 `plan_expired`, one `data_invalidated`, and one `market_regime_invalidated`) generated on **2026-09-10 09:30:16 +08:00**. They lacked fresh quote/market timestamps and were not opportunity triggers or downstream-receipt proof.
- The installed AstrBot 4.27.4 common send interface examined on 2026-09-12 exposed a boolean send result, not an idempotency or delivery-receipt lookup. Therefore the capability was recorded as version-bounded **not applicable**, not as downstream exactly-once support.
- The dated audit used read-only SQLite (`mode=ro`, `PRAGMA query_only=ON`) and bounded host inspection; it performed no production mutation. The snapshot remains unchanged rather than being relabelled as a new check.

## Verification procedure

Run only from the authoritative worktree and use the exact Python executable for every Python command:

```powershell
$Python = 'C:\Users\DIO\AppData\Local\Programs\Python\Python313\python.exe'
& $Python -m pytest tools/verification/test_v0138_automatic_close.py tools/verification/test_v0152_audit_corrections.py -q
& $Python -m pytest tools/verification/test_same_day_research.py tools/verification/test_v0137_offline_walk_forward.py tools/verification/test_v0142_remaining_plan.py -q
& $Python -m pytest -q
$Files = (Get-ChildItem -File -Filter '*.py').FullName + (Get-ChildItem webapp,tools/verification -Recurse -File -Filter '*.py').FullName
& $Python -m py_compile @Files
Get-Content -Raw _conf_schema.json | ConvertFrom-Json | Out-Null
node --check webapp/static/app.js
git diff --check
```

For an authorized, explicitly supplied **local/read-only** database only:

```powershell
& $Python market_comparison.py --database $Database --trade-date $TradeDate --benchmark-code $BenchmarkCode --universe-ref $BatchId --as-of $AvailabilityCutoff
& $Python tools/verification/audit_runtime_evidence.py --database $Database --as-of $AuditDate --max-seconds 90 --quote-max-age-seconds $QuoteAgePolicy --market-max-age-seconds $MarketAgePolicy
& $Python tools/verification/audit_point_in_time.py --dataset $Dataset --as-of $AuditDate
```

These audit commands must use `mode=ro`/`query_only` behavior and do not initialize `StockStore`; an unavailable exact key is `unknown`, not a substitute observation.

## 2026-09-22 local reconstruction usage

The following is an **after-close reconstruction**, not a historical 2026-09-22 prediction. It may use a local selection snapshot whose source data is through **2026-09-21** only when that file declares a source `available_at` no later than `2026-09-21T15:30:00+08:00`. Its physical `captured_at` may be later, but the tool will record `post_close_reconstruction` and never upgrade it to prediction evidence.

```powershell
$Python = 'C:\Users\DIO\AppData\Local\Programs\Python\Python313\python.exe'
$Research = 'tools/verification/same_day_research.py'
$Run = 'tools/verification/research/2026-09-22'

# Both inputs are local JSON files. The universe is mandatory and must be the
# explicit 2026-09-21 universe; no current turnover/list ranking is accepted.
& $Python $Research freeze `
  --input "$Run\selection_through_2026-09-21.json" `
  --universe "$Run\universe_2026-09-21.json" `
  --cutoff-at '2026-09-21T15:30:00+08:00' `
  --target-date '2026-09-22' `
  --frozen-at '2026-09-22T15:31:00+08:00' `
  --top-n 10 --random-seed 20260922 --benchmark-code '000300.SH' `
  --output "$Run\candidate_freeze_2026-09-22.json"

# Capture/parse the separate 2026-09-22 result snapshot first, preserve its raw
# response digest and source availability metadata, then mark the frozen list.
& $Python $Research evaluate `
  --freeze "$Run\candidate_freeze_2026-09-22.json" `
  --outcomes "$Run\outcomes_2026-09-22.json" `
  --target-date '2026-09-22' `
  --outcome-cutoff-at '2026-09-22T15:10:00+08:00' `
  --evaluated-at '2026-09-22T15:11:00+08:00' `
  --entry-field open --mark-field close `
  --entry-slippage-bps 5 --exit-slippage-bps 5 --fee-bps-per-side 5 `
  --output "$Run\marked_evaluation_2026-09-22.json"
```

## Final-run input requirements

Before a real local reconstruction is accepted for review, require all of the following:

- Selection `captured_at` must be no later than `frozen_at`; outcome `captured_at` must be no later than `evaluated_at`; and `evaluated_at` must not precede the freeze. The freeze and evaluation timestamps are part of the hashed artifacts.
- `raw_sha256` means a SHA-256 of the canonical parsed payload. When exact HTTP response bytes are retained, store their separate digest as `raw_response_sha256`. A post-close snapshot must also contain a non-empty `transformation_id` and `transformation_details`; an unexplained later reconstruction is rejected.
- Every observation must carry `risk_evidence` for `suspended`, `limit_up`, `limit_down`, and `name_risk_status`, with its method/source/as-of/quality. Unknown, missing, malformed, or non-verified evidence fails closed. `reconstructed_assumption` can enter only the post-close research pool and must be reported as assumed risk.
- The freeze must set `benchmark_code` explicitly. The outcome benchmark row must use that exact code. Benchmark comparison fields mean selector gross/net marked return minus the benchmark's **gross** marked return; they are not net-to-net tradable comparisons.
- Random and momentum baselines use the freeze's same selector risk pool. The equal-weight baseline is explicitly the whole supplied universe and may retain missing outcome coverage; it is not an eligible-pool baseline.

The outcome file must contain its own canonical payload/source/date/parser metadata, dated `2026-09-22` rows for each observed code, and the exact frozen benchmark row. Missing prices remain partial/`unknown`; a missing benchmark yields null comparison fields rather than substituting another index. Do not make network requests through this plan command without a separately reviewed public-data source and local raw capture.

## 2026-09-22 same-day research implementation verification

- Passed with the mandated Python 3.13 executable: new research-tool tests **13 passed**; focused research/replay/plan tests **34 passed**; the complete suite was run in bounded file groups and totaled **312 passed** (`185 + 94 + 33`, where the first focused run preceded the added public-URL guard test). No test failed or was skipped in these runs.
- Passed: complete source/Web/verification compilation **45 files compiled**, `_conf_schema.json` parsing, `node --check webapp/static/app.js`, `same_day_research.py --help`, and `git diff --check`. The diff check emitted only pre-existing CRLF conversion warnings for unrelated modified files; it found no whitespace errors.
- This verification did not call public endpoints, read plugin configuration/credentials, access production, deploy/reload, send messages, or place trades. No live 2026-09-22 candidate or outcome was generated.

## 2026-09-22 AI shadow implementation verification

- Added schema 17 review batches and per-recommendation decisions, a dedicated model timeout and shadow token budget, strict all-candidate JSON validation, no-automatic-retry identity, automatic post-freeze invocation, and read-only Web comparison groups.
- Passed with the mandated Python 3.13 executable: focused AI/storage/Web tests **47 passed** after the fixture correction; dedicated AI tests **5 passed**; complete `tools/verification` suite **327 passed** in 57.38 seconds.
- Passed: Python compilation for the changed runtime/Web modules, `_conf_schema.json` parsing, `node --check webapp/static/app.js`, and `git diff --check` with no whitespace errors.
- Passed browser acceptance against a synthetic read-only database at 1440x1000, 768x1024, 390x844, and 320x740: no body overflow, no broken images, no blank chart, all T+ horizons and existing workflows passed, and the report contained zero browser/console errors.
- This is local implementation evidence only. The production Raspberry Pi was not changed, the plugin was not reloaded, and the saved one-session DeepSeek result remains exploratory rather than validated strategy accuracy.

## 2026-09-22 daily acceptance and proactive alert verification

- Added additive schema 18 daily acceptance runs, state transitions, anomaly/recovery events, durable per-origin alert outbox, and delivery-capability evidence. A schema 17 database migrates in place without rewriting recommendation or candidate data.
- The daily check runs after the configured close scan cutoff and verifies the same-date complete snapshot, same-date completed candidate freeze, recommendation freeze count, optional AI shadow batch, stale AI `pending`, and overdue recommendation outcomes. Outcome blockers are grouped from durable database reasons such as `corporate_action_evidence_missing`; missing or unprovable evidence remains `unknown`.
- Alerts target only currently subscribed and push-allowed origins. The same anomaly identity is not resent while unchanged; a changed anomaly creates a new event, and recovery creates a separate notification. Ambiguous sends become `unknown_delivery` and are excluded from automatic retry.
- The read-only Web overview and health views expose the latest acceptance state, findings, history, and aggregate alert-outbox states. The plugin does not read the privileged Web snapshot path; Web snapshot freshness remains limited to the Web service's own published snapshot metadata.
- Passed with the mandated Python 3.13 executable: dedicated daily acceptance tests **5 passed**, focused automatic-close/recommendation/Web/AI/acceptance regression **76 passed** before the migration case was added, final focused Web/AI/acceptance regression **37 passed**, and the complete suite **332 passed** in 46.88 seconds.
- Passed: complete Python compilation, `_conf_schema.json` parsing, `node --check webapp/static/app.js`, and scoped `git diff --check`. No production files were changed, no plugin reload was performed, and no alert was sent to a real origin.

## 2026-09-23 local foundation and open-session read-only probe

- Made daily acceptance event creation and its per-origin alert outbox one SQLite transaction. A formatting or insertion failure rolls back the event, allowing the next tick to try again. Repeated ticks retain the active event identity without adding duplicate alerts; a superseded pending alert is cancelled before it can be claimed for sending. `unknown_delivery` remains non-retryable.
- The mandated Python 3.13 executable completed the full local suite: **336 passed**. The focused acceptance/automatic-close run passed **33 tests** before the scheduler integration case was added; changed modules compiled and scoped `git diff --check` passed. No local test proves an external receipt.
- Read-only public-market probes during the 2026-09-23 morning session fetched three requested Sina targets, twice six seconds apart, with same-date provider timestamps advancing as expected. The companion Eastmoney `push2.eastmoney.com` endpoint disconnected in this local environment, and all three returned quotes retained unknown suspension/limit flags. This is a **price-freshness smoke test only**, not an accepted intraday opportunity/delivery or production-host test. A different Eastmoney history host responding does not validate its realtime companion contract.
- At this phase the next gate was a second, independent point-in-time source for stock membership, corporate actions, suspension and limit evidence; BaoStock had not yet been tested or selected. Preserve the existing fail-closed signal behavior; do not synthesize risk flags or promote an Eastmoney preview to a complete active raw batch. Dual-basis research and licensed real-time alternatives remain pending source-contract validation. The isolated BaoStock probe below updates this source assessment.
- This turn did not read production credentials or the Raspberry Pi, reload the plugin, publish a Web release, send a real message, or place a trade. Production deployment still requires separate authorization and live preflight.

## 2026-09-23 BaoStock isolated public-data probe

- Inspected the `baostock==0.9.3` wheel before use. Its client connects to `public-api.baostock.com:10030` over TCP without a built-in socket timeout. The wheel was imported directly from an isolated temporary path with a six-second socket default and an outer process deadline; it was not installed into the plugin or the mandated Python environment. These are public, anonymous, read-only queries, not a reliability or licensing acceptance test.
- At 10:11-10:14 Beijing time, anonymous login and historical queries succeeded locally. The 2026-09-22 `query_all_stock` response contained 7,404 Shanghai/Shenzhen securities (7,391 trading, 13 suspended), but no Beijing exchange code. A 2009-12-28 query included `sh.600001` before its 2009-12-29 delisting; the current basic-info endpoint reports it delisted. This is evidence of dated universe membership, not proof of when a later-retrieved record was first available or that all security types are eligible shares.
- Unadjusted 2026-09-21/22 daily open/close for `sh.600000` and `sz.000001` matched separate Eastmoney history responses obtained at 10:13. The two sources' volume fields use different units in these responses (BaoStock shares versus Eastmoney lots); do not compare or merge raw volume numbers. BaoStock returned `tradestatus=0`, empty volume and `isST=0` for suspended `sh.601059` on both days; `isST=0` does not negate a suspension. A Beijing sample `bj.430047` was rejected as an unrecognized exchange code.
- The Shanghai sample's factor endpoint returned dated ex-dividend operations for 2025-07-16 and 2026-07-16, but no publication/first-available timestamp or historical revision record. Forward-adjusted and unadjusted prices agreed on the two September sample days. Across the 2025-07-16 ex-date, however, all three price bases differed: 2025-07-15 close was 13.9300 raw, 12.91007326 forward, and 172.56915830 backward. This is a current retrospective query, not a frozen 2025 observation or proof of a point-in-time conversion rule. At 10:14 the 2026-09-23 daily and universe requests both returned success with zero rows during the open session. No intraday suspension/limit evidence was obtained.
- Decision: keep BaoStock outside production selection and risk gates. It is a candidate for post-close, Shanghai/Shenzhen-only historical cross-checking after a repeatable capture, date/type filter, independent corporate-action announcement provenance, independently reconciled factor math, and source-availability/revision audit. A queried historical date is not its `known_at`; never backdate this 2026-09-23 capture to claim a 2026-09-22 live prediction. Beijing coverage, same-session risk flags, licensed realtime quotes, real deliveries and production-host reliability remain open. Missing or conflicting evidence stays `unknown`.

## 2026-09-23 local corporate-action capture gate

- Added schema 19 fields for the first locally observed Tushare `adj_factor` response time and SHA-256 of its canonical parsed response. The first successful observation is retained on an identical refetch; a changed factor/source/evidence marks the cached row conflicted rather than silently replacing its initial value. Existing schema-18 rows have empty capture fields and are not backfilled from `fetched_at` or the factor trade date. A later fresh fetch can establish evidence from then onward, never retroactively.
- The provider carries capture metadata through daily rows and quotes; recommendation creation cross-checks it against the persisted factor and the actual freeze/date cutoff. Daily-bar marking cross-checks the stored observation against the evaluation cutoff. Missing, late, invalid, unpersisted, or conflicting evidence remains `unknown` once the T+N window matures; immature windows remain `pending`. The independent read-only audit requires the same captured identity, digest, and chronological order rather than trusting a mutable `fetched_at` alone.
- This is a **local-observation** timestamp, not proof of the exchange announcement time or first API publication. The digest covers parsed response JSON rather than exact HTTP bytes. No 2025/2026 historical API query is promoted to a past live prediction. Actual Tushare `adj_factor` entitlement has not been checked with credentials; the stated 200-point account must not be assumed to have access. BaoStock remains a separate research-only probe, not a silent fallback.
- Synthetic fixture tests exercise capture, re-fetch, revision conflict, migration/legacy rows, cache, freeze, pending, late outcome bars, and read-only audit. No production data, token, Raspberry Pi, real alert, reload, trade, or deployment was accessed in this local implementation.
- Verification with the mandated Python 3.13 executable: complete `tools/verification` suite **343 passed**, focused corporate-action/market-comparison/acceptance suite **40 passed**, changed Python modules compiled, and `git diff --check` passed. Existing unrelated CRLF conversion warnings remain. These tests use synthetic factor responses; no account entitlement or first-publication timestamp was verified.

## 2026-09-24 production research dual-pool checkpoint

The later 2026-09-24 risk-evidence integration installed a read-only
BaoStock/AKShare companion for the 2026-09-23 batch and reloaded only the
stock plugin. It validates batch/date/close before displaying per-stock
retrospective suspension, ST and positive limit-pool matches in `/观察池`
and `/警戒池`; frozen `risk_level=unknown` remains unchanged. The 27-row
real-batch check and plugin load passed. The Pi subsequently enabled a
weekday 18:30-23:30 hourly risk-evidence collector, with its first service
invocation passing in waiting state. Its first real scheduled network run,
end-of-day failure notification, freeze-time risk gating, official
announcement chronology, and an actual chat reply after reload remain open.
See `tools/verification/RESEARCH_RISK_RETROSPECTIVE_20260924.md`.

- The 2026-09-23 Sol-only release replaced exactly four staged plugin files and reloaded only the stock plugin. A 2026-09-24 read-only check matched all four installed hashes against the staging manifest, found the container running with unchanged PID and `RestartCount=0`, and confirmed the rollback directory still held its manifest and nonempty database backup. This is the deployed four-file release, not a publication of this whole uncommitted worktree.
- Production SQLite passed `PRAGMA quick_check`, reported schema 20, and held exactly one `research_only` Tushare/unadjusted run for 2026-09-23. It froze seven primary and 20 radar codes with all 27 risks `unknown`; formal active candidates still referred to 2026-09-16. The run diagnostics recorded 5,209 selector input rows, 300 deep examined, and 185 ranked; neither number proves full-market coverage or tradability.
- Plugin load and inbound `/观察选股` and `/观察池` commands appear in bounded production logs. The user separately confirmed `/观察池` returned a reply. Its browser body and `/警戒池` reply were not captured by the read-only checkpoint, and no real radar alert or daily-repeat behavior was accepted. See `tools/verification/PRODUCTION_RESEARCH_POOLS_20260924.md` for exact hashes, batch ID, code lists, query boundaries, and remaining gates.
- This closes the first manual research-freeze/display checkpoint only. It does not turn research lists into formal recommendations, attribute old recommendation outcomes to the new lists, prove the data-source risk contract, or establish accuracy. Continue consecutive real freezes and held-out outcome collection before rule tuning; preserve the previous batch and rollback bundle.

## Earlier 2026-09-22 consolidation verification

- Passed with the mandated Python 3.13 executable: focused pytest **32 passed**, full pytest **300 passed**, and complete source/Web/verification compilation **51 files compiled**. No substitute interpreter was used.
- Passed: `_conf_schema.json` parsing, `node --check webapp/static/app.js`, and `git diff --check`. Static scoped checks also found zero remaining `datetime.utcnow()` calls and no live references to either retired plan name outside this historical consolidation note. Production gates remain open regardless of these local results.
- Exact removals in this consolidation: `tools/verification/REMAINING_PLAN_AUDIT.md` (merged historical material) and the obsolete `datetime.utcnow()` call form from `storage.py` plus its automatic-close fixture. No public compatibility aliases, schema migrations, persisted fields, or public commands were deleted.
