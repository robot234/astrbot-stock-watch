# 风险来源补采：入口与当前阻塞

## 用户确认

2026-10-04 用户“认可”2026-10-03 的三个只读核对结论和冻结门槛。
这不是来源验收通过，不授权把场景加入许可；不修改冻结协议、不打策略清单的钩。
原 20 日全风险比对仍为 incomplete，保留原始结果及哈希，不重复跑不变输入。

## 已实现的完整采集单元

- `TushareRequestGateway.request_existing_api` 只复用已在内存中配置的凭据，
  不读取配置文件，不把请求 payload/凭据写进采集结果；关闭延迟限流重试。
- `SinaQuoteProvider.collect_daily_risk_source_inputs` 是手动研究入口，
  没有任何自动筛选、正式推荐或模拟盘调用它。
- `daily_risk_sources.py` 分页采集 namechange、suspend_d、stock_basic L/D/P、
  沪深交易日历。字段、代码后缀、查询日期/市场、分页重复/冲突均需通过检查。
  分页到正例后的空终页才记完整；第一张空页没有完整性证据，不猜成安全。
- 请求数、页数和耗时有边界。权限/限流失败不按每个交易日重复请求，
  其他同一接口连续两次失败后停止该接口；错误只记录类型，不回显服务端消息。
  网关既有的屏蔽取消语义没有改变；退出或超时不等于证明远端请求已经停止。
- 采集结果可供推导输入使用：历史有效名、完整停牌证据、上市日期和至少六个
  连续交易日。日期、原始批次、真实取数时间及证据哈希保留。
  **不制造普通交易制度证明**，缺特殊状态证据仍保持 None。
- companion 的**研究采集适配**使用 `stock/get`，不再误套 `ulist.np/get`
  字段语义。逐代码请求显式限量、限时；校验代码、两位价格标度、同日收盘时间、
  参考收盘价及上下限。缺少停牌/ST 不推 False，失败数据四项全部未知。
  生产 `_enrich_risk_fields` 尚未切换，避免未验收适配进入正式链及无意增大请求量。

## 入口与验证

离线工具：`tools/capture_derived_risk_sources.py`。
只支持已配置网关或当前进程的 `TUSHARE_TOKEN`/`TUSHARE_URL`；
不自动读取 `.env`、AstrBot 配置、账密或其他凭据文件，也不写生产数据库。
输出独占创建，源数据只放本地私有目录，不发布。

```powershell
python tools/capture_derived_risk_sources.py --input .local_records/formal_risk_derived_20261003/comparison_input.json --trade-date 2026-09-30 --output .local_records/formal_risk_derived_20261003/source_capture_new.json
```

退出码 4 为没有运行时凭据、未执行；5 为未许可的研究采集，不能解释为验收通过。
companion 默认不请求；必须显式指定 `--companion-symbols`。

针对采集入口、空页/冲突/重复分页、日期、预算、权限失败停止、取消传播、
凭据不回显、companion 价格/日期，以及原未许可来源门进行了检查：
`test_daily_risk_source_collection.py` 和 `test_v0162_formal_source_gate.py` 共 27 项通过。
新测试是采集单元测试，不是来源许可或生产验收测试。
同一代码及环境的完整回归已通过：443 passed，3 subtests passed，89.87 秒。
后续仅做文档记录及干净提交快照的 release_check，不重跑不变的完整回归。

复用 10-03 已保存的 3 只真实 `stock/get` 快照验证新适配：3 只都能绑定
09-30 价格/时间，但停牌和 ST 仍为未知。没有重复取网来模拟进展。

## 阻塞与授权边界

本机及生产容器进程环境均未配置 TUSHARE_TOKEN/TUSHARE_URL。
离线工具实跑退出 4：Tushare `not_executed`、请求数 0；
不是 Tushare 接口已失败，也不能据此声称没有接口权限。
用户补充凭据在 AstrBot 插件配置中，只说明位置，不能当作取消原“不读凭据文件”
限制。已经询问是否允许采集程序仅在内存中读取该插件 Tushare 配置并立即调用；
未得到明确授权前不执行该读取，也不向用户索要令牌文本。

此外，20 日历史 companion 快照不能由当前接口响应重建；当前适配及三个样本
不能补上历史覆盖，缺特殊状态证明的风险字段也不会自动恢复。
必须补齐真实证据或由用户决定新的、事前冻结的观察窗口，不能暗改旧协议门槛。

尚未加入任何许可、合回 main、部署、重载插件或重启容器。
策略清单的未提交内容哈希与 10-03 开工时一致，未暂存或提交它。
