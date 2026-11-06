# 执行参数与产物

## 清单

支持数组，或对象 `{ "defaults": {...}, "tasks": [...] }`。defaults 可包含 model、effort、web_search、write_dirs、retry_safe。任务覆盖默认值，数组整体替换、不合并。任务必填 id、prompt，以及合并后非空的 model、effort；title 默认等于 id。

- `web_search`：disabled（默认）、cached、live。
- `write_dirs`：已存在的绝对目录；目录权限不提供并发编辑隔离。
- `retry_safe`：默认 false。只有确认任务整体可安全重复执行时才设 true；它允许在已观察到工具执行后，对明确的暂时性错误自动重跑。它不会使业务阻塞或格式错误自动重跑。

COMMON 与任务提示词、模型、推理强度、通道、网页搜索和写目录参与输入指纹校验。并发、超时与重试政策可在后续调用中调整，不改变任务身份。旧回执的输入无法核实或输入发生变化时，整批在启动前报错。

## 运行控制

| 参数 | 默认值及行为 |
| --- | --- |
| `--concurrency N` | 8；运行中的 EXEC 数量上限 |
| `--ramp-seconds N` | 20；先最多 4，再最多 12，之后到并发上限；0 直接使用上限 |
| `--timeout-seconds N` | 1200；每次 EXEC 超时，终止其进程组 |
| `--max-retries N` | 2；每任务每次批次调用的自动重试上限，0 关闭，最大 10 |
| `--retry-backoff-seconds N` | 30；带随机抖动的指数退避，最多 120 秒；等待期间不占 EXEC 槽位 |
| `--retry-failed` | 手动重跑选中任务的 failed、partial、blocked、interrupted；complete 始终复用 |
| `--only ID ...` | 仅本次处理指定任务；简报也仅包含这些任务 |
| `--status-interval-seconds N` | 2；落盘状态和资源采样周期 |
| `--progress` | 将周期状态也输出到标准输出；默认只输出开始与结束 |
| `--no-dashboard` | 不自动打开终端面板；不影响执行与落盘状态 |

可用内存低于约 1.5 GiB 时暂停新任务，status 的 waiting_for_memory 为 true；复用结果不受此门槛影响。任务失败不阻止其他任务，配置错误则在执行前拒绝。SIGTERM 会停止本次 worker 进程组并保存中断状态。同一运行目录禁止重叠调度，即使使用不同清单；独立目录可并行运行。

自动重试仅识别明确的服务拥堵、短时限流或连接中断等错误；不会重试额度耗尽、认证失败、partial、blocked、超时、结果格式错误。模型输出前发生过工具执行时，默认不自动重跑，以免重复副作用。手动 `--retry-failed` 仍需由主代理判断任务是否适合从头执行。

## 登录通道

默认 `--channel sub` 使用保存的 ChatGPT 订阅登录。用户指定 API 时用 `--channel api`，读取 `~/.config/codexexec/api-key`。通道不可用直接报错，不自动切换；调度器不会修改登录状态。其他隔离设置见 scripts/codexexec 与 dispatch.py 的启动命令。

## 结果读取

每次调度写入独立的 `runs/<run-id>/`：

- `report.json`：精简简报，异常预览最多 20 项；ok 仅在正常结束且没有待处理项时为 true。
- `results.jsonl`：每任务一条实际结果，包含 summary、artifacts、issues、状态和证据路径。
- `attention.jsonl`：非 complete、结果无法读取或 issues 非空的任务。
- `summary.json`：完整回执汇总，兼容旧消费者。
- `status.json`、`resources.jsonl`、`events.jsonl`：状态、资源采样和调度事件。

`--report` 读取当前调用的精简简报；未结束时返回状态。`--results` 在结束后分页读取结果，`--attention` 筛选待处理项，limit 为 1–100、默认 20。根目录 `status-<清单文件名主干>.json` 指向最新调用；同一目录避免给不同清单使用相同的文件名主干。

每个 worker 的 `prompt-NN.txt`、`events-NN.jsonl`、`stderr-NN.log`、`receipt-NN.json`、`result-NN.json` 保留尝试历史。receipt.json 指向最新回执；result.json 仅在当前尝试成功输出有效结果后更新。结果状态为 complete、partial 或 blocked；回执还可能为 failed 或 interrupted。

退出码 0 表示调度过程结束，不代表所有任务完成；自动化调用应检查 report.ok、counts 和 attention_count。失败详情以回执与绑定到该次尝试的结果为准。
