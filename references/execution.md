# 执行参数与产物

## 清单与工作区

支持任务数组，或对象 `{"defaults": {...}, "tasks": [...]}`。defaults 可包含 `model`、`effort`、`web_search`、`write_dirs`、`retry_safe`、`task_timeout_seconds`、`idle_warning_seconds`。任务字段覆盖 defaults；数组整体替换。每项需提供非空的 `id` 和 `prompt`，合并后需有非空的 `model` 和 `effort`，每次启动（含重试）均显式传入。省略 `title` 时用任务 ID。

- worker 只接收 `COMMON.md` 与该任务的提示词，默认在 `workers/<id>/` 工作，不继承主对话。默认关闭 apps、嵌套多代理、记忆、浏览器、电脑操作和网页搜索。
- `web_search`：`disabled`（默认）、`cached` 或 `live`。
- `write_dirs`：已存在的绝对目录数组。给并行任务划分互不重叠的写入范围；目录权限不提供文件锁或工作树隔离。
- `retry_safe`：默认 false。仅在任务可安全整体重做时设 true；这允许在观察到工具执行后，仍因明确的暂时性错误自动重试。

`COMMON.md`、任务提示词、模型、推理强度、登录通道、网页搜索和写目录参与输入指纹。并发、超时和重试策略不改变任务身份。复用相同工作区时，如果旧回执无法核实或指纹变化，启动前拒绝整批；要改这些输入，应换任务 ID 或用新工作区。

## 提交、等待与取消

以下命令假定 `$SKILL` 为技能目录、`$RUN` 为工作区、`$BATCH` 为提交返回的 `batch_id`：

| 命令（接在 `python3 "$SKILL/scripts/dispatch.py"` 后） | 行为 |
| --- | --- |
| `--run-dir "$RUN" --init` | 在新目录建立 `COMMON.md` 和 `tasks.json`。 |
| `--submit --run-dir "$RUN"` | 启动脱离提交命令的调度进程，返回启动状态、`run_id` 和可保存的 `batch_id`（`$RUN/runs/<id>` 的绝对路径）。`starting` 表示就绪尚未确认；`startup_failed` 时查看 `error` 与 `log_path`。 |
| `--batch "$BATCH" --wait --after CURSOR --wait-seconds 60` | 等待游标之后出现待处理结果，或批次结束、调度进程失联、等待时限到达。初始游标为 0，继续等待时传回上次等待返回的游标。 |
| `--batch "$BATCH" --status` | 查看这次调用的当前状态。 |
| `--batch "$BATCH" --report` | 批次结束并落盘后读简报，运行中返回状态。 |
| `--batch "$BATCH" --results --after CURSOR --limit 20` | 运行中或结束后，读取游标之后已落盘的终态任务行；可加 `--attention`。 |
| `--batch "$BATCH" --cancel --wait-seconds 60` | 请求停止这次调用及其所属 worker。返回后检查实际状态；若仍在清理，稍后按同一批次句柄再次检查。 |

`--wait-seconds` 默认 60，范围 0–60，只约束当次等待或取消命令的等待时间。等待超时不会终止批次。等待返回的 `event` 为 `attention`、`terminal` 或 `timeout`；`items` 只包含待处理项，普通成功不会单独触发返回。收到 `terminal` 时检查 `state`：`complete` 和 `interrupted` 与 `startup_failed`、`lost`、`orphaned` 的含义不同。调度器失联时，如发现仍存活的 worker，状态为 `orphaned`；`lost` 不保证清理完成。

取消必须显式提供 `--batch`，不能用工作区路径猜测要停止哪次调用。取消先持久化请求并关闭该批次当前拥有的重试计划，再停止调度器和核实所属进程。取消后看 `stopped`、`outcome` 和清理范围；如返回 `cleanup_pending` 或清理未确认，稍后按同一句柄查状态和 `cancellation.json`，必要时再次取消以核实清理。重复取消保留原请求及已认领的进程身份。遇到启动失败、失联或中断时，先核对证据再决定是否重跑。

前台命令仍可用：`--run-dir "$RUN" [运行参数]` 会持续到批次结束。兼容的 `--run-dir "$RUN" --status` 按所选清单查看最近提交，包括启动失败的调用，并另行提供仍活跃的批次信息；按 `--batch` 查询可固定到一次调用。最近提交和活跃执行分别记录，旧批次的周期状态更新不会改变最近提交的身份。根目录 `status-<清单文件名主干>.json` 是兼容状态快照，避免给不同清单使用同一文件名主干。

同一工作区的根锁禁止并行调度，即使用了不同清单或脱离终端提交也一样。独立工作区可并行。面板通过系统默认终端打开，供人查看，可关闭；它不向主对话推送通知。主代理在等待调用之间可以做独立工作，然后用原句柄跟进到终态。

## 单次尝试、资源与重试

批次没有总运行时限。每次尝试的硬时限和静默告警分别按任务字段、清单 defaults、命令行设置取值。

| 参数 | 默认值及行为 |
| --- | --- |
| `--task-timeout-seconds N`（旧别名 `--timeout-seconds`） | 0，禁用。设置正数后，从单次 EXEC 启动计时，超时清理该尝试所属的本机进程。排队和重试退避不计入。清单字段为 `task_timeout_seconds`。 |
| `--idle-warning-seconds N` | 600。运行中的 worker 无新事件达到阈值时，状态列出 `stalled_ids`；不会仅因静默杀掉进程。0 禁用，清单字段为 `idle_warning_seconds`。 |
| `--concurrency N` | 8；运行中的 EXEC 数量上限。 |
| `--ramp-seconds N` | 20；前 N 秒最多 4 个，接下来 N 秒最多 12 个，之后到并发上限；0 直接使用上限。 |
| `--max-retries N` | 2；每任务一条重试链的自动重试上限，0 关闭新链的自动重试，最大 10。崩溃后续跑保留旧链原预算、策略和退避截止时间；新提交的参数用于新链。 |
| `--retry-backoff-seconds N` | 30；带随机抖动的指数退避，最多 120 秒；退避期间不占 EXEC 槽位。 |
| `--only ID ...` | 仅处理指定任务；简报也只包含这些任务。 |
| `--retry-failed` | 显式重跑符合条件的 failed、模型声明的 partial/blocked、interrupted；启动前须确认旧工作已退出。complete 或已经成功执行但结果证据缺损的尝试仍复用并提示处理。 |
| `--status-interval-seconds N` | 2；状态和资源采样间隔，范围 0.1–60 秒。 |
| `--progress` | 前台标准输出也包含周期状态，默认只输出开始和结束状态。 |
| `--no-dashboard` | 不自动打开终端面板，不影响执行或状态落盘。 |

可用内存低于约 1.5 GiB 时暂停新启动，状态中的 `waiting_for_memory` 为 true；复用结果不受该门槛影响。任务失败不阻止其他任务，配置错误在执行前拒绝。

仅对明确的服务拥堵、短时限流或连接中断等暂时性错误自动重试。额度耗尽、认证失败、partial、blocked、任务超时和结果格式错误不自动重试。已观察到工具执行时，默认停止自动重试，避免重复副作用。即使显式使用 `--retry-failed`，主代理仍须先判断从头执行是否安全。

重新提交会生成新的调用句柄。同一工作区按指纹复用已有终态；尚未执行过的任务仍可执行。崩溃遗留的待重试计划可由新批次接管；主动取消关闭计划，普通提交不能将它复活。接管归属会落盘，因此取消旧批次不会停止后来已经接管计划的新批次。若要重做符合条件的非成功终态，例如只重试 `module-a`，使用 `--submit --run-dir "$RUN" --only module-a --retry-failed`，这会开启新链。新尝试从任务输入重新启动 EXEC，保留旧尝试证据；模型对话不会续接。

执行结局和结果文件可读性分别记录。编号回执是执行事实的权威，`receipt.json` 只是兼容指针；指针缺失、过时或写入失败不会授权重跑。已观察到成功退出及成功 turn，但结果缺失或无效时，保留 `execution: succeeded`，通过 `result_observation` 和待处理项报告证据问题。没有核实结局的崩溃记录不会自动重放；仅有模型输出文件不足以证明执行已成功退出。

取消、超时及重试前检查共用本机进程归属模块：每次尝试启动前保存随机归属标记，结合绑定的线程、宿主 PID、启动时间及进程账本识别工具程序，信号使用 PID 身份核验。它覆盖可观察的本机进程，包括正常继承标记但另起进程组的工具。记录清理范围和未核实项；远端作业、已发生的外部副作用，以及逃离本机可观察范围的程序不属于可撤销保证。

## 登录通道

默认 `--channel sub` 使用已保存的 ChatGPT 订阅登录。用户指定 API 时用 `--channel api`，读取 `~/.config/codexexec/api-key`。通道不可用时报错，不自动切换或修改登录状态。其他隔离设置见 `scripts/codexexec` 和 `scripts/dispatch.py` 的启动命令。

## 增量结果与最终简报

每次调用在独立的 `runs/<run-id>/` 落盘：

- `result-events.jsonl`：运行中追加。任务在本次调用达到最终结果时只写一行，含单调递增的游标；还会重试的暂时性失败只保留在尝试证据中。
- `report.json`：最终简报，含状态、计数、复用数、自动重试数、待处理数、最多 20 项异常预览和产物路径。
- `results.jsonl` 与 `attention.jsonl`：最终完整结果与待处理项索引。每行含任务状态、summary、artifacts、issues 和证据路径。非 complete、结果无法读取或 issues 非空的任务需要处理。
- `summary.json`：完整回执汇总，供旧消费者使用。
- `status.json`、`resources.jsonl`、`events.jsonl`：调度状态、资源采样和调度事件。
- `config.json`、`common.md`：提交时的任务元数据与共享提示词快照；历史面板不读取后来修改的任务配置。
- `bindings/<id>.json`：该批次的任务与尝试绑定；`attempts/<id>/` 记录本批次实际发起的每次尝试。复用别的批次结果不会取得其进程所有权。
- `submission.json`、`dispatcher.log`：后台提交记录与调度进程日志；`cancel_request.json` 保存取消请求，`cancellation.json` 保存清理结果。

增量读取从 `--after 0` 开始，用返回的 `cursor` 翻页，`--limit` 为 1–100（默认 20）；`--attention` 可筛选待处理项。为等待与逐页读取分别保存游标，避免用一次等待的进度跳过尚未消费的结果。省略 `--after` 时仍支持旧式 `--offset 20 --limit 20` 分页。批次运行中和结束后均按 `result-events.jsonl` 的提交顺序翻页；没有该文件的旧存档按 `results.jsonl` 原有顺序读取。需要按任务清单顺序合并所有产物时，直接处理 report 指向的 `results.jsonl`。

尝试证据保存在 `workers/<id>/` 的 `prompt-NN.txt`、`events-NN.jsonl`、`stderr-NN.log`、`receipt-NN.json` 和 `result-NN.json`；`control.json` 保存当前调度阶段、重试链和接管归属，进程账本按尝试单独保存。核对时优先使用结果行绑定的回执和结果路径。耗时字段分别记录排队等待（`queue_wait_seconds`）、重试退避（`retry_backoff_seconds`）和实际执行（`execution_elapsed_seconds`）；`batch_elapsed_seconds` 是整批经过时间。

退出码 0 表示命令正常返回。批次 `state: complete` 表示调度正常走到结束，仍可能有任务失败或待处理；只有正常结束且没有待处理项时，`report.ok` 才为 true。验收时检查 `ok`、`counts` 和 `attention_count`。

`automatic_retries_started` 统计本批次实际启动的自动重试。兼容字段 `automatic_retries` 记录本批次安排或接管的重试链预算计数，恢复旧链时可能包含前一批已消耗的预算；仅复用终态时为 0。

## 通知接入边界

本机 codex-cli 0.164.0 的隔离实验已验证：App Server 的子会话完成后，客户端能收到 `turn/completed`；客户端再显式调用隔离父会话的 `turn/start`，父会话可以消费完成消息。这验证了事件源与显式转接链路，未验证或实现当前桌面主对话的自动唤醒。现有 EXEC 调度器仍以 `--wait` 接回主代理。

后续接入应由宿主侧转接程序消费调度器的终态结果，去重并合并普通成功，只在待处理项、批次终态或调度器失联时通知主代理。不要直接把每个 worker 的完成事件变成一次主对话生成；单次 turn 结束也不等于任务验收通过或重试已经结束。转接程序还需处理主对话忙碌、重连补读、重复通知与用户停止，不能用一个独立 App Server 冒充桌面的会话所有者。
