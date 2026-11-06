# codex-parallel

**Parallel Codex CLI tasks, with a terminal workflow monitor.**

**Codex CLI 并行任务调度器，让每个 EXEC 的状态、提示词和回答都可见。**

Run independent **Codex CLI (`codex exec`)** tasks in parallel from a JSON task manifest. Set a model and reasoning effort for each task, bound concurrency, retry transient failures, and inspect batch results in an interactive terminal UI (TUI).

Use it as a **Codex skill** or a standalone **Python task runner** for parallel coding agents, module reviews, document processing, and independent experiments. The parent agent defines task boundaries; the queue executes the submitted tasks and collects their results.

- **Batch execution:** shared defaults with per-task overrides, concurrency limits, and memory-aware queueing.
- **Recoverable runs:** input fingerprints, per-attempt evidence, bounded retries, and structured JSON/JSONL reports.
- **Terminal visibility:** switch between batches and inspect each task's status, prompt, and final answer without opening its execution logs.
- **Small local footprint:** Linux-native, Python standard library only, no database or additional Python packages.

一个轻量的本地 Codex CLI 并行任务调度工具，附带终端工作流面板，支持批量执行、失败重试、异常汇总和跨批次查看。适合独立模块检查、资料处理与实验任务；可作为 Codex skill 使用，也可以直接运行 Python 脚本。主代理负责拆分任务和判断下一步，调度器负责执行清单、限制并发、保存证据和汇总异常。

Unofficial community tooling; not affiliated with or endorsed by OpenAI. Licensed under [MIT](LICENSE).

## 功能

- **独立任务并行执行**：支持批次默认模型与推理强度，任务可以单独覆盖；每次调用显式传入配置。
- **受控并发与内存门槛**：逐步增加并发，内存不足时暂停新任务。
- **运行目录锁**：防止同一运行目录被重复调度，不同目录可以并行。
- **逐次尝试记录**：独立保存提示词、日志、回执和结果；输入指纹防止复用错误结果。
- **有限自动重试**：默认只重试明确的暂时性连接或服务错误；观察到工具执行后停止自动重跑，除非任务显式声明 `retry_safe`。
- **批次报告**：精简摘要、异常列表、分页结果与完整 JSONL 索引。
- **终端面板**：跨批次切换，查看状态、提示词和最终回答正文；退出面板不会停止任务。

## 环境要求

- **Linux**：使用 `/proc`、`fcntl` 和 `/run/user/<uid>`。目前不支持原生 Windows/macOS。
- Python **3.10+**，包含 `curses`；没有第三方 Python 包依赖。本地验证环境为 Python 3.14.4。
- 已安装、登录且可执行 `codex exec` 的 Codex CLI。本地验证版本为 `codex-cli 0.164.0`，其他版本的参数兼容性请自行核对。
- 当前 `scripts/codexexec` 从 **`~/.local/bin/codex`** 启动 CLI。如果你的 CLI 安装在其他位置，在该路径建立指向现有可执行文件的符号链接，或调整包装脚本中的 `codex` 路径。不要覆盖已经存在的文件。
- 自动弹出面板使用系统默认终端：优先 `xdg-terminal-exec`，再使用 `x-terminal-emulator`。无桌面环境可传入 `--no-dashboard`，在已有终端中手动运行面板。

仓库中的模型名称是配置示例，不保证所有账户可用。请使用自己环境实际支持的模型 ID 与推理强度。

## 安装与快速开始

将仓库作为 Codex skill 安装；目标目录需要不存在：

```bash
git clone https://github.com/cjt42554-crypto/codex-parallel.git ~/.codex/skills/codex-parallel
```

也可以克隆到其他位置，直接运行脚本。

```bash
SKILL="$HOME/.codex/skills/codex-parallel"
RUN="$HOME/codex-runs/demo-$(date +%Y%m%d-%H%M%S)"
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --init
cp "$SKILL/examples/COMMON.md" "$RUN/COMMON.md"
cp "$SKILL/examples/tasks.json" "$RUN/tasks.json"
```

先编辑 `$RUN/tasks.json`，将 `YOUR_MODEL_ID` 替换为你可用的模型，再执行：

```bash
SUBMISSION=$(python3 "$SKILL/scripts/dispatch.py" --submit --run-dir "$RUN" --concurrency 2)
printf '%s\n' "$SUBMISSION"
BATCH=$(printf '%s\n' "$SUBMISSION" | python3 -c 'import json, sys; print(json.load(sys.stdin)["batch_id"])')
```

`--submit` 启动独立的后台调度进程。保存返回的 `batch_id`（批次目录的绝对路径），后续等待、查询和取消都可用它指定本次调用。`starting` 表示调度器尚未确认就绪；`startup_failed` 时查看返回的 `error` 和 `log_path`。省略 `--submit` 可在前台运行，命令会等到批次结束。

提交会调用真实模型，使用对应订阅或 API 额度。示例任务只在各自 worker 目录创建小文件。

清单支持对象形式，也兼容完整指定配置的旧式数组：

```json
{
  "defaults": {"model": "YOUR_MODEL_ID", "effort": "high"},
  "tasks": [
    {"id": "module-a", "prompt": "明确的输入路径、任务范围、产物和验收要求。"},
    {"id": "module-b", "effort": "medium", "prompt": "另一个独立任务。"}
  ]
}
```

修改项目文件时，通过任务的 `write_dirs` 指定已存在的绝对目录，并分配互不重叠的文件。目录写权限不提供工作树隔离或文件锁。worker 不继承主对话，只接收 `COMMON.md` 与自己的提示词。

## 查看结果

```bash
WAIT_CURSOR=0
RESULT_CURSOR=0
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --wait --after "$WAIT_CURSOR" --wait-seconds 60
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --status
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --report
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --results --after "$RESULT_CURSOR" --limit 20
```

`--wait` 在出现待处理项、批次进入终态或等待超时时返回；普通成功不会单独触发返回，等待超时不会停止批次。收到 `terminal` 时查看 `state`，区分正常结束、中断、启动失败和调度器失联。继续等待时，把该命令返回的 `cursor` 赋给 `WAIT_CURSOR`。`--results` 在运行中和结束后都可翻页，可加 `--attention` 只看待处理项；将它返回的 `cursor` 单独赋给 `RESULT_CURSOR`，避免跳过尚未读取的普通成功项。

`--run-dir "$RUN" --status` 是按清单查询的兼容入口。它分别报告最近提交（包括启动失败）和当前活跃批次，二者可能不同。用 `--batch "$BATCH"` 可固定查询同一次调用。

批次结束后，`report.json` 提供计数、复用、重试和待处理项概览；`results.jsonl` 保存完整结果与证据路径。**退出码 0 表示命令正常返回；批次 `state: complete` 表示调度结束。** 验收应检查报告的 `ok`、`counts` 和 `attention_count`。如果已核实执行成功，但结果文件缺失或无效，会保留 `execution: succeeded` 并通过 `result_observation` 报告证据问题。这类尝试会进入待处理列表，重试时仍复用已有执行。

## 取消批次

```bash
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --cancel --wait-seconds 60
```

取消必须显式指定 `--batch`。命令先保存取消请求并关闭该批次当前拥有的重试计划，再停止调度器和可核实归属的本机进程。检查返回的 `stopped`、`outcome` 和清理范围；若为 `cleanup_pending` 或清理尚未确认，继续用同一句柄查状态和批次目录中的 `cancellation.json`，必要时再次发出取消请求。取消和单次尝试超时共用本机进程归属检查，覆盖仍可识别的独立进程组；远端作业、已发生的外部副作用和无法核实归属的本机进程不在可确认清理的范围内。

## 终端面板

```bash
# 在当前终端打开批次列表
python3 "$SKILL/scripts/dashboard.py" --allow-multiple

# 直接打开指定批次
python3 "$SKILL/scripts/dashboard.py" --run-dir "$RUN" --allow-multiple

# 用系统默认终端打开总览
python3 "$SKILL/scripts/dashboard.py" --launch
```

| 按键 | 操作 |
| --- | --- |
| `↑` / `↓` 或 `k` / `j` | 选择批次或任务 |
| `Enter` | 进入选中的批次 |
| `PgUp` / `PgDn` | 翻阅提示词与回答 |
| `Esc` | 从任务返回批次列表；在批次列表退出 |
| `r` | 刷新 |
| `q` | 退出面板，任务继续 |

可在 shell 配置中添加快捷入口：

```bash
alias codex-parallel-monitor='python3 "$HOME/.codex/skills/codex-parallel/scripts/dashboard.py" --allow-multiple'
```

如果希望命令名为 `parallel`，请先检查是否已安装同名的 GNU Parallel；本仓库不会覆盖它，也不包含维护者本机的同名包装命令。

面板只是独立观察器，启动失败或退出不影响调度。批次登记存放在 `/run/user/<uid>/codex-parallel`，重启或用户运行时会话被清理后消失。**任务目录内的日志、提示词和产物不会自动删除。**

## 继续与重试

```bash
python3 "$SKILL/scripts/dispatch.py" --submit --run-dir "$RUN" --only numbers --retry-failed
```

再次提交会获得新的 `batch_id`。相同输入会复用已核实的终态；用 `--retry-failed` 可为符合条件的失败、模型声明的 `partial`/`blocked` 或中断任务开启新重试链，前提是确认旧任务已退出且可以安全重做。每次重试都是新的 EXEC，从任务输入开始，保留旧尝试证据。编号回执记录执行事实，兼容指针缺失或过时、崩溃时执行结局未核实，都不会自动授权重跑。

崩溃后接管待重试的旧链会保留原有预算、策略和退避截止时间；新提交参数只用于新链。主动取消后，普通提交不会恢复已关闭的重试计划；取消旧批次也不会停止已由新批次接管的计划。同一 ID 的提示词或执行配置变化时，应使用新 ID 或新运行目录。

完整参数、自动重试条件、登录通道与产物说明见 [执行参数与产物](references/execution.md)。模型输入可引用的外部文件内容不会自动纳入指纹；外部输入变化时也应使用新的任务 ID 或目录。

## 登录与数据

默认使用保存的 ChatGPT 登录。只有显式传入 `--channel api` 才从 `~/.config/codexexec/api-key` 读取 API key；不会自动更换通道，也不会修改登录状态。认证文件和密钥应保存在仓库外，不要提交到 Git。

worker 使用 `workspace-write`、`approval_policy="never"`，默认关闭 apps、嵌套多代理、记忆、浏览器、电脑操作和网页搜索。调度器仍会启动真实模型任务，授权范围由你的任务内容及可写目录决定。

## 测试与开发

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py' -v
```

当前测试套件共 89 项，使用临时目录、模拟 EXEC 和模拟终端启动器，无需 Codex 登录、API key 或模型调用。覆盖后台提交、取消及本机进程归属、重试链恢复、执行结果与证据的区分、增量分页、计时、面板和规模行为。

## 范围与当前限制

- 处理独立批量任务，不提供阶段依赖图或自动工作流规划。
- 必须由主代理保证任务边界与写入范围合理；结果通过格式校验不等于内容正确。
- 多个批次的并发上限分别生效，没有账户级全局并发控制。
- 自动终端启动遇到“启动器仍存活但面板未出现”的异常时可能持续等待；手动打开面板不经过该路径。
- 面板的进程存活判断在 PID 被复用等少见情况下可能显示旧状态；详细命令行状态还会校验调度器启动时间。

## License

[MIT](LICENSE). Contributions and issue reports are welcome. This repository is not affiliated with or endorsed by OpenAI.
