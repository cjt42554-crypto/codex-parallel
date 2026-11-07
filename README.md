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
- **受控并发与内存门槛**：每批默认最多运行 24 个 EXEC，逐步增加并发，内存不足时暂停启动。
- **运行目录锁**：防止同一运行目录被重复调度，不同目录可以并行。
- **逐次尝试记录**：独立保存提示词、日志、回执和结果；输入指纹防止复用错误结果。
- **有限自动重试**：默认只重试明确的暂时性连接或服务错误；观察到工具执行后停止自动重跑，除非任务显式声明 `retry_safe`。
- **增量交付**：逐步收取完整答复，按任务编号记住已交付结果，结束时汇总批次状态。
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
python3 "$SKILL/scripts/dispatch.py" --submit --run-dir "$RUN" --concurrency 2
```

`--submit` 在后台启动调度并返回 `batch_id`。用同一 `$RUN` 等待最新批次，查询指定批次或取消时使用 `batch_id`。`startup_failed` 时查看 `error` 和 `log_path`。

提交会调用真实模型，使用对应订阅或 API 额度。示例任务只在各自 worker 目录创建小文件。

清单可使用带 `defaults` 的对象，或直接列出完整配置的任务数组：

```json
{
  "defaults": {"model": "YOUR_MODEL_ID", "effort": "high"},
  "tasks": [
    {"id": "module-a", "prompt": "明确的输入路径、任务范围、产物和验收要求。"},
    {"id": "module-b", "effort": "medium", "prompt": "另一个独立任务。"}
  ]
}
```

修改项目文件时，通过 `write_dirs` 指定已存在的绝对目录，并分配互不重叠的文件。调度器向独立 EXEC 传入任务提示词和可选的 `COMMON.md`，worker 加载适用的项目指令。

## 查看结果

```bash
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --wait --wait-seconds 60
```

重复执行同一条 `--wait` 命令，读取 `items` 中的完整答复，直到 `event: terminal`，再查看 `state` 和已有的 `summary`。调度器按任务编号记录已交付结果。EXEC 以 `status` 和 `answer` 交付，产物路径写在回答正文。

```bash
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --status
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --report
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --results --after 0 --limit 20
```

`--results` 可独立分页，用返回的 `cursor` 作为下一页的 `--after`；加 `--attention` 查看待处理项。`report.json` 提供批次统计，`results.jsonl` 保存完整结果与证据路径。验收时查看 `ok`、`counts` 和 `attention_count`。

## 取消批次

将提交返回的 `batch_id` 保存为 `$BATCH`，取消该次调用：

```bash
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --cancel --wait-seconds 60
```

检查返回的 `stopped`、`outcome` 和清理范围；若为 `cleanup_pending`，用同一批次句柄查看状态及 `cancellation.json`。

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

同一图形会话复用一个批次总览。批次登记存放在 `/run/user/<uid>/codex-parallel`，日志、提示词和产物保存在任务目录。

## 继续与重试

```bash
python3 "$SKILL/scripts/dispatch.py" --submit --run-dir "$RUN" --only numbers --retry-failed
```

再次提交会获得新的 `batch_id`，相同输入会复用已核实的终态。确认旧任务已退出且可以安全重做后，`--retry-failed` 可为符合条件的失败、`partial`、`blocked` 或中断任务开启新重试链。新 EXEC 从任务输入开始，保留旧尝试证据。

任务输入或执行配置变化时，使用新 ID 或新运行目录。完整参数、恢复条件和产物说明见 [执行参数与产物](references/execution.md)。

## 登录与数据

默认使用保存的 ChatGPT 登录。只有显式传入 `--channel api` 才从 `~/.config/codexexec/api-key` 读取 API key；不会自动更换通道，也不会修改登录状态。认证文件和密钥应保存在仓库外，不要提交到 Git。

worker 使用 `workspace-write`、`approval_policy="never"`，默认关闭 apps、嵌套多代理、记忆、浏览器、电脑操作和网页搜索。授权使用子代理时，可设置 `multi_agent: true` 并指定 `subagent_model` 和 `subagent_effort`。

## 测试与开发

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py' -v
```

测试使用临时目录、模拟 EXEC 和模拟终端启动器，覆盖后台提交、增量交付、取消、重试链恢复、计时和面板行为。

## 范围与当前限制

- 处理独立批量任务，不提供阶段依赖图或自动工作流规划。
- 必须由主代理保证任务边界与写入范围合理；结果通过格式校验不等于内容正确。
- 多个批次的并发上限分别生效，没有账户级全局并发控制。
- 自动终端启动遇到“启动器仍存活但面板未出现”的异常时可能持续等待；手动打开面板不经过该路径。
- 面板的进程存活判断在 PID 被复用等少见情况下可能显示旧状态；详细命令行状态还会校验调度器启动时间。

## License

[MIT](LICENSE). Contributions and issue reports are welcome. This repository is not affiliated with or endorsed by OpenAI.
