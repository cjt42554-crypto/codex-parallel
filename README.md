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
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --concurrency 2
```

这一步会调用真实模型，使用对应订阅或 API 额度。示例任务只在各自 worker 目录创建小文件。

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
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --status
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --report
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --results --attention
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --results --offset 20 --limit 20
```

`report.json` 提供计数、复用和自动重试情况，以及需要关注的任务。`results.jsonl` 保存各任务摘要、产物、问题及证据路径。超过 20 个任务时，结束时的终端报告采用精简形式；具体失败项通过 `--results --attention` 查看。

**退出码 0 表示调度过程正常结束，不代表全部任务成功。** 自动化调用应检查 `report.ok`、`counts` 和 `attention_count`。任务可以正常返回 `blocked` 或 `partial`，进程退出码仍为 0。

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
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --only module-a --retry-failed
```

相同输入再次运行会复用已有终态，`--retry-failed` 重跑未完成项；已完成任务继续复用。每次重试都是新的 EXEC，不恢复旧对话。同一 ID 的提示词或执行配置变化时，应使用新 ID 或新运行目录。

完整参数、自动重试条件、登录通道与产物说明见 [执行参数与产物](references/execution.md)。模型输入可引用的外部文件内容不会自动纳入指纹；外部输入变化时也应使用新的任务 ID 或目录。

## 登录与数据

默认使用保存的 ChatGPT 登录。只有显式传入 `--channel api` 才从 `~/.config/codexexec/api-key` 读取 API key；不会自动更换通道，也不会修改登录状态。认证文件和密钥应保存在仓库外，不要提交到 Git。

worker 使用 `workspace-write`、`approval_policy="never"`，默认关闭 apps、嵌套多代理、记忆、浏览器、电脑操作和网页搜索。调度器仍会启动真实模型任务，授权范围由你的任务内容及可写目录决定。

## 测试与开发

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -p 'test_*.py' -v
```

测试使用临时目录、模拟 EXEC 和模拟终端启动器，无需 Codex 登录、API key 或模型调用。覆盖重复启动、错误隔离、重试、历史结果、批次汇总、面板缓存和规模行为。在本地执行上面的命令即可运行全部测试。

## 范围与当前限制

- 处理独立批量任务，不提供阶段依赖图或自动工作流规划。
- 必须由主代理保证任务边界与写入范围合理；结果通过格式校验不等于内容正确。
- 多个批次的并发上限分别生效，没有账户级全局并发控制。
- 自动终端启动遇到“启动器仍存活但面板未出现”的异常时可能持续等待；手动打开面板不经过该路径。
- 面板的进程存活判断在 PID 被复用等少见情况下可能显示旧状态；详细命令行状态还会校验调度器启动时间。

## License

[MIT](LICENSE). Contributions and issue reports are welcome. This repository is not affiliated with or endorsed by OpenAI.
