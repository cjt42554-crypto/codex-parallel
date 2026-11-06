---
name: codex-parallel
description: 用本地 Codex EXEC 队列批量并行执行独立任务，支持批次默认模型与推理强度、有界并发、有限重试和异常汇总。适用于大量独立文件、模块、资料或实验，或用户明确要求批量并行；不适用于简单单项任务、强依赖串行流程或并发修改同一文件。
---

# Codex 并行调度

主代理定义任务边界、提交一份清单、处理汇总中的异常。调度器负责排队、限流、重试和收集结果，不自动拆任务。大量同类任务用脚本生成清单；过小的工作可合并为一个任务，减少进程和上下文开销。

默认采纳子任务报告，不逐项重做或复审。只在异常、必要抽查或共同产物的集成检查时打开具体任务；不要逐个轮询 worker 或把全部日志读回主对话。每次启动与重试都会显式传入模型和推理强度。

## 提交

`$SKILL` 为本 SKILL.md 所在目录，`$RUN` 为本批次独立工作目录。

```bash
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --init
```

把简短共享上下文写入 `$RUN/COMMON.md`，任务写入 `$RUN/tasks.json`：

```json
{
  "defaults": {"model": "gpt-6.1-sol", "effort": "high"},
  "tasks": [
    {"id": "module-a", "prompt": "输入绝对路径、工作范围、产物及验收要求。"},
    {"id": "module-b", "effort": "medium", "prompt": "另一个可独立推进的任务。"}
  ]
}
```

任务字段覆盖 defaults；title 可省略。仍兼容每项完整指定配置的旧式数组清单。模型和推理强度必须由清单明确给出。

worker 仅获得 COMMON 与自己的 prompt，不继承主对话。默认在 `workers/<id>/` 工作；修改项目时为任务指定 `write_dirs` 绝对路径数组，并分配互不重叠的文件。目录写权限不提供文件锁或工作树隔离。worker 默认关闭 apps、嵌套多代理、记忆、浏览器、电脑操作和网页搜索。

```bash
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --concurrency 16
```

默认并发为 8，逐步提升到设置的上限；内存不足时暂停新启动。并发量按机器与账号容量选择，大清单不意味着同时启动所有任务。调度命令持续运行到结束；主代理可在此期间做独立工作。

## 收结果

```bash
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --status
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --report
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --results --attention
```

运行中按需看 status，结束先看 report：状态计数、复用数量、自动重试次数、待处理项及结果索引路径。成功且无问题的任务不需要逐项打开。`--results --attention` 分页读取异常；`--results --offset 20 --limit 20` 读取下一页完整结果。需要合并所有产物时，用脚本处理 report 指向的 `results.jsonl`，无需读取每个 worker 的提示词和日志。

默认标准输出仅给出开始与结束状态；自动面板使用系统默认终端，跨批次复用一个总览。日志、逐次回执和完整结果保存在运行目录。

## 继续与重试

相同输入再次运行会复用已有终态；修改输入或执行配置时必须换任务 ID 或目录，防止错用旧结果。仅重跑指定未完成任务：

```bash
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --only module-a --retry-failed
```

每次重跑是新的 EXEC，会保留旧尝试证据。明确的暂时性服务/连接错误默认最多自动重试 2 次，并退避；观察到工具执行后默认停止自动重试。partial、blocked、超时和格式错误不自动重试。需要调整重试策略、通道、权限或运行参数时，读 [执行参数与产物](references/execution.md)。
