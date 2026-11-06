---
name: codex-parallel
description: 用本地 Codex EXEC 队列批量并行执行独立任务，支持批次默认模型与推理强度、有界并发、有限重试和异常汇总。适用于大量独立文件、模块、资料或实验，或用户明确要求批量并行；不适用于简单单项任务、强依赖串行流程或并发修改同一文件。
---

# Codex 并行调度

主代理定义任务、提交清单、处理异常并验收共同产物。调度器负责排队、限流、重试和收集结果。大量同类任务可用脚本生成清单；小任务可合并。默认采纳子任务报告，只在异常、必要抽查或集成验收时查看具体证据。

## 定义并提交

`$SKILL` 为本文件所在目录，`$RUN` 为任务工作区。

```bash
python3 "$SKILL/scripts/dispatch.py" --run-dir "$RUN" --init
```

把共享背景写入 `COMMON.md`，把独立任务写入 `tasks.json`：

```json
{
  "defaults": {"model": "gpt-6.1-sol", "effort": "high"},
  "tasks": [
    {"id": "module-a", "prompt": "写明输入绝对路径、范围、产物和验收要求。"},
    {"id": "module-b", "effort": "medium", "prompt": "另一个可独立推进的任务。"}
  ]
}
```

每次启动和重试都要显式指定模型与推理强度，可由 defaults 提供，任务可覆盖。worker 只收到共享背景与自己的提示词。需要改项目时，用 `write_dirs` 指定可写目录并划分互不重叠的文件。

```bash
python3 "$SKILL/scripts/dispatch.py" --submit --run-dir "$RUN"
```

提交返回启动状态、`run_id` 和绝对路径 `batch_id`。检查启动状态，再保存 `batch_id` 作为 `$BATCH`；后续始终用它定位这次调用。

## 等待并收结果

第一次等待前把 `CURSOR` 设为 0。

```bash
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --wait --after "$CURSOR" --wait-seconds 60
python3 "$SKILL/scripts/dispatch.py" --batch "$BATCH" --report
```

下一次等待传回上次返回的 `cursor`。等待在出现待处理项、批次结束、调度进程失联或到达等待时限时返回。等待超时后，批次继续运行；主代理可以先做独立工作，再回来等待。不要逐个轮询 worker；终端面板供人查看，当前没有自动唤醒主对话的机制。

运行中需要产物时，用独立的结果游标 `--results --after RESULT_CURSOR --limit 20` 增量读取终态任务；加 `--attention` 只看待处理项。等待会越过普通成功项，因此不要拿等待游标代替完整结果游标。批次结束后先看 report 的 `ok`、计数与异常预览，按需读取证据或整合产物，把获授权的工作跟进到验收。遇到失联或取消，先核对真实状态与清理情况。

批次没有总时限；默认每次尝试也不限时，静默只告警。取消、重跑、兼容命令及高级参数见[执行参数与产物](references/execution.md)。
