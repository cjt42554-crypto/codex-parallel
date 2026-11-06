"""Ephemeral monitor index: the OS removes the runtime directory on reboot."""
import os
from pathlib import Path


def registry_directory():
    runtime = Path('/run/user') / str(os.getuid())
    if not runtime.is_dir() or runtime.stat().st_uid != os.getuid():
        raise RuntimeError('用户运行时目录不可用；不保存面板历史登记')
    directory = runtime / 'codex-parallel'
    directory.mkdir(mode=0o700, exist_ok=True)
    return directory
