#!/usr/bin/env python3
"""Test-only SSH stand-in: run the real helper/rsync inside an isolated home."""
import os
import subprocess
import sys

remote_home = os.environ["AGENT_SYNC_TEST_REMOTE_HOME"]
env = dict(os.environ, HOME=remote_home,
           XDG_CACHE_HOME=remote_home + "/.cache",
           XDG_DATA_HOME=remote_home + "/.local/share",
           XDG_CONFIG_HOME=remote_home + "/.config")
command = " ".join(sys.argv[2:])
if os.path.exists(remote_home + "/fail-push") and "--server" in command and "--sender" not in command:
    sys.exit(42)
status = subprocess.call(["/bin/sh", "-c", command], env=env, cwd=remote_home)
# Test-only fault after a successful receiver write, before manifest readback.
marker = os.path.join(remote_home, "corrupt-after-push")
if status == 0 and "--server" in command and "--sender" not in command and os.path.exists(marker):
    with open(marker) as handle:
        target = os.path.realpath(os.path.join(remote_home, handle.read().strip()))
    if os.path.commonpath([os.path.realpath(remote_home), target]) != os.path.realpath(remote_home):
        raise RuntimeError("fault target escapes isolated home")
    with open(target, "ab") as handle:
        handle.write(b"\ncorrupted after push\n")
sys.exit(status)
