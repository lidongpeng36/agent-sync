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
sys.exit(subprocess.call(["/bin/sh", "-c", command], env=env, cwd=remote_home))
