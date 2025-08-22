# -*- coding: utf-8 -*-
"""
Created on Sat May 31 00:09:19 2025

@author: hkumar
"""

import os
import psutil
import logging



def kill_child_processes(parent_pid=None, sig=15):
    """
    Kill all child processes of the current process.
    `sig=15` sends SIGTERM. Use `sig=9` for SIGKILL (force kill).
    """

    parent_pid = parent_pid or os.getpid()
    try:
        parent = psutil.Process(parent_pid)
    except psutil.NoSuchProcess:
        return

    children = parent.children(recursive=True)
    for child in children:
        try:
            logging.info(f"Terminating child process: PID={child.pid}, Name={child.name()}")
            child.send_signal(sig)
        except Exception as e:
            logging.error(f"Error terminating child PID={child.pid}: {e}")
