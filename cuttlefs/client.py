import logging
import requests
import signal
import subprocess
import time

from pathlib import Path

class CuttleFSForegroundRunner(object):
    log = logging.getLogger("cuttlefs.runner")

    def __init__(self, root, mount_point, log_level="info",
        metadir=None, nothreads=True, fsync_behavior="generic",
        fault_list_file=None, port=8888,
        stdout_file=None, stderr_file=None):

        self.root = Path(root).resolve()
        self.mount_point = Path(mount_point).resolve()
        assert self.root.exists()
        assert self.mount_point.exists()

        self.log_level = log_level
        self.nothreads = nothreads
        self.metadir = metadir
        self.fsync_behavior = fsync_behavior
        self.fault_list_file = fault_list_file
        self.port = port

        self.stdout_file = Path(stdout_file).resolve() if stdout_file else None
        self.stderr_file = Path(stderr_file).resolve() if stderr_file else None

        self.proc = None
        self.stdout_fh = None
        self.stderr_fh = None

    def _gen_cmd(self):
        cmd = ["cuttlefs",
            "--foreground",
            "--log-level", self.log_level,
            "--fsync-behavior", self.fsync_behavior,
            "--port", str(self.port),
        ]

        if self.nothreads:
            cmd.append("--nothreads")

        if self.metadir is not None:
            cmd.extend(["--fs-metadir", self.metadir])

        if self.fault_list_file is not None:
            cmd.extend(["--fault-list-file", self.fault_list_file])

        cmd.append(self.root)
        cmd.append(self.mount_point)

        return cmd

    def mount(self):
        cmd = self._gen_cmd()

        kwargs = {}
        if self.stdout_file is not None:
            self.stdout_fh = open(self.stdout_file, 'ab')
            kwargs['stdout'] = self.stdout_fh

        if self.stderr_file is not None:
            self.stderr_fh = open(self.stderr_file, 'ab')
            kwargs['stderr'] = self.stderr_fh

        self.log.debug("running command %r", cmd)
        self.proc = subprocess.Popen(cmd, **kwargs)
        time.sleep(2)
        assert self.proc.poll() is None

    def _umount(self):
        if self.proc.poll() is None:
            # process still running so we can interrupt it
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(10) # wait 10 seconds
                self.log.debug("fuse process exited with returncode %d", self.proc.returncode)
                return
            except subprocess.TimeoutExpired:
                self.log.error("fuse process still alive after sigint!")

        # there was a timeout, so lets unmount using fusermount3?

        # does the mount point exist?
        if not self.mount_point.is_mount():
            self.log.warn("mount point does not exist anymore, doing nothing")
            return

        subprocess.check_call(["fusermount3", "-u", self.mount_point])
        time.sleep(2)
        assert not self.mount_point.is_mount()

    def umount(self):
        if self.proc is None:
            return

        self._umount()
        for fh in (self.stderr_fh, self.stdout_fh):
            if fh is not None:
                fh.close()

    def send_command(self, command):
        resp = requests.post(f"http://localhost:{self.port}", json=command)
        return resp.json()

    def __enter__(self):
        self.mount()
        return self

    def __exit__(self, *args):
        self.umount()
