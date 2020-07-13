#!/usr/bin/env python

import argparse
import json
import logging
import os
import threading
import traceback

from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from io import BytesIO

from fuse import FUSE

from .cuttlefs import CuttleFS
from .fsyncs import SUPPORTED_FSYNC_CLASSES

def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("root",
        help="Existing folder to act as root",
    )
    parser.add_argument("mount",
        help="Mount point for fuse filesystem",
    )

    parser.add_argument("--fs-metadir", default=os.getcwd(),
        help="meta directory for file system, defaults to %(default)s",
    )

    parser.add_argument("--log-level", default="info",
        choices=("debug", "info", "warn", "error"),
        help="Log Level, defaults to %(default)s",
    )

    parser.add_argument("--foreground", default=False,
        action="store_true",
        help="run fuse in foreground? defaults to %(default)s",
    )

    parser.add_argument("--nothreads", default=False,
        action="store_true",
        help="run fuse without threads? defaults to %(default)s",
    )

    parser.add_argument("--fsync-behavior", default="ext4-ordered",
        choices=list(SUPPORTED_FSYNC_CLASSES.keys()),
        help="simulates fsync behavior, defaults to %(default)s",
    )

    parser.add_argument("--port", type=int, default=8888,
        help="port to run http server, defaults to %(default)s",
    )

    helpstr = (
        'File containing a json list of dictionaries of the form\n'
        '{"path": "/foo", "seq": "xxwxW", "block": 4}\n'
        'OR\n'
        '{"path": "/foo", "seq": "xxwxW", "sector": 4}\n'
    )
    parser.add_argument("--fault-list-file", default=None, help=helpstr)

    args = parser.parse_args()
    args.root = Path(args.root).resolve().as_posix()
    args.mount = Path(args.mount).resolve().as_posix()
    args.fs_metadir = Path(args.fs_metadir).resolve()
    args.fsync_behavior = SUPPORTED_FSYNC_CLASSES[args.fsync_behavior]
    assert hasattr(args.fsync_behavior, "on_fsync")
    assert hasattr(args.fsync_behavior, "on_sync_write")

    return args

def configure_logger(log_level):
    log_level = log_level.upper()
    level = getattr(logging, log_level)
    logging.basicConfig(level=level)

class HTTPRequestHandler(BaseHTTPRequestHandler):
    # NOTE fs is set by the caller of http server
    fs = None

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        self.send_response(200)
        self.end_headers()

        response = BytesIO()
        try:
            jdata = json.loads(body)
            resp = self.fs.handle_command(jdata)
        except Exception:
            exception = traceback.format_exc()
            resp = {"success": False, "exception": exception}

        response.write(json.dumps(resp).encode('utf8'))
        self.wfile.write(response.getvalue())

def main():
    args = get_args()
    configure_logger(args.log_level)

    log = logging.getLogger(__name__)
    log.info("mounting %s at %s", args.root, args.mount)

    fault_list = []
    if args.fault_list_file is not None:
        log.info("loading faultlist %s", args.fault_list_file)
        with open(args.fault_list_file, 'r') as fp:
            fault_list = json.load(fp)

    fs = CuttleFS(args.root, args.fs_metadir, args.fsync_behavior, fault_list)

    HTTPRequestHandler.fs = fs
    server = HTTPServer(('localhost', args.port), HTTPRequestHandler)
    server_thread = threading.Thread(target=server.serve_forever)
    fs.server = server
    fs.server_thread = server_thread
    fs.server_thread.start()

    fuse = FUSE(
        fs,
        args.mount,
        raw_fi=True,
        foreground=args.foreground,
        nothreads=args.nothreads,
        # direct_io is required so that our responses for read are not
        # cached in the kernel. Every single read/write system call should
        # be sent to us.
        direct_io=True,
        # This option is required so that fuse does not generate its own
        # inodes. Instead, we return the same inode number for the underlying
        # file in the root dir
        use_ino=True,
    )

if __name__ == '__main__':
    main()
