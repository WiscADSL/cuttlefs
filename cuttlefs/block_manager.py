# CuttleFS maximizes the use of the underlying file system except for
#  data-block mapping, timestamps, and file size.
# It uses the underlying file system for directory structure, links,
# permissions, and ownership.
# TODO: what about inode numbers?

# The contents of a file are stored in PAGE_SZ blocks in a separate file.
# The underlying file contains the custom metadata such as the mapping,
# timestamps, and filesize.

# The block manager can also be configured to fail specific offsets for
# a given path at the block level or the sector level. Both block and
# sector can be specified as long as they do not overlap.

import json
import logging
import os

from pathlib import Path

from .constants import PAGE_SZ, SECTOR_SZ
from .failseq import FailSequence

class BlockManager(object):
    log = logging.getLogger("BlockManager")

    def __init__(self, path):
        assert isinstance(path, Path)

        self.path = path.absolute()
        self.metapath = Path(f'{self.path}.meta')
        self.size = 0
        self.free_list = []
        self.largest_block_num = 0
        self.faulty_paths = {}

        if not self.path.parent.exists():
            self.path.parent.mkdir(parents=True)

        if not self.metapath.exists():
            self._init_empty_files()

        self.fp = open(self.path, "r+")
        with open(self.metapath, "r") as fp:
            meta = json.load(fp)
            self.size = meta['size']
            self.largest_block_num = meta['largest_block_num']
            self.free_list = meta['free_list']

    def sync(self):
        self.log.info("syncing")
        self.fp.flush()
        os.fsync(self.fp.fileno())
        self.fp.close()

        meta = {
            "size": self.size,
            "largest_block_num": self.largest_block_num,
            "free_list": self.free_list,
        }

        with open(self.metapath, "w") as fp:
            json.dump(meta, fp)
            fp.flush()
            os.fsync(fp.fileno())

    def _init_empty_files(self):
        meta = {
            "size": PAGE_SZ,
            "largest_block_num": 0,
            "free_list": [],
        }

        with open(self.path, "wb") as fp:
            fp.truncate(PAGE_SZ)
            fp.flush()
            os.fsync(fp.fileno())

        with open(self.metapath, "w") as fp:
            json.dump(meta, fp)
            fp.flush()
            os.fsync(fp.fileno())

    def bread(self, bnum):
        self.log.info("BREAD block=%d", bnum)
        offset = bnum * PAGE_SZ
        assert offset < self.size
        return bytearray(os.pread(self.fp.fileno(), PAGE_SZ, offset))

    # Deterministic fault injection
    # We want to fault the i'th write to a block belonging to a file.
    # However, we cannot count on determinstic block allocation because
    # multithreaded applications might race to get blocks.
    # We cannot count on deterministic inodes for the same reason.
    # So path and offset seem like a good idea.
    # There may be confusion when a file is renamed. Should the fault
    # configuration for the original path be carried on by the new one?
    # For now, we only use path and offset ignoring effects of rename.
    def enable_failures_on(self, path, seq, idx, is_block=True):
        """
        By default, we assume region to be 4K unless is_sector is specified.
        """
        if not isinstance(seq, FailSequence):
            seq = FailSequence(seq)

        offset_seq_map = self.faulty_paths.setdefault(path, {})
        if is_block:
            offsets = [i for i in range(idx * PAGE_SZ, (idx + 1) * PAGE_SZ, SECTOR_SZ)]
        else:
            offsets = [idx * SECTOR_SZ]

        assert {i%SECTOR_SZ for i in offsets} == {0}, "idx must be sector or block aligned"

        for offset in offsets:
            assert offset not in offset_seq_map
            offset_seq_map[offset] = seq.copy()

    def bwrite(self, bnum, data, ref):
        """
        write `data` to block `bnum`.
        `ref` is a reference path and offset that is being written.
        We use ref to figure out what to fail.
        """
        assert len(data) == PAGE_SZ
        success = True

        path, offset = ref
        offset_seq_map = self.faulty_paths.get(path, {})

        # Writing data sector by sector to simulate cases where only
        # one sector fails.
        bfile_offset = bnum * PAGE_SZ
        file_offset = offset
        for i in range(PAGE_SZ // SECTOR_SZ):
            write_sector = True
            seq = offset_seq_map.get(file_offset)
            if seq is not None and seq.next() == 'x':
                write_sector = False
                success = False

            if write_sector:
                os.pwrite(self.fp.fileno(), data[i*512:(i+1)*512], bfile_offset)

            bfile_offset += 512
            file_offset += 512

        msg = {"block": bnum, "path": ref[0], "offset": ref[1], "bio_success": success}
        self.log.info("BWRITE %s", json.dumps(msg))

        # update file size if required
        if bfile_offset >= self.size:
            self.size = bfile_offset

        return success

    # Block allocation is intentionally simple. We aren't
    # optimizing for efficiency or minimizing seeks.
    # When we need a block, we first check for any recently
    # de-allocated blocks, maintained in the free_list.
    # If the free list is empty, we allocate from the next available
    # block in the file, growing the file if needed.
    def alloc_block(self):
        if len(self.free_list) > 0:
            block = self.free_list.pop()
            return block

        # NOTE: thread unsafe code.
        block = self.largest_block_num
        self.largest_block_num += 1
        return block

    def dealloc_block(self, block):
        self.free_list.append(block)

if __name__ == '__main__':
    bm = BlockManager(Path("/tmp/myblock"))
