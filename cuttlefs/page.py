# CuttleFS maintains its own user-space page cache.
#
# Each page in the page cache is backed by a block on disk.
# We use the BlockManager's bread and bwrite methods to read from / write to
# a block.

# Each page is associated with a specific offset in a file.
# When persisting a page, we query the inode's offset_to_block mapping.
# If a block exists, depending on whether the file system does in-place or
# copy on write block placement, we either use the same block or allocate
# a new one.
# If a block does not exist, a block must be allocated.

import errno
import json
import logging

from .constants import PAGE_SZ

class Page(object):
    __slots__ = (
        "inode", "offset", "content", "flag_dirty",
    )
    def __init__(self, inode, offset):
        self.inode = inode
        self.offset = offset
        self.content = bytearray(PAGE_SZ)
        self.flag_dirty = False

class MemInode(object):
    """
    In-memory representation of an inode. The on-disk file
    on disk only contains the metadata. All data is stored
    with the block manager.
    """
    __slots__ = (
        "inode", "path", "realpath", "offset_to_block", "atime", "mtime", "size",
        "offset_to_page"
    )
    def __init__(self, inode, path, realpath):
        self.inode = inode
        self.path = path
        self.realpath = realpath

        self.offset_to_block = {}
        self.atime = None
        self.mtime = None
        # TODO figure out when to change ctime?
        self.size = 0

        self.offset_to_page = {}

        with open(self.realpath, 'r') as fp:
            data = json.load(fp)
            self.atime = data['atime']
            self.mtime = data['mtime']
            self.size = data['size']
            # json does not let keys be integers, so we convert it here
            self.offset_to_block = {
                int(offset) : block
                for offset, block in data['offset_to_block'].items()
            }

    def get_dirty_pages(self):
        # TODO: for better performance, maintain a structure for dirty pages
        # so this doesn't have to be computed all the time
        return [p for p in self.offset_to_page.values() if p.flag_dirty]

    def __repr__(self):
        return f'MemInode({self.realpath}, size={self.size})'

    __str__ = __repr__
    __unicode__ = __repr__

class PageCache(object):
    log = logging.getLogger("PageCache")

    def __init__(self, block_manager):
        self.minode_map = {}
        self.block_manager = block_manager

    def get(self, inode, default=None):
        val = self.minode_map.get(inode, default)
        self.log.info("get(%d, default=%r) => %r", inode, default, val)
        return val

    def put(self, inode, minode):
        assert isinstance(minode, MemInode) and isinstance(inode, int)
        self.minode_map[inode] = minode
        self.log.info("put(%d, %r)", inode, minode)

    def contains(self, inode):
        return inode in self.minode_map

    def remove(self, inode):
        # NOTE: unsafe operation. Any dirty pages or unsyncd data will
        # be removed
        if inode in self.minode_map:
            del self.minode_map[inode]

    def checkpoint(self):
        for inode, minode in self.minode_map.items():
            dirty_pages = minode.get_dirty_pages()
            if len(dirty_pages) == 0:
                continue

            self.sync_inode(minode)
