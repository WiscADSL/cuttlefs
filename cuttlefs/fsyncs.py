import errno
import json
import logging
import os

class GenericFsync(object):
    """
    Writes dirty pages out to disk and marks them clean regardless of failure.
    On failure, keeps the latest contents in memory.
    Reports failures immediately.
    Simulates behavior for ext4 ordered mode and XFS
    """
    log = logging.getLogger("GenericFsync")
    def __init__(self, fs):
        self.fs = fs
        # If there is a failure, then all open fd's for that inode should be
        # notified whenever they call fsync.
        # If there are no open file descriptors, the first one that calls fsync
        # should be notified.
        self.failed_inodes_fd_map = {}

    def _should_notify_fd(self, fd, inode):
        failed_fds = self.failed_inodes_fd_map.get(inode, None)
        if failed_fds is None:
            return False

        # first file descriptor that has been opened ever since
        # the failure occurred
        if len(failed_fds) == 0:
            return True

        return fd in failed_fds

    def _add_fds_to_notify(self, inode):
        all_open_fds = self.fs.inode_to_open_fds_map.get(inode, set())
        fd_set = self.failed_inodes_fd_map.setdefault(inode, set())
        fd_set.update(all_open_fds)

    def _mark_fd_notified(self, fd, inode):
        failed_fds = self.failed_inodes_fd_map.get(inode, set())
        failed_fds.discard(fd)
        if len(failed_fds) == 0:
            self.failed_inodes_fd_map.pop(inode, None)

    def on_close_fd(self, fd, inode):
        # when a file descriptor is closed, it can't be notified
        # anymore, so we need to clean up that state.
        failed_fds = self.failed_inodes_fd_map.get(inode, set())
        failed_fds.discard(fd)
        # Unlike _mark_fd_notified, we do not delete the mapping if
        # the set is empty. That way, a new fd that is opened after
        # a failure will see an empty list and will report the failure.

    def on_fsync(self, fd, inode, minode):
        dirty_pages = minode.get_dirty_pages()
        ret = self.sync_pages(minode, dirty_pages)
        self.sync_meta(minode)

        if ret != 0:
            self._add_fds_to_notify(inode)

        if self._should_notify_fd(fd, inode):
            ret = -errno.EIO
            self._mark_fd_notified(fd, inode)

        return ret

    def on_sync_write(self, fd, inode, minode, pages):
        """
        called when writing to a fd opened with O_SYNC
        """
        if self._should_notify_fd(fd, inode):
            # if there was an error, don't sync any pages
            # just report the error
            self._mark_fd_notified(fd, inode)
            return -errno.EIO

        ret = self.sync_pages(minode, pages)
        self.sync_meta(minode)

        if ret != 0:
            self._add_fds_to_notify(inode)
            self._mark_fd_notified(fd, inode)

        return ret

    def sync_pages(self, minode, pages):
        ret = 0
        for dirty_page in pages:
            if not dirty_page.flag_dirty:
                continue

            block = minode.offset_to_block.get(dirty_page.offset)
            if block is None:
                block = self.fs.block_manager.alloc_block()
                minode.offset_to_block[dirty_page.offset] = block

            # NOTE: all blocks are written at the same time to disk.
            # i.e. usually a single bio request.
            # Therefore, any error in a single block should not prevent
            # other blocks from being written out.

            # as seen in the kernel, set dirty bit before writing to disk.
            dirty_page.flag_dirty = False
            bsuccess = self.fs.block_manager.bwrite(block, dirty_page.content,
                ref=(minode.path, dirty_page.offset))

            if not bsuccess:
                ret = -errno.EIO

        return ret

    def sync_meta(self, minode):
        meta = {
            "size": minode.size,
            "atime": minode.atime,
            "mtime": minode.mtime,
            "offset_to_block": minode.offset_to_block,
        }

        with open(minode.realpath, 'w') as fp:
            json.dump(meta, fp, indent=True)

class Ext4Ordered(GenericFsync):
    log = logging.getLogger("Ext4Ordered")

class XFS(GenericFsync):
    log = logging.getLogger("XFS")

class Ext4Data(GenericFsync):
    """
    Reports failures on "next" fsync.
    """
    log = logging.getLogger("Ext4Data")
    def on_fsync(self, fd, inode, minode):
        # If there was a previous error, ext4 does not write anything to
        # the journal (Unlike ordered mode which writes data).
        if self._should_notify_fd(fd, inode):
            self._mark_fd_notified(fd, inode)
            return -errno.EIO

        dirty_pages = minode.get_dirty_pages()
        ret = self.sync_pages(minode, dirty_pages)
        self.sync_meta(minode)

        # Technically it should have just put it in the journal.
        # But we don't simulate the journalling - just late error reporting.
        # So this adds it's own fds as well as other fds to be notified on
        # next fsync. But we return 0 for this specific call.
        if ret != 0:
            self._add_fds_to_notify(inode)

        return 0

    def on_sync_write(self, fd, inode, minode, pages):
        if self._should_notify_fd(fd, inode):
            self._mark_fd_notified(fd, inode)
            return -errno.EIO

        ret = self.sync_pages(minode, pages)
        self.sync_meta(inode)

        # for the same reasons as for fsync, this is just simulated.
        # we will always return success since this was put in the journal.
        if ret != 0:
            self._add_fds_to_notify(inode)

        return 0

class Btrfs(GenericFsync):
    """
    Reverts to old state on failure
    """
    log = logging.getLogger("Btrfs")
    def on_fsync(self, fd, inode, minode):
        """
        make a full copy of the
        """
        if self._should_notify_fd(fd, inode):
            # TODO: would this revert anything else that has been modified?
            self._mark_fd_notified(fd, inode)
            return -errno.EIO

        dirty_pages = minode.get_dirty_pages()
        ret = self.sync_pages(minode, dirty_pages)
        if ret == 0:
            self.sync_meta(minode)
            return ret

        # data block write failed so we reload whatever is on disk.
        # NOTE : since we never modify a block in place, and allocate new blocks,
        # we just have to reload whatever metadata was on disk last time.
        # That will automatically give us the old block mapping.
        # NOTE : we remove the pages from the page cache here so that
        # it will get it from disk. Ideally we should reload them from disk
        # but since any future write will anyway get it from disk, this should
        # be fine for now.
        with open(minode.realpath, 'r') as fp:
            disk_meta = json.load(fp)

        minode.size = disk_meta["size"]
        minode.atime = disk_meta["atime"]
        minode.mtime = disk_meta["mtime"]
        minode.offset_to_block = {
            int(offset) : block
            for offset, block in disk_meta["offset_to_block"].items()
        }

        # remove all dirty pages from the page cache
        for page in dirty_pages:
            minode.offset_to_page.pop(page.offset, None)

        # all fds must be notified of the failure
        self._add_fds_to_notify(inode)
        self._mark_fd_notified(fd, inode) # failing this fd right now
        return ret

    def on_sync_write(self, fd, inode, minode, pages):
        # an O_SYNC fd write is the same as an fsync to btrfs
        return self.on_fsync(fd, inode, minode)

    def sync_pages(self, minode, pages):
        ret = 0
        old_blocks = [] # if no error, deallocate these
        new_blocks = [] # if error, deallocate these
        for dirty_page in pages:
            if not dirty_page.flag_dirty:
                continue

            # copy on write approach - always allocate a new block
            old_block = minode.offset_to_block.get(dirty_page.offset)
            if old_block is not None:
                old_blocks.append(old_block)

            block = self.fs.block_manager.alloc_block()
            minode.offset_to_block[dirty_page.offset] = block
            new_blocks.append(block)

            dirty_page.flag_dirty = False
            bsuccess = self.fs.block_manager.bwrite(block, dirty_page.content,
                ref=(minode.path, dirty_page.offset))

            if not bsuccess:
                ret = -errno.EIO
                # technically if there are multiple blocks, all are written
                # but since we are doing things sequentially, we can stop here
                # as we need to revert to old state
                break

        blocks_to_dealloc = old_blocks if ret == 0 else new_blocks
        for block in blocks_to_dealloc:
            self.fs.block_manager.dealloc_block(block)

        return ret

    def sync_meta(self, minode):
        meta = {
            "size": minode.size,
            "atime": minode.atime,
            "mtime": minode.mtime,
            "offset_to_block": minode.offset_to_block,
        }

        with open(minode.realpath, 'w') as fp:
            json.dump(meta, fp, indent=True)

SUPPORTED_FSYNC_CLASSES = {
    "ext4-ordered": Ext4Ordered,
    "ext4-data": Ext4Data,
    "xfs": XFS,
    "btrfs": Btrfs,
}
