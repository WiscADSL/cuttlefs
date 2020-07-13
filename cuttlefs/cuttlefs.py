import datetime
import errno
import json
import logging
import os
import stat
import time

from collections import namedtuple
from pathlib import Path

from fuse import Operations, FuseOSError

from .constants import PAGE_SZ
from .page import Page, PageCache, MemInode
from .block_manager import BlockManager
from .fsyncs import SUPPORTED_FSYNC_CLASSES

stat_fields = tuple([i for i in dir(os.stat_result) if i.startswith('st_')])
statvfs_fields = tuple([i for i in dir(os.statvfs_result) if i.startswith('f_')])
# function to check if flags specify O_SYNC / O_DSYNC
has_sync_enabled = lambda flags: (flags & os.O_SYNC) or (flags & os.O_DSYNC)

class PassthroughOperations(object):
    """
    CuttleFS leverages the underlying file system
    for most operations such as directory manipulations,
    permissions, etc.
    They are implemented here in the PassthroughOperations class.
    """

    def post_init_validation(self):
        assert hasattr(self, 'log')
        assert hasattr(self, 'realpath')
        assert callable(self.realpath)
        parent = super(PassthroughOperations, self)
        if hasattr(parent, 'post_init_validation'):
            parent.post_init_validation()

    def access(self, path, amode):
        p = self.realpath(path)
        if os.access(p, amode):
            return 0

        raise FuseOSError(errno.EACCESS)

    def chmod(self, path, mode):
        p = self.realpath(path)
        return os.chmod(p, mode)

    def chown(self, path, uid, gid):
        p = self.realpath(path)
        return os.chown(p, uid, gid)

    def fsyncdir(self, path, datasync, fh):
        # fsyncdir ignores raw_fi and sends fh itself
        fsync = os.fsync
        if datasync != 0:
            fsync = os.fdatasync

        if fh != 0:
            return fsync(fh)

        # recreate by trying sync ~/mp/ or sync ~/mp/some/dir
        self.log.warn("fh=0, opening dir %s and calling fsync", path)
        p = self.realpath(path)
        dirfd = os.open(p, os.O_DIRECTORY)
        ret = os.fsync(dirfd)
        os.close(dirfd)
        if ret is None:
            return 0

        return ret

    # TODO mark as not implemented
    def link(self, target, source):
        # NOTE stat of both yield different inodes.
        # Fuse ignores the st_ino when we stat(file) unless we use -o use_ino
        src = self.realpath(source)
        dst = self.realpath(target)
        return os.link(src, dst)

    def mkdir(self, path, mode):
        p = self.realpath(path)
        return os.mkdir(p, mode)

    def mknod(self, path, mode, dev):
        p = self.realpath(path)
        return os.mknod(p, mode, dev)

    def readdir(self, path, fh):
        p = self.realpath(path)
        files = ['.', '..'] + [f.name for f in p.iterdir()]
        return files

    # TODO mark as not implemented
    def readlink(self, path):
        p = self.realpath(path)
        link = os.readlink(p)
        return link

    def rmdir(self, path):
        p = self.realpath(path)
        return os.rmdir(p)

    def symlink(self, target, source):
        # NOTE only get realpath for target
        # write the source as is
        # TODO can this cause issues for applications?
        dst = self.realpath(target)
        return os.symlink(source, dst)

   ########################## XATTR Support #################################
    def getxattr(self, path, name, position=0):
        # getxattr called multiple times for security.capability
        # always precedes a write operation
        # from https://sourceforge.net/p/fuse/mailman/fuse-devel/thread/0DEBAD03-97FD-4A55-B953-75964DEBE226%40caringo.com/
        # disable CONFIG_SECURITY_FILE_CAPABILITIES in kernel configuration to avoid this.
        p = self.realpath(path)
        return os.getxattr(p, name)

    def listxattr(self, path):
        p = self.realpath(path)
        return os.listxattr(p)

    def setxattr(self, path, name, value, options, position=0):
        p = self.realpath(path)
        return os.setxattr(p, name, value, options)

    def removexattr(self, path, name):
        p = self.realpath(path)
        return os.removexattr(p, name)
    ##########################################################################

FDInfo = namedtuple('FDInfo', ["inode", "path"])

class CuttleFS(PassthroughOperations, Operations):
    """
    CuttleFS implementation.
    """
    log = logging.getLogger("cuttlefs")

    def __init__(self, root, metadir, fsync_cls, fault_list=[]):
        self.root = Path(root)
        self.realpath = lambda x: self.root / x.lstrip("/")
        self.metadir = Path(metadir)
        self.block_manager = BlockManager(self.metadir / "block_manager")
        for item in fault_list:
            path, seq = item['path'], item['seq']
            if 'block' in item:
                self.block_manager.enable_failures_on(path, seq, item['block'], is_block=True)
            else:
                self.block_manager.enable_failures_on(path, seq, item['sector'], is_block=False)

        self.to_be_deleted = self.metadir / "to_be_deleted"
        if not self.to_be_deleted.exists():
            self.to_be_deleted.mkdir(parents=True)

        self.page_cache = PageCache(self.block_manager)
        self._last_fd_alloced = 3
        # fd => (path, inode)
        # we save the inode so that we don't have to keep calling 'stat'
        self.fd_info_map = {}
        self.inode_to_open_fds_map = {}
        # set of file descriptors which have O_SYNC / O_DSYNC flags.
        # Whenever anything is written, those pages have to be sync'd.
        self.sync_fds = set()

        assert fsync_cls in tuple(SUPPORTED_FSYNC_CLASSES.values())
        self.fsync_obj = fsync_cls(self)

        self.post_init_validation()

        self.server_thread = None

    def post_init_validation(self):
        assert hasattr(self, 'log')
        assert hasattr(self, 'realpath')
        assert callable(self.realpath)
        parent = super(PassthroughOperations, self)
        if hasattr(parent, 'post_init_validation'):
            parent.post_init_validation()

    def _alloc_fd(self, path, inode=None):
        """
        returns a non-decreasing integer to serve as a file descriptor
        and updates the fd->info map as well as inode->[fd1,fd2]
        """
        # we will only allocate fd's if the metadata file (realpath) exists.
        assert path.exists()

        # TODO: make this thread safe if using more than one thread
        new_fd = self._last_fd_alloced + 1
        self._last_fd_alloced = new_fd

        if inode is None:
            inode = os.stat(path).st_ino

        info = FDInfo(inode=inode, path=path)
        self.fd_info_map[new_fd] = info
        self.inode_to_open_fds_map.setdefault(inode, set()).add(new_fd)
        return new_fd

    def create(self, path, mode, fi):
        # TODO add support for O_DIRECT and O_NOFOLLOW
        assert not fi.flags & os.O_DIRECT
        assert not fi.flags & os.O_NOFOLLOW

        p = self.realpath(path)
        fd = os.open(p, fi.flags, mode)
        assert fd > 0
        os.close(fd)

        # initial metadata for this file
        now = time.time()
        meta = {
            "offset_to_block": {},
            "size": 0,
            "atime": now,
            "mtime": now,
        }
        # TODO: verify that this only truncates the file created
        # above and does not override any permissions.
        with open(p, "w") as fp:
            json.dump(meta, fp)

        # get the inode num and create in-mem inode
        inode = os.lstat(p).st_ino
        minode = MemInode(inode, path, p)
        assert not self.page_cache.contains(inode)
        self.page_cache.put(inode, minode)

        fi.fh = self._alloc_fd(p, inode)
        if has_sync_enabled(fi.flags):
            self.sync_fds.add(fi.fh)

        return 0

    def flush(self, path, fi):
        # Normally, this is where we should write anything we buffer
        # to kernel space. We don't care about it. If something goes
        # wrong, rerun the experiment and don't trust the previous results.
        return 0

    def fsync(self, path, datasync, fi):
        # NOTE: We don't differentiate between fsync and fdatasync
        fd = fi.fh
        fd_info = self.fd_info_map[fd]
        if not self.page_cache.contains(fd_info.inode):
            self.log.warning("Ignoring fsync %r, not in page cache", fd_info)
            return 0

        minode = self.page_cache.get(fd_info.inode)
        ret = self.fsync_obj.on_fsync(fd, fd_info.inode, minode)
        return ret

    def getattr(self, path, fh=None):
        p = self.realpath(path)
        if not p.exists():
            raise FuseOSError(errno.ENOENT)

        # using os.lstat instead of os.stat so that we stat
        # symlinks instead of following them
        s = os.lstat(p)
        stat_result = dict((key, getattr(s, key)) for key in stat_fields)

        # we only care about regular files for now. links, directories, etc
        # can go the passthrough path.
        if not stat.S_ISREG(s.st_mode):
            return stat_result

        inode = s.st_ino
        if not self.page_cache.contains(inode):
            minode = MemInode(inode, path, p)
            self.page_cache.put(inode, minode)

        minode = self.page_cache.get(inode)

        # overwrite the filesize, atime, mtime with values from page cache.
        stat_result['st_size'] = minode.size
        stat_result['st_atime'] = minode.atime
        stat_result['st_mtime'] = minode.mtime
        return stat_result

    def open(self, path, fi):
        # TODO add support for O_DIRECT and O_NOFOLLOW
        assert not fi.flags & os.O_DIRECT
        assert not fi.flags & os.O_NOFOLLOW

        p = self.realpath(path)
        inode = os.stat(p).st_ino
        if not self.page_cache.contains(inode):
            minode = MemInode(inode, path, p)
            self.page_cache.put(inode, minode)

        fi.fh = self._alloc_fd(p, inode)
        if has_sync_enabled(fi.flags):
            self.sync_fds.add(fi.fh)

        return 0

    def _get_page_for_offset(self, minode, offset):
        assert offset % PAGE_SZ == 0
        page = minode.offset_to_page.get(offset)
        if page is not None:
            return page

        page = Page(minode.inode, offset)
        minode.offset_to_page[offset] = page

        # if there is a block associated with the offset, read it from disk.
        # otherwise, a block will be allocated when we sync the inode.
        block = minode.offset_to_block.get(offset)
        if block is not None:
            page.content = self.block_manager.bread(block)

        return page

    def read(self, path, size, offset, fi):
        fd = fi.fh
        fd_info = self.fd_info_map[fd]
        if not self.page_cache.contains(fd_info.inode):
            minode = MemInode(fd_info.inode, path, fd_info.path)
            self.page_cache.put(fd_info.inode, minode)

        minode = self.page_cache.get(fd_info.inode)

        if size == 0:
            return b''

        if offset >= minode.size:
            return b''

        if (offset + size) >= minode.size:
            size = minode.size - offset

        buf = bytearray()
        remaining = size
        current_offset = offset
        while remaining > 0:
            page_num = current_offset // PAGE_SZ
            page = self._get_page_for_offset(minode, page_num * PAGE_SZ)
            pg_start = current_offset % PAGE_SZ
            pg_nbytes = min(remaining, PAGE_SZ - pg_start)
            data = page.content[pg_start: pg_start + pg_nbytes]
            buf.extend(data)

            current_offset += len(data)
            remaining -= len(data)

        # TODO update atime?
        return bytes(buf)

    def write(self, path, data, offset, fi):
        fd = fi.fh
        fd_info = self.fd_info_map[fd]
        size = len(data)

        if not self.page_cache.contains(fd_info.inode):
            minode = MemInode(fd_info.inode, path, fd_info.path)
            self.page_cache.put(fd_info.inode, minode)

        minode = self.page_cache.get(fd_info.inode)

        if size == 0:
            return 0

        dirty_pages = []
        sync_fd = False
        if fd in self.sync_fds:
            sync_fd = True

        data_idx = 0
        remaining = size
        current_offset = offset
        while remaining > 0:
            page_num = current_offset // PAGE_SZ
            page = self._get_page_for_offset(minode, page_num * PAGE_SZ)
            pg_start = current_offset % PAGE_SZ
            pg_nbytes = min(remaining, PAGE_SZ - pg_start)
            page.content[pg_start:pg_start + pg_nbytes] = data[data_idx: data_idx + pg_nbytes]
            page.flag_dirty = True
            dirty_pages.append(page)

            data_idx += pg_nbytes
            current_offset += pg_nbytes
            remaining -= pg_nbytes

        # update file size, mtime
        nbytes_written = size - remaining
        if (offset + nbytes_written) > minode.size:
            self.log.info("Updating size from %d to %d", minode.size, offset + nbytes_written)
            minode.size = offset + nbytes_written

        minode.mtime = time.time()
        self.log.info("Finished writing, inode %d, %r", fd_info.inode, minode)

        if sync_fd:
            ret = self.on_sync_write(fd, fd_info.inode, minode, dirty_pages)
            if ret < 0:
                return ret

        return nbytes_written

    def release(self, path, fi):
        fd = fi.fh
        if fd not in self.fd_info_map:
            self.log.error("Trying to release unknown fd %d", fd)
            return 0

        self.sync_fds.discard(fd)
        info = self.fd_info_map[fd]
        assert self.realpath(path) == info.path
        del self.fd_info_map[fd]

        # If proc was supposed to be notified of an error, now
        # it can't since it closed the fd.
        self.fsync_obj.on_close_fd(fd, info.inode)

        open_fds = self.inode_to_open_fds_map[info.inode]
        open_fds.remove(fd)
        if len(open_fds) != 0:
            return 0

        # this was the last open fd for this path
        del self.inode_to_open_fds_map[info.inode]
        if info.path.parent != self.to_be_deleted:
            return 0

        # this file was in the to_be_deleted folder so lets delete it
        # TODO: this code is exactly the same as suffix of self.unlink
        # The only reason we've placed it here is because the "path" argument
        # get's translated by realpath which we don't want in this case
        minode = self.page_cache.get(info.inode)
        if minode is None:
            minode = MemInode(info.inode, path, info.path)

        for block in minode.offset_to_block.values():
            self.block_manager.dealloc_block(block)

        # unlinked file need not be in page cache once all fd's are closed
        self.page_cache.remove(inode)
        return os.unlink(info.path)

    def rename(self, old, new):
        p_old = self.realpath(old)
        p_new = self.realpath(new)
        old_ino = os.stat(p_old).st_ino

        if p_new.exists():
            # safe to call unlink on 'new' instead of 'p_new'
            self.unlink(new)

        if not self.page_cache.contains(old_ino):
            # not in the page cache, so just renaming should be enough
            return os.rename(p_old, p_new)

        # it's in the page cache, so we need to modify paths
        minode = self.page_cache.get(old_ino)
        minode.realpath = p_new

        # if the inode has any open file descriptors, we need to change them
        new_fd_info = FDInfo(inode=old_ino, path=p_new)
        for fd in self.inode_to_open_fds_map.get(old_ino, []):
            self.fd_info_map[fd] = new_fd_info

        # TODO what about mtime? Technically just the directory mtime changes.
        # But take a look at what POSIX says must happen here..
        return os.rename(p_old, p_new)

    def statfs(self, path):
        # TODO is this correct?
        p = self.realpath(path)
        s = os.statvfs(p)
        return dict((key, getattr(s, key)) for key in statvfs_fields)

    def truncate(self, path, length, fh=None):
        p = self.realpath(path)
        # ignoring file handle / file descriptor.
        # will use path itself.
        inode = os.stat(p).st_ino
        if not self.page_cache.contains(inode):
            minode = MemInode(inode, path, p)
            self.page_cache.put(inode, minode)

        minode = self.page_cache.get(inode)

        if length == minode.size:
            return length

        if length == 0:
            minode.offset_to_page = {}
            for block in minode.offset_to_block.values():
                # TODO this should be done only on fsync
                self.block_manager.dealloc_block(block)

            minode.offset_to_block = {}
            minode.size = 0
            return 0

        if length < minode.size:
            last_valid_page = (length - 1) // PAGE_SZ
            page = self._get_page_for_offset(minode, last_valid_page * PAGE_SZ)
            # NOTE: zeroing out everything after length in that page
            pg_start = length % PAGE_SZ
            pg_nbytes = PAGE_SZ - pg_start
            if pg_nbytes > 0:
                page.content[pg_start: pg_start + pg_nbytes] = b'\0' * pg_nbytes
                page.flag_dirty = True

            # everything after this page must be removed.
            current_offset = (last_valid_page + 1) * PAGE_SZ
            while current_offset < minode.size:
                self.offset_to_page.pop(current_offset, None)
                block = self.offset_to_block.pop(current_offset, None)
                if block is not None:
                    # TODO this should be done only on fsync
                    self.block_manager.dealloc_block(block)

                current_offset += PAGE_SZ

            minode.size = length
            return length

        if length > minode.size:
            current_offset = minode.size
            remaining = length - minode.size
            while remaining > 0:
                page_num = current_offset // PAGE_SZ
                page = self._get_page_for_offset(minode, page_num * PAGE_SZ)
                pg_start = current_offset % PAGE_SZ
                pg_nbytes = min(remaining, PAGE_SZ - pg_start)
                page.content[pg_start:pg_start + pg_nbytes] = b'\0' * pg_nbytes
                page.flag_dirty = True

                current_offset += pg_nbytes
                remaining -= pg_nbytes

            minode.size = length
            return length

        # the only other case is length == minode.size where we don't do anything
        assert length == minode.size
        return minode.size

    def _unlink_open_file(self, realpath, inode):
        """
        if an inode still has open file descriptors, they should still be able to
        read and write to it. So instead, we just move it to a special "deleted" folder.
        the files there can be deleted once all file descriptors pointing to the inode
        are closed.
        """
        ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
        newpath = self.to_be_deleted / f"file_{ts}"
        os.rename(realpath, newpath)
        newinode = os.stat(newpath).st_ino
        assert inode == newinode
        new_fd_info = FDInfo(inode=inode, path=newpath)
        for fd in self.inode_to_open_fds_map[inode]:
            self.fd_info_map[fd] = new_fd_info

        # if there are open file descriptors then the minode has to be in the page cache
        # NOTE: assuming all metadata like minodes reside in page cache and will never be evicted.
        # only data blocks will be evicted.
        # TODO: Document this earlier on.
        minode = self.page_cache.get(inode)
        minode.realpath = new_fd_info.path

    def unlink(self, path):
        p = self.realpath(path)
        inode = os.stat(p).st_ino

        if inode in self.inode_to_open_fds_map:
            self._unlink_open_file(p, inode)
            return 0

        # no open file descriptors, can delete it normally
        minode = self.page_cache.get(inode)
        if minode is None:
            minode = MemInode(inode, path, p)

        for block in minode.offset_to_block.values():
            self.block_manager.dealloc_block(block)

        self.page_cache.remove(inode)
        return os.unlink(p)

    def utimens(self, path, times=None):
        if times is None:
            now = time.time()
            times = (now, now)

        p = self.realpath(path)
        inode = os.stat(p).st_ino
        minode = self.page_cache.get(inode)
        if minode is None:
            minode = MemInode(inode, path, p)
            self.page_cache.put(inode, minode)

        atime, mtime = times
        minode.atime = atime
        minode.mtime = mtime

    def fs_checkpoint(self):
        """
        writes all dirty pages to disk
        """
        # I know I know, iterating through a map, and then a linear
        # search over all pages to find the dirty ones is inefficient.
        # But i'm not so worried about performance. I'm going to run
        # small workloads, so it isn't going to take that much time.
        # TODO have better data structures for faster fsync and checkpointing
        for minode in self.page_cache.minode_map.values():
            # we pass a fd = -1 because this isn't really called by any fd
            self.fsync_obj.on_fsync(-1, minode.inode, minode)

    def destroy(self, path):
        self.log.info("shutting down cuttlefs")
        self.fs_checkpoint()
        self.log.info("shutting down http server")
        self.server.shutdown()
        self.log.info("waiting on server thread")
        self.server_thread.join()
        self.log.info("syncing block manager to disk")
        self.block_manager.sync()

    def _command_allow_all_writes(self, command):
        """
        Go through every flakey file and reset sequences to allow all writes
        """

        self.log.info("allowing all writes, removing all failure sequences")
        self.block_manager.faulty_paths = {}

    # TODO add command to dynamically add failure sequences

    def _command_insert_log_entry(self, command):
        self.log.info("User Specified Log: %s", command["msg"])

    def _command_evict_clean_pages(self, command):
        self.log.info("evicting clean pages")
        for inode, minode in self.page_cache.minode_map.items():
            page_offsets_to_evict = [
                offset for offset, page in minode.offset_to_page.items()
                if page.flag_dirty is False
            ]
            for offset in page_offsets_to_evict:
                minode.offset_to_page.pop(offset)

            self.log.info("evicted %d clean pages for %r",
                len(page_offsets_to_evict), minode)

    def _command_checkpoint(self, command):
        self.log.info("checkpointing")
        self.fs_checkpoint()

    def handle_command(self, command):
        """
        command is a dictionary
        """
        cmd = command.get("cmd", None)
        if not cmd:
            return f"No command (key=cmd) specified"

        commands = {
            "allow-all-writes": self._command_allow_all_writes,
            "insert-log-entry": self._command_insert_log_entry,
            "evict-clean-pages": self._command_evict_clean_pages,
            "checkpoint": self._command_checkpoint,
        }

        if cmd not in commands:
            return f"Unknown cmd {cmd}"

        commands[cmd](command)
        return {"success": True}

    def __call__(self, op, *args):
        fn = getattr(self, op, None)
        path = args[0]

        if fn is None:
            self.log.error("%s not implemented", op)
            raise FuseOSError(errno.ENOTSUPP)

        # TODO have flag to disable logging ops?
        self.log.info("FUSE_OPERATION: %s, %s", op, path)
        return fn(*args)
