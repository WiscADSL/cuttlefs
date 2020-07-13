import errno
import json
import os
import tempfile
import unittest

from pathlib import Path

from cuttlefs import CuttleFSForegroundRunner as CuttleFS

class GenericFSTests(object):
    """
    Testing operations on an empty file system
    """
    FSYNC_BEHAVIOR = None

    @classmethod
    def setUpClass(cls):
        cls.workspace = Path(tempfile.mktemp(suffix="-test-cuttlefs"))
        cls.mnt = cls.workspace / "mnt"
        cls.src = cls.workspace / "src"

        os.makedirs(cls.workspace)
        os.makedirs(cls.src)
        os.makedirs(cls.mnt)

        cls.cuttlefs = CuttleFS(cls.src, cls.mnt,
            metadir=cls.workspace / "fsmeta",
            stdout_file=cls.workspace / "cuttlefs_stdout",
            stderr_file=cls.workspace / "cuttlefs_stderr",
            fsync_behavior=cls.FSYNC_BEHAVIOR,
            nothreads=True,
        )

        cls.cuttlefs.mount()

    @classmethod
    def tearDownClass(cls):
        cls.cuttlefs.umount()

    def test_001_listdir(self):
        dirs = [i for i in self.mnt.iterdir()]
        self.assertEqual(dirs, [])

    def test_002_mkdir(self):
        os.makedirs(self.mnt / "mydir")
        mnt_dirs = [i.relative_to(self.mnt) for i in self.mnt.iterdir()]
        src_dirs = [i.relative_to(self.src) for i in self.src.iterdir()]
        self.assertEqual(mnt_dirs, src_dirs)
        self.assertEqual(mnt_dirs, [Path("mydir")])

    def test_003_newfile(self):
        f = self.mnt / "mydir/f1.txt"
        with open(f, "wb") as fp:
            fp.write(b'a' * 8192)

        mnt_contents = [i.relative_to(self.mnt) for i in (self.mnt / "mydir").iterdir()]
        src_contents = [i.relative_to(self.src) for i in (self.src / "mydir").iterdir()]
        self.assertEqual(mnt_contents, src_contents)
        self.assertEqual(mnt_contents, [Path("mydir/f1.txt")])

    def test_004_read_newfile_from_pagecache(self):
        f = self.mnt / "mydir/f1.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        self.assertEqual(data, b'a' * 8192)

        # TODO have a way to query cuttlefs for dirty pages and assert that those pages are dirty?

        # ensure that on disk file is still 0 bytes because it is only in the page cache
        with open(self.src / "mydir/f1.txt", "r") as fp:
            meta = json.load(fp)

        self.assertEqual(meta['size'], 0)

    def test_005_fsync_newfile(self):
        f = self.mnt / "mydir/f1.txt"
        fd = os.open(f, os.O_WRONLY)
        os.fsync(fd)
        os.close(fd)

        with open(self.src / "mydir/f1.txt", "r") as fp:
            meta = json.load(fp)

        # TODO assert that even the offset to block mapping changed?
        self.assertEqual(meta['size'], 8192)

    def test_006_modify_first_block_of_newfile(self):
        f = self.mnt / "mydir/f1.txt"

        with open(self.src / "mydir/f1.txt", "r") as fp:
            old_meta = json.load(fp)

        fd = os.open(f, os.O_WRONLY)
        os.pwrite(fd, b'b' * 4096, 0)
        os.close(fd)

        # the latest content should be read back from page cache
        with open(f, "rb") as fp:
            data = fp.read()

        self.assertEqual(len(data), 8192)
        self.assertEqual(data[:4096], b'b' * 4096)
        self.assertEqual(data[4096:], b'a' * 4096)

        with open(self.src / "mydir/f1.txt", "r") as fp:
            new_meta = json.load(fp)

        # there should be no change in metadata - only page cache changes
        self.assertEqual(old_meta, new_meta)

    # TODO trigger a checkpoint to test that out?
    # TODO tests for unlinking and renaming while there is an open file descriptor

    def test_007_move_newfile(self):
        oldf = self.mnt / "mydir/f1.txt"
        newf = self.mnt / "mydir/f2.txt"
        os.rename(oldf, newf)

        mnt_contents = [i.relative_to(self.mnt) for i in (self.mnt / "mydir").iterdir()]
        src_contents = [i.relative_to(self.src) for i in (self.src / "mydir").iterdir()]
        self.assertEqual(mnt_contents, src_contents)
        self.assertEqual(mnt_contents, [Path("mydir/f2.txt")])

    def test_008_fsync_newfile(self):
        f = self.mnt / "mydir/f2.txt"
        with open(self.src / "mydir/f2.txt", "r") as fp:
            old_meta = json.load(fp)

        fd = os.open(f, os.O_WRONLY)
        os.fsync(fd)
        os.close(fd)

        with open(self.src / "mydir/f2.txt", "r") as fp:
            new_meta = json.load(fp)

        # atleast mtime should have changed?
        self.assertNotEqual(old_meta, new_meta)

    def test_009_unlink_newfile(self):
        f = self.mnt / "mydir/f2.txt"
        os.unlink(f)
        mnt_contents = [i.relative_to(self.mnt) for i in (self.mnt / "mydir").iterdir()]
        src_contents = [i.relative_to(self.src) for i in (self.src / "mydir").iterdir()]
        self.assertEqual(mnt_contents, src_contents)
        self.assertEqual(mnt_contents, [])

        # TODO since unlinked file does not have open file descriptors, the
        # blocks should be reclaimed by block manager. test that too.

    # create and fsync file again before unmounting so we can test if it persists
    # on remount
    test_010_newfile = test_003_newfile
    test_011_fsync_newfile = test_005_fsync_newfile

    def test_012_remount(self):
        # TODO should umount flush all dirty pages?
        # repeat this process without test_011_fsync_newfile
        self.cuttlefs.umount()
        self.cuttlefs.mount()

    def test_013_check_data_survive_remount(self):
        f = self.mnt / "mydir/f1.txt"
        self.assertEqual(f.exists(), True)

        with open(f, "rb") as fp:
            data = fp.read()

        self.assertEqual(data, b'a' * 8192)

class Ext4OrderedTests(GenericFSTests, unittest.TestCase):
    FSYNC_BEHAVIOR = "ext4-ordered"
    # these tests run after the generic tests
    def test_101_create_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "wb") as fp:
            fp.write(b'a' * 4096)
            fp.write(b'b' * 4096)
            fp.write(b'c' * 4096)
            os.fsync(fp.fileno())

    def test_102_remount_with_failures(self):
        self.cuttlefs.umount()
        fault_list_file = self.workspace / "faultlist.json"
        fault_list = [
            {"path": "/mydir/faulty.txt", "block": 1, "seq": "xW"},
        ]
        with open(fault_list_file, 'w') as fp:
            json.dump(fault_list, fp, indent=2)

        self.cuttlefs.fault_list_file = fault_list_file.as_posix()

        self.cuttlefs.mount()

    # TODO test to make sure that the contents are 'a', 'b', 'c'?

    def test_103_write_fsync_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        fd = os.open(f, os.O_WRONLY)

        lseek_ret = os.lseek(fd, 4096, os.SEEK_SET)
        self.assertEqual(lseek_ret, 4096)

        write_ret = os.write(fd, b'x' * 4096)
        self.assertEqual(write_ret, 4096)

        with self.assertRaises(os.error) as exc:
            os.fsync(fd)

        self.assertEqual(exc.exception.errno, errno.EIO)
        os.close(fd)

    def test_104_read_after_failed_fsync(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = b''.join([b'a' * 4096, b'x' * 4096, b'c' * 4096])
        self.assertEqual(data, expected_data)

    def test_105_read_after_remount(self):
        self.cuttlefs.umount()
        self.cuttlefs.mount()

        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = b''.join([b'a' * 4096, b'b' * 4096, b'c' * 4096])
        self.assertEqual(data, expected_data)

    # Test failure while appending

    def test_106_remount_with_failures(self):
        self.cuttlefs.umount()
        fault_list_file = self.workspace / "faultlist.json"
        fault_list = [
            {"path": "/mydir/faulty.txt", "block": 3, "seq": "xW"},
        ]
        with open(fault_list_file, 'w') as fp:
            json.dump(fault_list, fp, indent=2)

        self.cuttlefs.fault_list_file = fault_list_file.as_posix()
        self.cuttlefs.mount()

    def test_107_append_fsync_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = ''.join([i*4096 for i in 'abc']).encode()
        self.assertEqual(data, expected_data)

        fd = os.open(f, os.O_WRONLY | os.O_APPEND)
        write_ret = os.write(fd, b'x'*4096)
        self.assertEqual(write_ret, 4096)

        with self.assertRaises(os.error) as exc:
            os.fsync(fd)

        self.assertEqual(exc.exception.errno, errno.EIO)
        # continue to append after fsync failure
        write_ret = os.write(fd, b'y'*4096)
        self.assertEqual(write_ret, 4096)
        os.fsync(fd)
        os.close(fd)

        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = ''.join([i*4096 for i in 'abcxy']).encode()
        self.assertEqual(data, expected_data)

        # TODO maybe just evict page cache?
        self.cuttlefs.umount()
        self.cuttlefs.mount()

        with open(f, "rb") as fp:
            data = fp.read()

        expected_prefix = ''.join([i*4096 for i in 'abc']).encode()
        expected_suffix = b'y' * 4096

        self.assertEqual(data[:len(expected_prefix)], expected_prefix)
        self.assertEqual(data[-len(expected_suffix):], expected_suffix)
        self.assertNotEqual(data[4096*3:4096*4], b'x'*4096)

# XFS behaves similarly to ext4 ordered
class XFSTests(Ext4OrderedTests):
    FSYNC_BEHAVIOR = "xfs"

class BtrfsTests(GenericFSTests, unittest.TestCase):
    FSYNC_BEHAVIOR = "btrfs"

    def test_101_create_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "wb") as fp:
            fp.write(b'a' * 4096)
            fp.write(b'b' * 4096)
            fp.write(b'c' * 4096)
            os.fsync(fp.fileno())

    def test_102_remount_with_failures(self):
        self.cuttlefs.umount()
        fault_list_file = self.workspace / "faultlist.json"
        fault_list = [
            {"path": "/mydir/faulty.txt", "block": 1, "seq": "xW"},
        ]
        with open(fault_list_file, 'w') as fp:
            json.dump(fault_list, fp, indent=2)

        self.cuttlefs.fault_list_file = fault_list_file.as_posix()

        self.cuttlefs.mount()

    # TODO test to make sure that the contents are 'a', 'b', 'c'?

    def test_103_write_fsync_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        fd = os.open(f, os.O_WRONLY)

        lseek_ret = os.lseek(fd, 4096, os.SEEK_SET)
        self.assertEqual(lseek_ret, 4096)

        write_ret = os.write(fd, b'x' * 4096)
        self.assertEqual(write_ret, 4096)

        with self.assertRaises(os.error) as exc:
            os.fsync(fd)

        self.assertEqual(exc.exception.errno, errno.EIO)
        os.close(fd)

    def test_104_read_after_failed_fsync(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        # reverted old data should be in the page cache
        expected_data = b''.join([b'a' * 4096, b'b' * 4096, b'c' * 4096])
        self.assertEqual(data, expected_data)

    def test_105_read_after_remount(self):
        self.cuttlefs.umount()
        self.cuttlefs.mount()

        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = b''.join([b'a' * 4096, b'b' * 4096, b'c' * 4096])
        self.assertEqual(data, expected_data)

    # Test failure while appending
    def test_106_remount_with_failures(self):
        self.cuttlefs.umount()
        fault_list_file = self.workspace / "faultlist.json"
        fault_list = [
            {"path": "/mydir/faulty.txt", "block": 3, "seq": "xW"},
        ]
        with open(fault_list_file, 'w') as fp:
            json.dump(fault_list, fp, indent=2)

        self.cuttlefs.fault_list_file = fault_list_file.as_posix()
        self.cuttlefs.mount()

    def test_107_append_fsync_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = ''.join([i*4096 for i in 'abc']).encode()
        self.assertEqual(data, expected_data)

        fd = os.open(f, os.O_WRONLY | os.O_APPEND)
        self.assertEqual(os.write(fd, b'x'*4096), 4096)
        with self.assertRaises(os.error) as exc:
            os.fsync(fd)

        self.assertEqual(exc.exception.errno, errno.EIO)
        # continue to append after fsync failure
        self.assertEqual(os.write(fd, b'y'*4096), 4096)
        os.fsync(fd)
        os.close(fd)

        with open(f, "rb") as fp:
            data = fp.read()

        # holes instead of x
        expected_data = ''.join([i*4096 for i in 'abc\0y']).encode()
        self.assertEqual(data, expected_data)

        self.cuttlefs.umount()
        self.cuttlefs.mount()

        with open(f, "rb") as fp:
            data = fp.read()

        self.assertEqual(data, expected_data)


class Ext4DataTests(GenericFSTests, unittest.TestCase):
    FSYNC_BEHAVIOR = "ext4-data"

    # these tests run after the generic tests
    def test_101_create_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "wb") as fp:
            fp.write(b'a' * 4096)
            fp.write(b'b' * 4096)
            fp.write(b'c' * 4096)
            os.fsync(fp.fileno())

    def test_102_remount_with_failures(self):
        self.cuttlefs.umount()
        fault_list_file = self.workspace / "faultlist.json"
        fault_list = [
            {"path": "/mydir/faulty.txt", "block": 1, "seq": "xW"},
        ]
        with open(fault_list_file, 'w') as fp:
            json.dump(fault_list, fp, indent=2)

        self.cuttlefs.fault_list_file = fault_list_file.as_posix()

        self.cuttlefs.mount()

    # TODO test to make sure that the contents are 'a', 'b', 'c'?

    def test_103_write_fsync_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        fd = os.open(f, os.O_WRONLY)

        lseek_ret = os.lseek(fd, 4096, os.SEEK_SET)
        self.assertEqual(lseek_ret, 4096)

        write_ret = os.write(fd, b'x' * 4096)
        self.assertEqual(write_ret, 4096)

        os.fsync(fd) # This should pass because it should be put in the journal

        # second fsync should fail
        with self.assertRaises(os.error) as exc:
            os.fsync(fd)

        self.assertEqual(exc.exception.errno, errno.EIO)
        os.close(fd)

    def test_104_read_after_failed_fsync(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = b''.join([b'a' * 4096, b'x' * 4096, b'c' * 4096])
        self.assertEqual(data, expected_data)

    def test_105_read_after_remount(self):
        self.cuttlefs.umount()
        self.cuttlefs.mount()

        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = b''.join([b'a' * 4096, b'b' * 4096, b'c' * 4096])
        self.assertEqual(data, expected_data)

    # Test failure while appending
    def test_106_remount_with_failures(self):
        self.cuttlefs.umount()
        fault_list_file = self.workspace / "faultlist.json"
        fault_list = [
            {"path": "/mydir/faulty.txt", "block": 3, "seq": "xW"},
        ]
        with open(fault_list_file, 'w') as fp:
            json.dump(fault_list, fp, indent=2)

        self.cuttlefs.fault_list_file = fault_list_file.as_posix()
        self.cuttlefs.mount()

    def test_107_append_fsync_faulty_file(self):
        f = self.mnt / "mydir/faulty.txt"
        with open(f, "rb") as fp:
            data = fp.read()

        expected_data = ''.join([i*4096 for i in 'abc']).encode()
        self.assertEqual(data, expected_data)

        fd = os.open(f, os.O_WRONLY | os.O_APPEND)
        self.assertEqual(os.write(fd, b'x'*4096), 4096)
        os.fsync(fd) # fails but shouldn't report it

        self.assertEqual(os.write(fd, b'y'*4096), 4096)

        with self.assertRaises(os.error) as exc:
            os.fsync(fd)
        self.assertEqual(exc.exception.errno, errno.EIO)

        os.close(fd)

        with open(f, "rb") as fp:
            data = fp.read()

        # holes instead of x
        expected_data = ''.join([i*4096 for i in 'abcxy']).encode()
        self.assertEqual(data, expected_data)

        self.cuttlefs.umount()
        self.cuttlefs.mount()

        with open(f, "rb") as fp:
            data = fp.read()

        expected_prefix = ''.join([i*4096 for i in 'abc']).encode()
        expected_suffix = b'y' * 4096

        self.assertEqual(data[:len(expected_prefix)], expected_prefix)
        self.assertEqual(data[-len(expected_suffix):], expected_suffix)
        self.assertNotEqual(data[4096*3:4096*4], b'x'*4096)

if __name__ == '__main__':
    unittest.main()
