# cuttlefs

FUSE file system with private page cache to simulate post fsync failure characteristics of modern file systems

## Installation

Install ninja and meson

```bash
sudo apt-get install --yes ninja-build
git clone https://github.com/mesonbuild/meson.git
cd meson
git checkout 0.52.0
pip install .
```

Install libfuse v3.8.0

```bash
git clone https://github.com/libfuse/libfuse.git
cd libfuse
git checkout "fuse-3.8.0"
mkdir build && cd $_ && meson ..
ninja
sudo chown root:root util/fusermount3
sudo chmod 4755 util/fusermount3
python -m pytest test/
sudo ninja install
```

Install python bindings for fuse

```bash
git clone https://github.com/fusepy/fusepy.git
cd fusepy
git checkout "v2.0.4"
pip install .
```

Install cuttlefs

```bash
git clone git@github.com:WiscADSL/cuttlefs.git
cd cuttlefs
pip install .

cd tests/ && python -m unittest -v fstests
```

## Usage

```
$> cuttlefs --help

usage: cuttlefs [-h] [--fs-metadir FS_METADIR] [--log-level {debug,info,warn,error}] [--foreground] [--nothreads] [--fsync-behavior {ext4-ordered,ext4-data,xfs,btrfs}] [--port PORT] [--fault-list-file FAULT_LIST_FILE] root mount

positional arguments:
  root                  Existing folder to act as root
  mount                 Mount point for fuse filesystem

optional arguments:
  -h, --help            show this help message and exit
  --fs-metadir FS_METADIR
                        meta directory for file system, defaults to current directory
  --log-level {debug,info,warn,error}
                        Log Level, defaults to info
  --foreground          run fuse in foreground? defaults to False
  --nothreads           run fuse without threads? defaults to False
  --fsync-behavior {ext4-ordered,ext4-data,xfs,btrfs}
                        simulates fsync behavior, defaults to ext4-ordered
  --port PORT           port to run http server, defaults to 8888
  --fault-list-file FAULT_LIST_FILE
                        File containing a json list of dictionaries of the form {"path": "/foo", "seq": "xxwxW", "block": 4} OR {"path": "/foo", "seq": "xxwxW", "sector": 4}
```
