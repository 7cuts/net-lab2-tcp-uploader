#1/usr/bin/env bash
cd ../

TMPDIR=/mnt/disk_full_testt
sudo mkdir -p $TMPDIR
sudo mount -t tmpfs -o size=16M tmpfs $TMPDIR

sudo chown $USER:$USER $TMPDIR

go build .
cp server $TMPDIR
cd $TMPDIR

./server 4444

sudo umount $TMPDIR

cd -
