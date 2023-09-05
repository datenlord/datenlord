#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/symlink/03.t,v 1.1 2007/01/17 01:42:11 pjd Exp $

desc="symlink returns ENAMETOOLONG if an entire length of either path name exceeded 1023 characters"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..14"

n0=`namegen`

expect 0 symlink ${path1023} ${n0}
expect 0 unlink ${n0}
expect 0 mkdir ${name255} 0755
expect 0 mkdir ${name255}/${name255} 0755
expect 0 mkdir ${name255}/${name255}/${name255} 0755
expect 0 mkdir ${path1021} 0755
expect 0 symlink ${n0} ${path1023}
expect 0 unlink ${path1023}
create_too_long
expect ENAMETOOLONG symlink ${n0} ${too_long}
expect ENAMETOOLONG symlink ${too_long} ${n0}
unlink_too_long
expect 0 rmdir ${path1021}
expect 0 rmdir ${name255}/${name255}/${name255}
expect 0 rmdir ${name255}/${name255}
expect 0 rmdir ${name255}
