#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/rename/06.t,v 1.1 2007/01/17 01:42:10 pjd Exp $

desc="rename returns EPERM if the file pointed at by the 'from' argument has its immutable, undeletable or append-only flag set"

dir=`dirname $0`
. ${dir}/../misc.sh

require chflags

echo "1..84"

n0=`namegen`
n1=`namegen`

expect 0 create ${n0} 0644
for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND SF_NOUNLINK UF_NOUNLINK; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect EPERM rename ${n0} ${n1}
done
expect 0 chflags ${n0} none
expect 0 unlink ${n0}

expect 0 mkdir ${n0} 0755
for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND SF_NOUNLINK UF_NOUNLINK; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect EPERM rename ${n0} ${n1}
done
expect 0 chflags ${n0} none
expect 0 rmdir ${n0}

# expect 0 mkfifo ${n0} 0644
# for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND SF_NOUNLINK UF_NOUNLINK; do
# 	expect 0 chflags ${n0} ${flag}
# 	expect ${flag} stat ${n0} flags
# 	expect EPERM rename ${n0} ${n1}
# done
# expect 0 chflags ${n0} none
# expect 0 unlink ${n0}

expect 0 symlink ${n1} ${n0}
for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND SF_NOUNLINK UF_NOUNLINK; do
	expect 0 lchflags ${n0} ${flag}
	expect ${flag} lstat ${n0} flags
	expect EPERM rename ${n0} ${n1}
done
expect 0 lchflags ${n0} none
expect 0 unlink ${n0}
