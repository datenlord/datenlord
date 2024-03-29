#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/rename/07.t,v 1.1 2007/01/17 01:42:10 pjd Exp $

desc="rename returns EPERM if the parent directory of the file pointed at by the 'from' argument has its immutable or append-only flag set"

dir=`dirname $0`
. ${dir}/../misc.sh

require chflags

echo "1..106"

n0=`namegen`
n1=`namegen`
n2=`namegen`

expect 0 mkdir ${n0} 0755

expect 0 create ${n0}/${n1} 0644
for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect EPERM rename ${n0}/${n1} ${n2}
done
expect 0 chflags ${n0} none
expect 0 unlink ${n0}/${n1}

expect 0 mkdir ${n0}/${n1} 0755
for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect EPERM rename ${n0}/${n1} ${n2}
done
expect 0 chflags ${n0} none
expect 0 rmdir ${n0}/${n1}

# expect 0 mkfifo ${n0}/${n1} 0644
# for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND; do
# 	expect 0 chflags ${n0} ${flag}
# 	expect ${flag} stat ${n0} flags
# 	expect EPERM rename ${n0}/${n1} ${n2}
# done
# expect 0 chflags ${n0} none
# expect 0 unlink ${n0}/${n1}

expect 0 symlink ${n2} ${n0}/${n1}
for flag in SF_IMMUTABLE UF_IMMUTABLE SF_APPEND UF_APPEND; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect EPERM rename ${n0}/${n1} ${n2}
done
expect 0 chflags ${n0} none
expect 0 unlink ${n0}/${n1}

expect 0 create ${n0}/${n1} 0644
for flag in SF_NOUNLINK UF_NOUNLINK; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect 0 rename ${n0}/${n1} ${n2}
	expect 0 rename ${n2} ${n0}/${n1}
done
expect 0 chflags ${n0} none
expect 0 unlink ${n0}/${n1}

expect 0 mkdir ${n0}/${n1} 0755
for flag in SF_NOUNLINK UF_NOUNLINK; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect 0 rename ${n0}/${n1} ${n2}
	expect 0 rename ${n2} ${n0}/${n1}
done
expect 0 chflags ${n0} none
expect 0 rmdir ${n0}/${n1}

# expect 0 mkfifo ${n0}/${n1} 0644
# for flag in SF_NOUNLINK UF_NOUNLINK; do
# 	expect 0 chflags ${n0} ${flag}
# 	expect ${flag} stat ${n0} flags
# 	expect 0 rename ${n0}/${n1} ${n2}
# 	expect 0 rename ${n2} ${n0}/${n1}
# done
# expect 0 chflags ${n0} none
# expect 0 unlink ${n0}/${n1}

expect 0 symlink ${n2} ${n0}/${n1}
for flag in SF_NOUNLINK UF_NOUNLINK; do
	expect 0 chflags ${n0} ${flag}
	expect ${flag} stat ${n0} flags
	expect 0 rename ${n0}/${n1} ${n2}
	expect 0 rename ${n2} ${n0}/${n1}
done
expect 0 chflags ${n0} none
expect 0 unlink ${n0}/${n1}

expect 0 rmdir ${n0}
