#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/open/23.t,v 1.1 2007/01/17 01:42:10 pjd Exp $

desc="open returns EINVAL when an attempt was made to open a descriptor with an illegal combination of O_RDONLY, O_WRONLY, and O_RDWR"

dir=`dirname $0`
. ${dir}/../misc.sh

echo "1..4"

n0=`namegen`

expect 0 create ${n0} 0644
case "${os}" in
Linux)
	expect 0 open ${n0} O_WRONLY,O_RDWR
	expect 0 open ${n0} O_RDONLY,O_WRONLY,O_RDWR
        ;;
*)
	expect EINVAL open ${n0} O_WRONLY,O_RDWR
	expect EINVAL open ${n0} O_RDONLY,O_WRONLY,O_RDWR
        ;;
esac
expect 0 unlink ${n0}
