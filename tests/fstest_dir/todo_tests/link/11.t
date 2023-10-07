#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/link/11.t,v 1.1 2007/01/17 01:42:09 pjd Exp $

desc="link returns EPERM if the source file is a directory"

dir=`dirname $0`
. ${dir}/../misc.sh

n0=`namegen`
n1=`namegen`
n2=`namegen`

case "${os}:${fs}" in
SunOS:UFS)
	echo "1..10"

	expect 0 mkdir ${n0} 0755
	expect 0 link ${n0} ${n1}
	expect 0 unlink ${n1}
	expect 0 rmdir ${n0}
	;;
*)
	echo "1..9"

	expect 0 mkdir ${n0} 0755
	expect EPERM link ${n0} ${n1}
	expect 0 rmdir ${n0}
	;;
esac

expect 0 mkdir ${n0} 0755
expect 0 chown ${n0} 65534 65534
cdir=`pwd`
cd ${n0}

expect 0 -u 65534 -g 65534 mkdir ${n1} 0755
expect EPERM -u 65534 -g 65534 link ${n1} ${n2}
expect 0 -u 65534 -g 65534 rmdir ${n1}

cd ${cdir}
expect 0 rmdir ${n0}
