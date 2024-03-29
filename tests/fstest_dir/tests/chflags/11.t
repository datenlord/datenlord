#!/bin/sh
# $FreeBSD: src/tools/regression/fstest/tests/chflags/11.t,v 1.1 2007/01/17 01:42:08 pjd Exp $

desc="chflags returns EPERM if a user tries to set or remove the SF_SNAPSHOT flag"

dir=`dirname $0`
. ${dir}/../misc.sh

require chflags

echo "1..46"

n0=`namegen`
n1=`namegen`
n2=`namegen`

expect 0 mkdir ${n0} 0755
cdir=`pwd`
cd ${n0}

expect 0 create ${n1} 0644
expect EPERM -u 65534 -g 65534 chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect EPERM chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect 0 chown ${n1} 65534 65534
expect EPERM -u 65534 -g 65534 chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect EPERM chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect 0 unlink ${n1}

expect 0 mkdir ${n1} 0644
expect EPERM -u 65534 -g 65534 chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect EPERM chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect 0 chown ${n1} 65534 65534
expect EPERM -u 65534 -g 65534 chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect EPERM chflags ${n1} SF_SNAPSHOT
expect none stat ${n1} flags
expect 0 rmdir ${n1}

# expect 0 mkfifo ${n1} 0644
# expect EPERM -u 65534 -g 65534 chflags ${n1} SF_SNAPSHOT
# expect none stat ${n1} flags
# expect EPERM chflags ${n1} SF_SNAPSHOT
# expect none stat ${n1} flags
# expect 0 chown ${n1} 65534 65534
# expect EPERM -u 65534 -g 65534 chflags ${n1} SF_SNAPSHOT
# expect none stat ${n1} flags
# expect EPERM chflags ${n1} SF_SNAPSHOT
# expect none stat ${n1} flags
# expect 0 unlink ${n1}

expect 0 symlink ${n2} ${n1}
expect EPERM -u 65534 -g 65534 lchflags ${n1} SF_SNAPSHOT
expect none lstat ${n1} flags
expect EPERM lchflags ${n1} SF_SNAPSHOT
expect none lstat ${n1} flags
expect 0 lchown ${n1} 65534 65534
expect EPERM -u 65534 -g 65534 lchflags ${n1} SF_SNAPSHOT
expect none lstat ${n1} flags
expect EPERM lchflags ${n1} SF_SNAPSHOT
expect none lstat ${n1} flags
expect 0 unlink ${n1}

cd ${cdir}
expect 0 rmdir ${n0}
