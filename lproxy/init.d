#!/bin/bash
#
#       /etc/init.d/lcluster
#
#       lproxy - LevelDB cluster connect proxy
#

# Source function library.
. /etc/init.d/functions

name="lproxy"
CONFIG="/etc/lcluster/$name.json"
exec="/usr/bin/$name"
daemon="/usr/sbin/daemonize -u root"
PIDFILE="/var/run/lcluster/$name.pid"

start() {
    [ -f $CONFIG ] || exit 6
    [ -x $exec ] || exit 5
    echo -n "Starting $name: "
    if [ $EUID -ne 0 ]; then
        RETVAL=1
        failure
    else
        ulimit -n 65536
        touch $PIDFILE
        chmod 777 $PIDFILE
        $daemon $exec -c $CONFIG -d && success || failure
        RETVAL=$?
    fi
    echo
    return $RETVAL
}

stop() {
    echo -n "Shutting down $name: "
    if [ $EUID -ne 0 ]; then
        RETVAL=1
        failure
    else
        test -e $PIDFILE && kill $(cat $PIDFILE) && success || failure
        RETVAL=$?
    fi;
    echo
    rm -f $PIDFILE
    return $RETVAL
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
    status)
        status -p $PIDFILE $name || exit 1
        RETVAL=$?
        ;;
    *)
        echo "Usage: $name {start|stop|restart|status}"
        exit 1
        ;;
esac
exit $?

