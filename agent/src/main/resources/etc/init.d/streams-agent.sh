PATH=/usr/bin:/sbin:/bin:/usr/sbin
export PATH

[ -f /etc/sysconfig/streams-agent ] && . /etc/sysconfig/streams-agent
lockfile=${LOCKFILE-/var/lock/subsys/streams-agent}
pidfile=${PIDFILE-/var/run/streams-agent.pid}
streamsd=${STREAMSD-/opt/streams-agent/bin/streams.sh}
RETVAL=0

# Source function library.
. /etc/rc.d/init.d/functions


start() {
    echo -n $"Starting streams agent: "
        daemon $streamsd -start agent &> /dev/null &
    RETVAL=$?
    echo
        [ $RETVAL = 0 ] && touch ${lockfile}
        return $RETVAL
}

stop() {
    echo -n $"Stopping streams: "
        $streamsd -stop agent
    RETVAL=$?
    echo
    [ $RETVAL = 0 ] && rm -f ${lockfile} ${pidfile}
}

restart() {
    stop
    start
}

rh_status() {
    status | grep -q -- '-p' 2>/dev/null && statusopts="-p $pidfile"
    status $statusopts $streamsd
}

case "$1" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    restart
    ;;
  condrestart|try-restart)
    rh_status_q || exit 0
    restart
    ;;
  status)
        rh_status
    ;;
  *)
    echo $"Usage: $0 {start|stop|status|restart|condrestart}"
    exit 1
esac

exit $RETVAL

