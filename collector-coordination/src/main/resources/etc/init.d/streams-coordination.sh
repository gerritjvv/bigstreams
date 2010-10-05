PATH=/usr/bin:/sbin:/bin:/usr/sbin
export PATH

[ -f /etc/sysconfig/streams-coordination ] && . /etc/sysconfig/streams-coordination
lockfile=${LOCKFILE-/var/lock/subsys/streams-coordination}
streamsd=${STREAMSD-/opt/streams-coordination/bin/streams.sh}
REGEX="coordination"

RETVAL=0

# Source function library.
. /etc/rc.d/init.d/functions


start() {

   pid=`pgrep -f "$REGEX"`

   if [ -n "$pid" ]; then
    echo "The Streams coordination is already running"
    RETVAL=2
   else

    echo -n $"Starting streams coordination: "
        daemon $streamsd -start coordination &> /dev/null &
    RETVAL=$?
    echo
        [ $RETVAL = 0 ] && touch ${lockfile}

   fi

   return $RETVAL

}

stop() {

    pid=`pgrep -f "$REGEX"`
    if [ -n "$pid" ]; then
      echo -n $"Stopping streams: "
        $streamsd -stop coordination
      RETVAL=$?
      echo
       [ $RETVAL = 0 ] && rm -f ${lockfile} ${pidfile}
    else
       echo "No Streams coordination instance is running" 
       RETVAL=2
    fi

    return $RETVAL

}

restart() {
    stop
    start
}

status() {

   pid=`pgrep -f "$REGEX"`
   if [ -n "$pid" ]; then
      echo "Streams coordination is running"
      echo "$pid"
      RETVAL=1
   else
       echo "Streams coordination instance is stopped" 
       RETVAL=2
   fi

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
        status
    ;;
  *)
    echo $"Usage: $0 {start|stop|status|restart}"
    exit 1
esac

exit $RETVAL

