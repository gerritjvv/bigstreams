PATH=/usr/bin:/sbin:/bin:/usr/sbin
export PATH

[ -f /etc/sysconfig/streams-collector ] && . /etc/sysconfig/streams-collector
lockfile=${LOCKFILE-/var/lock/subsys/streams-collector}
streamsd=${STREAMSD-/opt/streams-collector/bin/streams.sh}
REGEX="org.streams.collector.main.Main -start collector"

RETVAL=0

# Source function library.
. /etc/rc.d/init.d/functions


start() {

   pid=`pgrep -f "$REGEX"`

   if [ -n "$pid" ]; then
    echo "The Streams collector is already running"
    RETVAL=2
   else

    echo -n $"Starting streams collector: "
        daemon $streamsd -start collector &> /dev/null &
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
        $streamsd -stop collector
      RETVAL=$?
      echo
       [ $RETVAL = 0 ] && rm -f ${lockfile} ${pidfile}
    else
       echo "No Streams collector instance is running" 
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
      echo "Streams collector is running"
      echo "$pid"
      RETVAL=1
   else
       echo "Streams collector instance is stopped" 
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

