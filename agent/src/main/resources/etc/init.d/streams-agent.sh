PATH=/usr/bin:/sbin:/bin:/usr/sbin
export PATH

[ -f /etc/sysconfig/streams-agent ] && . /etc/sysconfig/streams-agent
lockfile=${LOCKFILE-/var/lock/subsys/streams-agent}
streamsd=${STREAMSD-/opt/streams-agent/bin/streams.sh}
REGEX="org.streams.agent.main.Main -start agent"

RETVAL=0

# Source function library.
. /etc/rc.d/init.d/functions


start() {

   pid=`pgrep -f "$REGEX"`

   if [ -n "$pid" ]; then
    echo "The Streams agent is already running"
    RETVAL=2
   else

    echo -n $"Starting streams agent: "
        daemon $streamsd -start agent &> /dev/null &
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
        $streamsd -stop agent
      RETVAL=$?
      echo
       [ $RETVAL = 0 ] && rm -f ${lockfile} ${pidfile}
    else
       echo "No Streams agent instance is running" 
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
      echo "Streams agent is running"
      echo "$pid"
      RETVAL=1
   else
       echo "Streams agent instance is stopped" 
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

