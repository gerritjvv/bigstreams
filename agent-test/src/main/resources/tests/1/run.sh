###################
# Starts the Streams Agent Test Framework
#!/usr/bin/env bash


if [ -z $3 ]; then

 echo "Please type <streams home> <agent installation directory> <work directory>"
 exit -1

fi

STREAMS_HOME=$1
AGENT_DIR=$2
WORK_DIR=$3


# the root of the Pig installation
export TEST_HOME=`dirname $0`

export TEST_CONF_DIR=$TEST_HOME/conf

#create test pid file
# some Java parameters
if [ "$JAVA_HOME" != "" ]; then
    #echo "run java in $JAVA_HOME"
   JAVA_HOME=$JAVA_HOME
fi

if [ "$JAVA_HOME" = "" ]; then
     echo "Error: JAVA_HOME is not set."
     exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx1000m

# check envvars which might override default args
# CLASSPATH initially contains $STREAMS_CONF_DIR
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# for developers, add Pig classes to CLASSPATH

# so that filenames w/ spaces are handled correctly in loops below
IFS=
# add libs to CLASSPATH.
for f in $STREAMS_HOME/lib/*.jar; do
  CLASSPATH=${CLASSPATH}:$f;
done


#CLASS="com.specificmedia.hadoop.streams.agent.main.Main"

CLASSPATH=$TEST_CONF_DIR:$CLASSPATH


if [ -d $WORK_DIR ]; then
 echo "USING $WORK_DIR"
else
 echo "$WORK_DIR does not exist"
 exit -1
fi

if [ -d $AGENT_DIR ]; then
 echo "USING $AGENT_DIR"
else
 echo "$AGENT_DIR does not exist"
 exit -1
fi



"$JAVA" $JAVA_HEAP_MAX -Djava.library.path="$STREAMS_HOME/lib/native/Linux-amd64-64/" -classpath "$CLASSPATH" org.streams.tools.LogWriter $TEST_CONF_DIR/conf.properties&
LOG_WRITER_PID=$!
echo "LOG_WRITER_PID: $LOG_WRITER_PID"

"$JAVA" $JAVA_HEAP_MAX -Djava.library.path="$STREAMS_HOME/lib/native/Linux-amd64-64/" -classpath "$CLASSPATH" org.streams.tools.DummyServer $TEST_CONF_DIR/conf.properties&
SERVER_PID=$!
echo "SERVER_PID: $SERVER_PID"

sleep 2s

#run test class

if ! $AGENT_DIR/bin/streams.sh -start agent ; then

  echo "FAILED TO START AGENT $AGENT_DIR/bin/streams.sh"
else
  export AGENT_PID=$!
fi

#wait 2 minutes and kill the logwriter
sleep 120s
kill $LOG_WRITER_PID

echo "Stoppped LOG WRITER"

#keep server and agent running for another 5 minutes
sleep 600s

#call agent ls command
if ! $AGENT_DIR/bin/streams.sh -ls ; then
  echo "FAILED to list status objects from stream"
fi

#kill server and agent
kill $SERVER_PID

kill $AGENT_PID

exit 0

