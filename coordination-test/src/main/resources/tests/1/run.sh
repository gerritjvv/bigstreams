###################
# Starts the Streams Coordination Test Framework
#!/usr/bin/env bash


if [ -z $3 ]; then

 echo "Please type <streams home> <coordination installation directory> <work directory>"
 exit -1

fi

STREAMS_HOME=$1
COORDINATION_DIR=$2
WORK_DIR=$3


# the root of the coordination installation
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

if [ -d $COORDINATION_DIR ]; then
 echo "USING $COORDINATION_DIR"
else
 echo "$COORDINATION_DIR does not exist"
 exit -1
fi


cd $COORDINATION_DIR && $COORDINATION_DIR/bin/streams.sh -start coordination &
C_PID=!

sleep 20s

cd $TEST_HOME


echo "Use JMX Port 8004 to minitor"

"$JAVA" $JAVA_HEAP_MAX -Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=8004 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
 -classpath "$CLASSPATH" org.streams.coordinationtest.TestSingleFileLock $TEST_CONF_DIR/conf.properties

PID=$!
echo "PID: $PID"

"$JAVA" $JAVA_HEAP_MAX -Dcom.sun.management.jmxremote \
-Dcom.sun.management.jmxremote.port=8004 \
-Dcom.sun.management.jmxremote.authenticate=false \
-Dcom.sun.management.jmxremote.ssl=false \
 -classpath "$CLASSPATH" org.streams.coordinationtest.TestMultiFileLock $TEST_CONF_DIR/conf.properties



