###################
# Starts the Streams Coordination Test Framework
#!/usr/bin/env bash


if [ -z $3 ] ; then

 echo "This command is meant to be run by streams.sh of this test project"
 echo "Please type <streams home> <coordination installation directory> <collector instalation directory> <work directory>"
 exit -1

fi

STREAMS_HOME=$1
COORDINATION_DIR=$2
COLLECTOR_DIR=$3
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

if [ -d $WORK_DIR/stage ]; then
  rm -rf $WORK_DIR/stage
fi

if [ -d $COORDINATION_DIR ]; then
 echo "USING $COORDINATION_DIR"
else
 echo "$COORDINATION_DIR does not exist"
 exit -1
fi


echo "Starting Coordination Service $COORDINATION_DIR"
cd $COORDINATION_DIR && $COORDINATION_DIR/bin/streams.sh -start coordination &
C_PID=!

echo "Starting Collector Service $COLLECTOR_DIR"
cd $COLLECTOR_DIR && $COLLECTOR_DIR/bin/streams.sh -start collector &
C_PID=!


sleep 20s



cd $TEST_HOME


#WRITE 10 files 1000 lines each

mkdir -p $WORK_DIR/stage/input
if ! "$JAVA" $JAVA_HEAP_MAX -classpath "$CLASSPATH" org.streams.collectortest.tools.LogWriter $WORK_DIR/stage/input 10 1000 ; then
    echo "Error Writing Test Logs"
    exit -1
fi

#SEND FILE DATA TO COLLECTOR the dummy agent will use the test configuration to know the collector address
if ! "$JAVA" $JAVA_HEAP_MAX -classpath "$CLASSPATH" org.streams.collectortest.tools.DummyAgent $WORK_DIR/stage/input ; then
 echo "Error Sending Data To Collector"
 exit -1
fi

#COMPARE COLLECTOR OUTPUT WITH The Original Files

# REWRITE to 2 comparable files 
#NOTE THAT Compression is used on the collector site so the java code will:
# --- uncompress the files and rewrite the files as plain text into one result file
# --- rewrite the original files into one file


mkdir -p $WORK_DIR/stage/compare

#The InputToCompareWriter will use the configuration to get the collector output directory

if ! "$JAVA" $JAVA_HEAP_MAX -classpath "$CLASSPATH"  org.streams.collectortest.tools.InputToCompareWriter $WORK_DIR/starge/compare/agentonefile.txt $WORK_DIR/stage/compare/collectoronefile.txt $WORK_DIR/stage/input ; then

 echo "Error in comparing the collector output with the generate input logs"
 exit -1

fi


echo "Test Done"










