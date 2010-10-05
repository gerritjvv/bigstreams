###########################
#
# Executes the Streams Collector Command line.
# e.g. streams.sh -start collector
#      streams.sh -ls
#!/usr/bin/env bash


abspath=$(cd ${0%/*} && echo $PWD/${0##*/})
STREAMS_BIN_HOME=`dirname $abspath`

STREAMS_HOME=$STREAMS_BIN_HOME/../

export STREAMS_CONF_DIR=$STREAMS_HOME/conf

#source environment variables
. $STREAMS_CONF_DIR/streams-env.sh


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


if [ -z $JAVA_HEAP ]; then
 export JAVA_HEAP="-Xmx1024m"
fi

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

CLASS="org.streams.collector.main.Main"

CLASSPATH=$STREAMS_CONF_DIR:$STREAMS_CONF_DIR/META-INF:$CLASSPATH



exec "$JAVA" -Xss128k -XX:MaxDirectMemorySize=2048M -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+DisableExplicitGC $JAVA_HEAP $JAVA_OPTS -Djava.library.path="$STREAMS_HOME/lib/native/Linux-amd64-64/" -classpath "$CLASSPATH" $CLASS "$@"

