###########################
#
# Executes the Streams Kafka Collector Command line.
# e.g. collector.sh
#!/usr/bin/env bash


abspath=$(cd ${0%/*} && echo $PWD/${0##*/})
BIN_HOME=`dirname $abspath`

HOME=$BIN_HOME/../

export CONF_DIR=$HOME/conf

#source environment variables
[ -f $CONF_DIR/collector-env.sh ] && . $CONF_DIR/collector-env.sh


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
 export JAVA_HEAP="-Xmx2048m"
fi

# check envvars which might override default args
# CLASSPATH initially contains $CONF_DIR
CLASSPATH=${CLASSPATH}:$JAVA_HOME/lib/tools.jar

# for developers, add Pig classes to CLASSPATH

# so that filenames w/ spaces are handled correctly in loops below
IFS=
# add libs to CLASSPATH.
CLASSPATH="${CLASSPATH}:$HOME/lib/*"

CLASS="org.streams.kafkacol.collector.KafkaCollector"

CLASSPATH="$CONF_DIR:$CONF_DIR/META-INF:$CLASSPATH"



exec "$JAVA" -Xss128k -XX:MaxDirectMemorySize=1024M -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+DisableExplicitGC $JAVA_HEAP $JAVA_OPTS -Djava.library.path="$HOME/lib/native/Linux-amd64-64/" -classpath "$CLASSPATH" $CLASS -config "$CONF_DIR" "$@"

