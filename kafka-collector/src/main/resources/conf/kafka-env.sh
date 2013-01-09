#####
##  This file sets the environment variables for streams.sh
##  Its in conf because different environment will have different settings for memory etc.
####

## JAVA OPTIONS
export JAVA_OPTS="-Djava.library.path=$STREAMS_HOME/lib/native/Linux-amd64-64/"

## JAVA HEAP
export JAVA_HEAP="-Xmx1024m"


