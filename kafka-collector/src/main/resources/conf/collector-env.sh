#####
##  This file sets the environment variables for kafka-collector.sh
##  Its in conf because different environment will have different settings for memory etc.
####

## JAVA OPTIONS
export JAVA_OPTS="-Djava.library.path=/opt/hadoopgpl/native/Linux-amd64-64/"

export JAVA_MEM_OPTS="-Xmx1024m -Xss160k -XX:MaxDirectMemorySize=1024M -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+DisableExplicitGC"

