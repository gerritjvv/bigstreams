###################
# Starts the Streams Agent Test Framework
#!/usr/bin/env bash


if [ -z $1 ]; then

 echo "Please type <coordination instalation directory> "
 echo "Keep in mind that the configuration will be overwritten for this coordination installation"
 exit -1

fi


bin=`dirname "$this"`
script=`basename "$this"`
bin=`unset CDPATH; cd "$bin"; pwd`


# the root of the Pig installation
export STREAMS_HOME=$bin

export STREAMS_TEST_DIR=$STREAMS_HOME/tests

WORK_DIR="$STREAMS_HOME/work"

COORDINATION_HOME=$1

if [ -d $COORDINATION_HOME ]; then

 echo "Using $COORDINATION_HOME"
else

 echo "Please provide an coordination installation at $COORDINATION_HOME"
 exit -1

fi


rm -rf $WORK_DIR
mkdir -p $WORK_DIR


for f in $( ls $STREAMS_TEST_DIR )
do


	## Substitute the coordination configuration for that of the configuration in this test run.
	## this is achieved by creating a soft link to the from $pwd/conf to $COORDINATION_HOME/conf

	##check first to see if an agent conf dir exists and that its not a softlink
	##if its a normal directory then move it to maintain a backup


        echo "Linking configuration for $f"

	if [ -h $COORDINATION_HOME/conf ]; then
	  rm $COORDINATION_HOME/conf

	elif [ -d $COORDINATION_HOME/conf ]; then

	  echo "Backing up $COORDINATION_HOME/conf to $COORDINATION_HOME/conf_back"
	  if ! mv $COORDINATION_HOME/conf $COORDINATION_HOME/conf_back ; then
	    echo "Unable to move the directory $COORDINATION_HOME/conf"
	    exit -1
	  fi

	fi

	echo "Creating symbolic link to test configuration"
        echo "ln -s $STREAMS_TEST_DIR/$f/conf $COORDINATION_HOME/conf"
	if ! ln -s $STREAMS_TEST_DIR/$f/conf $COORDINATION_HOME/conf ; then
	 echo "Unable to create symbolic link $STREAMS_TEST_DIR/$f/conf $COORDINATION_HOME/conf"
	 exit -1
	fi

cho "Creating symbolic link to test configuration"
        echo "ln -s $STREAMS_TEST_DIR/$f/conf $COLLECTOR_HOME/conf"
	if ! ln -s $STREAMS_TEST_DIR/$f/conf $COLLECTOR_HOME/conf ; then
	 echo "Unable to create symbolic link $STREAMS_TEST_DIR/$f/conf $COLLECTOR_HOME/conf"
	 exit -1
	fi

	 echo "Running test $f"
	 exec $STREAMS_TEST_DIR/$f/run.sh $STREAMS_HOME $COORDINATION_HOME $WORK_DIR


done

