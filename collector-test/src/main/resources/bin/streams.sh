###################
# Starts the Streams Agent Test Framework
#!/usr/bin/env bash


if [ -z $2 ]; then

 echo "Please type <COORDINATION instalation directory> <COLLECTOR instalation directory> "
 echo "Keep in mind that the configuration will be overwritten for this COORDINATION and COLLECTOR installation"
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
COLLECTOR_HOME=$2


if [ -d $COORDINATION_HOME ]; then

 echo "Using $COORDINATION_HOME"
else

 echo "Please provide an COORDINATION installation at $COORDINATION_HOME"
 exit -1

fi


if [ -d $COLLECTOR_HOME ]; then

 echo "Using $COLLECTOR_HOME"
else

 echo "Please provide an COLLECTOR installation at $COLLECTOR_HOME"
 exit -1

fi


rm -rf $WORK_DIR
mkdir -p $WORK_DIR


#FOR EACH tests subdirectory e.g. tests/1 tests/2 link the configuration and execute the run.sh command
#This will run each test sequencially
for f in $( ls $STREAMS_TEST_DIR )
do

    echo "Linking configuration for $f"

    ## Substitute the COORDINATION configuration for that of the configuration in this test run.
	## this is achieved by creating a soft link to the from $pwd/conf to $COORDINATION_HOME/conf

    if [ -h $COORDINATION_HOME/conf ]; then
	  rm $COORDINATION_HOME/conf

	elif [ -d $COORDINATION_HOME/conf ]; then

	  echo "Backing up $COORDINATION_HOME/conf to $COORDINATION_HOME/conf_back"
	  if ! mv $COORDINATION_HOME/conf $COORDINATION_HOME/conf_back ; then
	    echo "Unable to move the directory $COORDINATION_HOME/conf"
	    exit -1
	  fi

	fi

    ## Substitute the COLLECTOR configuration for that of the configuration in this test run.
	## this is achieved by creating a soft link to the from $pwd/conf to $COLLECTOR_HOME/conf

	if [ -h $COLLECTOR_HOME/conf ]; then
	  rm $COLLECTOR_HOME/conf

	elif [ -d $COLLECTOR_HOME/conf ]; then

	  echo "Backing up $COLLECTOR_HOME/conf to $COLLECTOR_HOME/conf_back"
	  if ! mv $COLLECTOR_HOME/conf $COLLECTOR_HOME/conf_back ; then
	    echo "Unable to move the directory $COLLECTOR_HOME/conf"
	    exit -1
	  fi

	fi

	echo "Creating symbolic link to test configuration"
        echo "ln -s $STREAMS_TEST_DIR/$f/conf $COLLECTOR_HOME/conf"
	if ! ln -s $STREAMS_TEST_DIR/$f/conf $COLLECTOR_HOME/conf ; then
	 echo "Unable to create symbolic link $STREAMS_TEST_DIR/$f/conf $COLLECTOR_HOME/conf"
	 exit -1
	fi

     echo "ln -s $STREAMS_TEST_DIR/$f/conf $COORDINATION_HOME/conf"
       if ! ln -s $STREAMS_TEST_DIR/$f/conf $COORDINATION_HOME/conf ; then
          echo "Unable to create symbolic link $STREAMS_TEST_DIR/$f/conf $COORDINATION_HOME/conf"
       exit -1
     fi

    #RUN Test command
	 echo "Running test $f"
	 exec $STREAMS_TEST_DIR/$f/run.sh $STREAMS_HOME $COORDINATION_HOME $COLLECTOR_HOME $WORK_DIR


done

