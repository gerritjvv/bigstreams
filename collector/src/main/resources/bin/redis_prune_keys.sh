basedir=`dirname $0`

currentdate=$(/bin/date --date "2 week ago" +%Y-%m-%d)
loopenddate=$(/bin/date --date "1 week ago"  +%Y-%m-%d)

dates=()

until [ "$currentdate" == "$loopenddate" ]
do
  echo $currentdate

  currentdate=$(/bin/date --date "$currentdate 1 day" +%Y-%m-%d)
  dates+=($currentdate)

done

redis-cli --eval "$basedir/redis_prune_keys.lua" ${dates[@]} 

