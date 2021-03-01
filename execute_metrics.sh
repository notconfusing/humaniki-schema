HUMANIKI_COMPLETE_NUM=29
HUMANIKI_GEN_PROCS=4
HUMANIKI_DUMP_DT=''
MAX_ITERATIONS=100

#nota bene, this needs to be run from the directory that has generate_metrics.py
source ../.env # if its the right directory, the one above it should have a .env there.

if [ -z $HUMANIKI_PYTHON ]
  then
    echo "humaniki python unset using default python"
    HUMANIKI_PYTHON=python
  else
    echo "humaniki python is $HUMANIKI_PYTHON"
    fi



generete_metrics_single_proc(){
  generate_complete=false
  iterations=0
  # sleep a little bit to help with multithreading
  # large sleep time ensures that making properties wont overlap
  sleep_amt=$[ ( $RANDOM % 120 ) + 1 ]s
  echo "$$ sleeping for "$sleep_amt ", iterations "$iterations
  sleep $sleep_amt
  while [ "$generate_complete" = false -a "$iterations" -lt $MAX_ITERATIONS ]
  do
    ((iterations++))
    echo "$$ attempting to execute generate metrics, iteration: "$iterations
  #  python humaniki_schema/generate_metrics.py execute 20201130
    echo "$HUMANIKI_PYTHON generate_metrics.py execute $HUMANIKI_DUMP_DT"
    $HUMANIKI_PYTHON generate_metrics.py execute $HUMANIKI_DUMP_DT
    python_exit_signal=$?
    if [ $python_exit_signal = $HUMANIKI_COMPLETE_NUM ]
      then
        echo "$$ generate complete"
        generate_complete=true
      else
        echo "$$ not complete yet"
        generate_complete=false
      fi
    sleep 2
  done
  echo "Generate metrics single proc exiting"
  exit
}

if [ -z "$1" ]
  then
    echo "Using default number of processes: "$HUMANIKI_GEN_PROCS
  else
    HUMANIKI_GEN_PROCS=$1
    echo "Using specified number of processes: "$HUMANIKI_GEN_PROCS
fi

if [ -z "$2" ]
  then
    echo "No dump_dt specified"
  else
    HUMANIKI_DUMP_DT=$2
    echo "Using dump_dt: "$HUMANIKI_DUMP_DT
fi


# Launch processes
for (( i = 0; i < $HUMANIKI_GEN_PROCS; i++ )); do
  echo "$$ Launching gen proc "$i
  generete_metrics_single_proc &
  pids[${i}]=$!
done

# wait for all pids
for pid in ${pids[*]}; do
    wait $pid
done