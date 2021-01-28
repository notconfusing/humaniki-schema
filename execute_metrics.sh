HUMANIKI_COMPLETE_NUM=29
HUMANIKI_GEN_PROCS=2
HUMANIKI_DUMP_DT=''
MAX_ITERATIONS=100

generete_metrics_single_proc(){
  generate_complete=false
  iterations=0
  # sleep a little bit to help with multithreading
  sleep_amt=.0$[ ( $RANDOM % 10 ) + 1 ]s
  sleep $sleep_amt
  echo "$$ sleeping for "$sleep_amt ", iterations "$iterations
  while [ "$generate_complete" = false -a "$iterations" -lt $MAX_ITERATIONS ]
  do
    ((iterations++))
    echo "$$ attempting to execute generate metrics, iteration: "$iterations
  #  python humaniki_schema/generate_metrics.py execute 20201130
    python generate_metrics.py execute $HUMANIKI_DUMP_DT
    python_exit_signal=$?
    if [ $python_exit_signal = $HUMANIKI_COMPLETE_NUM ]
      then
        echo "$$ generate complete"
        generate_complete=true
        generate_complete=false
      else
        echo "$$ not complete yet"
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
done

