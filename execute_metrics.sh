HUMANIKI_COMPLETE_NUM=29
HUMANIKI_GEN_PROCS=2



generete_metrics_single_proc(){
generate_complete=false

# sleep a little bit to help with multithreading
sleep_amt=.0$[ ( $RANDOM % 10 ) + 1 ]s
sleep $sleep_amt

while [ $generate_complete == false ]
do
  echo "attempting to execute generate metrics"
  python humaniki_schema/generate_metrics.py execute 20201130
  python_exit_signal=$?
  if [ $python_exit_signal == $HUMANIKI_COMPLETE_NUM ]
    then
      echo "generate complete"
      generate_complete=true
    else
      echo "not complete yet"
    fi
done
}

if [ -z "$1" ]
  then
    echo "Using default number of processes: "$HUMANIKI_GEN_PROCS
  else
    HUMANIKI_GEN_PROCS=$1
    echo "Using specified number of processes: "$HUMANIKI_GEN_PROCS
fi

# Launch processes
for (( i = 0; i < $HUMANIKI_GEN_PROCS; i++ )); do
  echo "Launching gen proc "$i
  generete_metrics_single_proc &
done

