#!/bin/bash

clear
clear

func_name=""
times=1
log_name="output"
add_flag="-race "

usage(){
  echo -e \
    "usage: $0 [-f func_name] [-n times] [-o log_name] [-x] [-h]" \
    "\n\t-f: if empty, test all" \
    "\n\t-n: times to run, default 1" \
    "\n\t-o: log_name, default \"output\"" \
    "\n\t-x: without -race flag" \
    "\n\t-h: print usage"
}

while getopts "f:n:o:xh" opt; do
  case $opt in
    f) 
      func_name="$OPTARG"
      ;;
    n)
      if [ $OPTARG -gt 0 ]; then
        times="$OPTARG"
      fi
      ;;
    o) 
      log_name="$OPTARG"
      ;;
    x) 
      add_flag=""
      ;;
    h)
      usage
      exit
      ;;
  esac
done

if [ ! -z $func_name ]; then
  func_name="-run "$func_name
fi

echo "testing $add_flag... check the log for details"

if [ $times -le 1 ]; then
  outfile=test_"$log_name".log
  go test $func_name $add_flag >$outfile
  tail -n 1 $outfile
else
  dir=test_"$log_name"
  if [ ! -d $dir ]; then
    mkdir $dir
  fi

  i=1
  while [ $i -le $times ]; do 
    echo "test: $i"
    outfile="$dir"/"$log_name"_"$i".log
    go test $func_name $add_flag >$outfile
    tail -n 1 $outfile
    i=$((i+1))
  done
fi
