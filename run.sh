#!/bin/bash

# PHP Location
PHP_PATH=/usr/local/bin/php

# Main Script
SCRIPT_FILE=./JobDaemon.php

# Config Location
CONFIG_FILE=./config.ini

# PID Location
PID_FILE=./jobmgr.pid

# Log Location
LOG_FILE=./jobmgr.log

# Check PHP_PATH
if [ ! -f $PHP_PATH ]; then
  echo "ERROR : PHP_PATH did not exist"
  exit 1
fi

# Check SCRIPT_FILE
if [ ! -f $SCRIPT_FILE ]; then
  echo "ERROR : SCRIPT_FILE did not exist"
  exit 1
fi

# Check CONFIG_FILE
if [ ! -f $CONFIG_FILE ]; then
  echo "ERROR : CONFIG_FILE did not exist"
  exit 1
fi

job_start() {
  if [ -f $PID_FILE ]; then
    echo "Already Started"
    exit 1
  fi
  $PHP_PATH $SCRIPT_FILE -c $CONFIG_FILE -P $PID_FILE -l $LOG_FILE -d -vvvv
  echo "Starting"
  sleep 3
  if [ -f $PID_FILE ]; then
    echo "Started"
  else
    echo "Fail to Start"
    exit 1
  fi
}

job_stop() {
  if [ ! -f $PID_FILE ]; then
    echo "Already Stopped"
    exit 1
  fi
  kill -s SIGTERM `cat $PID_FILE`
  echo "Stopping"
  sleep 3
  if [ ! -f $PID_FILE ]; then
    echo "Stopped"
  else
    echo "Fail to Stop"
    exit 1
  fi
}

case "$1" in
  start)
        job_start
        ;;
  stop)
        job_stop
        ;;
  restart)
        job_stop
        job_start
        ;;
  *)
        echo "Usage: "`basename $0`" {start|stop|restart}"
        ;;
esac
exit 0