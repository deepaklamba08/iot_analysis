#!/usr/bin/bash
CURRENT_DATETIME=`date +%m-%d-%Y" "%H:%M:%S`
LOG_FILE_PATH="../logs/webapp.log"
source set_env.sh

HOST_NAME="0.0.0.0"

PID_FILE=$PATH_TO_ANALYSIS_APP/home/tmp/app.pid

log_message() {
  LEVEL=$1
  MESSAGE=$2
  if [ "$LEVEL" = "ERROR" ]; then
    echo -e "\033[0;31m[$LEVEL] [$CURRENT_DATETIME] [$MESSAGE]"
  else
    echo "[$LEVEL] [$CURRENT_DATETIME] [$MESSAGE]"
    echo "[$LEVEL] [$CURRENT_DATETIME] [$MESSAGE]" >> $LOG_FILE_PATH
  fi
}

print_usage(){
  log_message "INFO" "Usage"
  echo "+-------------------------Analysis Web App Parameters----------------------+"
  echo "| -a or -action           : Action                                     |"
  echo "| -cf or -config          : Config file                                |"
  echo "| -su or -submitter       : Application runner                         |"
  echo "| Any key value pair      : Key value pair input to the App            |"
  echo "+----------------------------------------------------------------------+"
}

parse_cli() {
  CLI_INPUT=( "$1" )
  log_message "INFO" "Parsing CLI"
  CLI_ARRAY=()
  GENERIC_PARAMETERS=()
  for ARGUMENT in $CLI_INPUT
  do
    CLI_ARRAY+=($ARGUMENT)
  done
  CLI_COUNT=${#CLI_ARRAY[@]}
  log_message "INFO" "Number of CLI - $CLI_COUNT"

  if [ $CLI_COUNT -eq 0 ]
  then
    print_usage
    exit 1
  fi

  if [[ "${CLI_ARRAY[0]}" = "help" || "${CLI_ARRAY[0]}" = "-help" || "${CLI_ARRAY[0]}" = "-h"   ]]
  then
    print_usage
    exit 1
  fi

  TOTAL_PAIRS=$((CLI_COUNT % 2))

  if [ $TOTAL_PAIRS -ne 0 ]
  then
    log_message "ERROR" "Invalid number of arguments provided"
    exit 1
  fi

  counter=0
  while [ $counter -ne $CLI_COUNT ]
  do
    ARGUMENT_NAME=${CLI_ARRAY[counter]}
    counter=$((counter + 1))
    case $ARGUMENT_NAME in
      -config)
        CONFIG_FILE_PATH=${CLI_ARRAY[counter]}
      ;;
      -cf)
        CONFIG_FILE_PATH=${CLI_ARRAY[counter]}
      ;;
      -action)
        ACTION=${CLI_ARRAY[counter]}
      ;;
      -a)
        ACTION=${CLI_ARRAY[counter]}
      ;;

      -submitter)
        SUBMITTER=${CLI_ARRAY[counter]}
      ;;
      -su)
        SUBMITTER=${CLI_ARRAY[counter]}
      ;;
      -h)
        print_usage
        exit 1
      ;;
      -help)
        print_usage
        exit 1
      ;;
      *)
        GENERIC_PARAMETERS+=("$ARGUMENT_NAME ${CLI_ARRAY[counter]}")
    esac
    counter=$((counter + 1))
  done
}

log_parameters(){
  log_message "INFO" "Action - $ACTION"
  log_message "INFO" "Config file path - $CONFIG_FILE_PATH"
  log_message "INFO" "Submitter - $SUBMITTER"
}

set_parameters_if_absent(){
  DEFAULT_PARAMETERS=(SUBMITTER)
  for parameter_name in "${DEFAULT_PARAMETERS[@]}"
  do
    parameter_value="${!parameter_name}"
    if [[ -z "$parameter_value" ]];
    then
      if [[ "$parameter_name" = "SUBMITTER" ]]
      then
        SUBMITTER=`whoami`
        log_message "INFO" "Parameter $parameter_name is not set, setting default value to $SUBMITTER"
      fi
    fi
  done
}

validate_parameters(){
  MANDATORY_PARAMETERS=(ACTION)
  for parameter_name in "${MANDATORY_PARAMETERS[@]}"
  do
    parameter_value="${!parameter_name}"
    if [[ -z "$parameter_value" ]];
    then
      log_message "ERROR" "Parameter $parameter_name is invalid or not set"
      log_message "INFO" "Please refer below menu"
      print_usage
      exit 1
    fi
  done
}

orchestrate(){
  if [[ "$ACTION" = "start" ]]; then
      log_message "INFO" "starting app"
      run_app
  elif [[ "$ACTION" = "stop" ]]; then
      log_message "INFO" "stopping app"
      stop_app
  elif [[ "$ACTION" = "status" ]]; then
      log_message "INFO" "Getting app status"
      app_status
  else
    log_message "ERROR" "Action must be either start or stop"
  fi
}

app_status() {
  if is_app_running
  then
   log_message "INFO" "Application is running"
  else
   log_message "INFO" "Application is not running"
  fi
}

is_app_running() {

  if [ ! -f "$PID_FILE" ]
  then
    return 1
  fi

  APP_PID=$(cat "$PID_FILE")

  if [[ ! "$APP_PID" =~ ^[0-9]+$ ]]
  then
    return 1
  fi

  if kill -0 "$APP_PID" 2>/dev/null
  then
    return 0
  else
    return 1
  fi
}

stop_app() {

  log_message "INFO" "Stopping app"

  if [ ! -f "$PID_FILE" ]
  then
    log_message "INFO" "Application is not running. PID file does not exist."
    return 0
  fi

  APP_PID=$(cat "$PID_FILE")

  if [[ ! "$APP_PID" =~ ^[0-9]+$ ]]
  then
    log_message "ERROR" "Invalid PID found in PID file: $APP_PID"
    rm -f "$PID_FILE"
    return 1
  fi

  if ! kill -0 "$APP_PID" 2>/dev/null
  then
    log_message "INFO" \
      "Application is already stopped. Removing stale PID file."

    rm -f "$PID_FILE"

    return 0
  fi

  log_message "INFO" \
    "Stopping application with PID - $APP_PID"

  kill "$APP_PID"

  # Wait for process to stop
  for i in {1..10}
  do

    if ! kill -0 "$APP_PID" 2>/dev/null
    then
      break
    fi

    sleep 1

  done

  if kill -0 "$APP_PID" 2>/dev/null
  then

    log_message "ERROR" \
      "Application did not stop gracefully. Sending SIGKILL."

    kill -9 "$APP_PID"

  fi

  rm -f "$PID_FILE"

  log_message "INFO" "Application stopped"

  return 0
}

run_app(){
  log_message "INFO" "Running app"

    # Check whether application is already running
  if is_app_running
  then
    APP_PID=$(cat "$PID_FILE")

    log_message "INFO" \
      "Application is already running with PID - $APP_PID"

    return 0
  fi

  if [[ -z "$CONFIG_FILE_PATH" ]];
  then
    log_message "INFO" "Parameter config file is not set"
    exit 1
  fi

  CLI_INPUT_STRING="['config_file','$CONFIG_FILE_PATH', 'submitter', '$SUBMITTER']" # ${GENERIC_PARAMETERS[@]} $DEFAULT_ARGS_TO_WEB_APP"
  log_message "INFO" "CLI input - $CLI_INPUT_STRING"
  SHELL_CMD="$PYTHON_HOME -m flask --app \"$PYTHON_WEB_APP_NAME:create_app($CLI_INPUT_STRING)\" run --host $HOST_NAME"

  log_message "INFO" "Shell cmd - $SHELL_CMD"

  #eval "$SHELL_CMD" > /dev/null 2>&1 &
  "$PYTHON_HOME" -m flask \
    --app "$PYTHON_WEB_APP_NAME:create_app($CLI_INPUT_STRING)" \
    run \
    --host "$HOST_NAME" \
    > /dev/null 2>&1 &

  APP_PID=$!

  # Store PID
  echo "$APP_PID" > "$PID_FILE"
  log_message "INFO" "Application started with PID - $APP_PID"

  # Give the process a moment to start
  sleep 2
  if is_app_running
  then
    log_message "INFO" \
      "Application is running successfully with PID - $APP_PID"

    return 0
  else
    log_message "ERROR" \
      "Application failed to start"

    rm -f "$PID_FILE"

    return 1
  fi
}


if [ -d "$PATH_TO_ANALYSIS_APP/home/tmp" ]; then
    log_message "INFO" "PID directory exists."
else
    log_message "INFO" "Directory does not exist."
    mkdir -p $PATH_TO_ANALYSIS_APP/home/tmp
    touch $PID_FILE
fi

parse_cli "$*"
set_parameters_if_absent
validate_parameters
log_parameters
orchestrate