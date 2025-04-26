#!/usr/bin/bash
CURRENT_DATETIME=`date +%m-%d-%Y" "%H:%M:%S`
LOG_FILE_PATH="../logs/job_executor.log"
source set_env.sh
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
  echo "+-------------------------Job Executor Parameters----------------------+"
  echo "| -cfg or -config_file    : Configuration file path                    |"
  echo "+----------------------------------------------------------------------+"
}

validate_parameters(){
  MANDATORY_PARAMETERS=(CONFIG_FILE_PATH PYTHON_HOME)
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


log_parameters(){
  log_message "INFO" "Config file path - $CONFIG_FILE_PATH"
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
      -config_file)
        CONFIG_FILE_PATH=${CLI_ARRAY[counter]}
      ;;
      -cfg)
        CONFIG_FILE_PATH=${CLI_ARRAY[counter]}
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

run_app(){
  log_message "INFO" "Running job executor"
  CLI_INPUT_STRING="config_file $CONFIG_FILE_PATH"
  log_message "INFO" "CLI input - $CLI_INPUT_STRING"
  SHELL_CMD="$PYTHON_HOME $PYTHON_ANALYSIS_JOB_EXECUTOR_APP_NAME $CLI_INPUT_STRING"
  log_message "INFO" "Shell cmd - $SHELL_CMD"
  eval $SHELL_CMD
  APP_RUN_STATUS=$?
  log_message "INFO" "Job executor run status code - $APP_RUN_STATUS"
  if [ $APP_RUN_STATUS -eq 0 ]
  then
    log_message "INFO" "Job executor run completed with status code - $APP_RUN_STATUS"
    log_message "INFO" "Exiting job executor"
    exit 0
  else
    log_message "ERROR" "Job executor run failed with status code - $APP_RUN_STATUS"
    exit 1
  fi
}

parse_cli "$*"
log_parameters
validate_parameters
run_app