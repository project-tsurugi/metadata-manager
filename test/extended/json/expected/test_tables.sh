if [ $# -ne 1 -o ! -x "$1" ]; then
  printf "Usage: %s <tables_test Module>\n" "$(basename $0)"
  exit 1
fi

set_environment() {
  SCRIPT_DIR=$(cd $(dirname $0); pwd)
  BASE_DIR=$(cd $(dirname $1); pwd)
  METADATA_DIR=${TSURUGI_METADATA_DIR}
  if [ -z "$METADATA_DIR" ]; then
    METADATA_DIR=~/.local/tsurugi/metadata
  fi

  TEST_MODULE=$BASE_DIR/$1
  RESULT_FILE=$BASE_DIR/result.txt
}

run_test() {
  rm -f $METADATA_DIR/oid $METADATA_DIR/tables.json &> /dev/null
  $TEST_MODULE > $RESULT_FILE
  if [ $? -eq 0 ]; then
    $SCRIPT_DIR/diff.sh $RESULT_FILE
  fi
  rm -f $RESULT_FILE
}

set_environment $*
run_test

exit 0