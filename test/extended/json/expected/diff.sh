if [ $# -ne 1 -o ! -f $1 ]; then
  printf "Usage: %s <result file path>\n" "$(basename $0)"
  exit 1
fi

set_environment() {
  CURRENT=$(cd $(dirname $0); pwd)
  METADATA_DIR=${TSURUGI_METADATA_DIR}
  if [ -z "$METADATA_DIR" ]; then
    METADATA_DIR=~/.local/tsurugi/metadata
  fi

  ACTUAL_OID_PATH=$METADATA_DIR/oid
  ACTUAL_TABLES_PATH=$METADATA_DIR/tables.json
  ACTUAL_RESULT=${1}
  EXPECTED_OID_PATH=$CURRENT/oid
  EXPECTED_TABLES_PATH=$CURRENT/tables.json
  EXPECTED_RESULT=$CURRENT/result.txt
}

diff_check() {
  printf -- "-%0.s" {0..70}; printf "\n"
  printf "[%s]\n" "$1"

  local temp_file=$(basename $0).temp
  /usr/bin/diff $2 $3 > ${temp_file}
  if [ -s ${temp_file} ]; then
    cat ${temp_file}
  else
    echo "Succeed: There is no difference."
  fi
  rm -f ${temp_file}
}

check_main() {
  diff_check "OID file" $EXPECTED_OID_PATH $ACTUAL_OID_PATH
  diff_check "Tables metadata" $EXPECTED_TABLES_PATH $ACTUAL_TABLES_PATH
  diff_check "Test result" $EXPECTED_RESULT $ACTUAL_RESULT
}

set_environment $*
check_main
