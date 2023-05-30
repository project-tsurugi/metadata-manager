OID=oid
RESULT=result.txt
TABLES=tables.json
OUTPUT=~/.local/tsurugi/metadata
BUILD=../../../../../build.json/output

diff $OID $OUTPUT/$OID
diff $RESULT $BUILD/$RESULT
diff $TABLES $OUTPUT/$TABLES
