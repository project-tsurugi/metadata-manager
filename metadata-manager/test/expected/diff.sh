DATATYPES=datatypes.json
OID=oid
RESULT=result.txt
TABLES=tables.json
OUTPUT=~/.local/tsurugi/metadata/
BUILD=../../../build/

diff $DATATYPES $OUTPUT$DATATYPES
diff $OID $OUTPUT$OID
diff $RESULT $BUILD$RESULT
diff $TABLES $OUTPUT$TABLES
