DATATYPES=datatypes.json
OID=oid
RESULT=result.txt
TABLES=tables.json
OUTPUT=../../build/output/tsurugi_metadata/

diff $DATATYPES $OUTPUT$DATATYPES
diff $OID $OUTPUT$OID
diff $RESULT $OUTPUT$RESULT
diff $TABLES $OUTPUT$TABLES
