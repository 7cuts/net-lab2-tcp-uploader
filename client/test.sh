#!/usr/bin/bash
cd ../

NUM_TESTS=10
SIZE=1G
FILENAME=file
IP=127.0.0.1
PORT=4444
TESTDIR="test"

UPLOADS_DIR=../server/uploads

cleanup() {
    rm -f $UPLOADS_DIR/*
    for (( test=0; test < NUM_TESTS; test++ )); do
        fname="${TESTDIR}/${SIZE}_${FILENAME}_${test}"
        rm -f $fname
    done
}

cleanup

echo "Generating ${SIZE} files..."

truncate -s 2G  "${TESTDIR}/${SIZE}_${FILENAME}"



for (( test=0; test < NUM_TESTS; test++ )); do
    cp --sparse=always ${TESTDIR}/${SIZE}_"$FILENAME"{,_${test}}
done


echo "Building uplader"
go build -o uploader .


echo "Start sending"
for (( test=0; test < NUM_TESTS; test++ )); do
    fname=${TESTDIR}/${SIZE}_"$FILENAME"_${test} # run in backgroup
    ./uploader ${IP}:${PORT} $fname &
done
wait

cleanup
