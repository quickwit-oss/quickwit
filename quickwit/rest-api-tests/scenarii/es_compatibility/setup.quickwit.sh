echo "Removing possibly existing gharchive index "
curl -s -X DELETE "localhost:7280/api/v1/indexes/gharchive" > /dev/null
echo "Creating gharchive index "
curl -s -X POST "localhost:7280/api/v1/indexes/" -H 'Content-Type: application/json' -d @quickwit-gharchive.json > /dev/null
echo "Ingesting data for gharchive"
gunzip -c gharchive-bulk.json.gz | curl -s -X POST "localhost:7280/api/v1/_bulk?refresh=true" -H 'Content-Type: application/json' --data-binary @- > /dev/null
