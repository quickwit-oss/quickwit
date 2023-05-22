echo "Removing possibly existing gharchive index "
curl -s -X DELETE "localhost:9200/gharchive?pretty" > /dev/null
echo "Creating gharchive index "
curl -s -X PUT "localhost:9200/gharchive/" -H 'Content-Type: application/json' -d @elasticsearch-gharchive.json > /dev/null
echo "Ingesting data for gharchive"
gunzip -c gharchive-bulk.json.gz | curl -s -X POST "localhost:9200/_bulk?refresh=wait_for" -H 'Content-Type: application/json' --data-binary @- > /dev/null
