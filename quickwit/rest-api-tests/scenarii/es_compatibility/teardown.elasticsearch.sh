echo "Removing index"
curl -s -X DELETE "localhost:9200/gharchive?pretty" > /dev/null
