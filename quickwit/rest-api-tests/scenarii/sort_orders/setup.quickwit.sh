curl -s -X DELETE "localhost:7280/api/v1/indexes/sortorder" > /dev/null
curl -s -X POST "localhost:7280/api/v1/indexes/" -H 'Content-Type: application/json' -d @quickwit-sortorder.json > /dev/null
cat split1.json | curl -s -X POST "localhost:7280/api/v1/_elastic/_bulk?refresh=true" -H 'Content-Type: application/json' --data-binary @- > /dev/null
cat split2.json | curl -s -X POST "localhost:7280/api/v1/_elastic/_bulk?refresh=true" -H 'Content-Type: application/json' --data-binary @- > /dev/null
