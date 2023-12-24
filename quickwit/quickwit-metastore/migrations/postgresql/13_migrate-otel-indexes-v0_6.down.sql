UPDATE 
   indexes
SET 
   index_metadata_json = REPLACE(index_metadata_json, '"output_format":"hex"', '"output_format": "base64"')
WHERE 
    index_id in ('otel-logs-v0_6', 'otel-traces-v0_6');
