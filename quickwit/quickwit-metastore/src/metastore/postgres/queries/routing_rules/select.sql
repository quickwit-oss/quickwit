SELECT routing_table_id, rank, filter, index_id
FROM index_routing_rules
WHERE routing_table_id = $1
ORDER BY rank ASC
