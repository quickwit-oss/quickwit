-- drop current timestamp field
ALTER TABLE splits
DROP COLUMN update_timestamp;

-- create new timestamp fields
ALTER TABLE splits
ADD created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE splits
ADD updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- apply update trigger
SELECT diesel_manage_updated_at('splits');
