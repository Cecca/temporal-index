-- Remove the old column for the index size, which was using a too-small datatype
ALTER TABLE
	batch_raw DROP COLUMN index_size_bytes;

-- Use 64 bits integers for it
ALTER TABLE
	batch_raw
ADD
	COLUMN index_size_bytes INT64 NOT NULL;