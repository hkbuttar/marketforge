-- Run from the repository root after the corresponding raw datasets exist.
-- These views retain only query metadata; Parquet remains the single data copy.
CREATE OR REPLACE VIEW raw_prices AS
SELECT * FROM read_parquet('data/raw/prices/year=*/month=*/*.parquet', hive_partitioning=true);

CREATE OR REPLACE VIEW raw_fundamentals AS
SELECT * FROM read_parquet('data/raw/fundamentals/year=*/month=*/*.parquet', hive_partitioning=true);

CREATE OR REPLACE VIEW raw_earnings AS
SELECT * FROM read_parquet('data/raw/earnings/year=*/month=*/*.parquet', hive_partitioning=true);

CREATE OR REPLACE VIEW raw_macro AS
SELECT * FROM read_parquet('data/raw/macro/year=*/month=*/*.parquet', hive_partitioning=true);

CREATE OR REPLACE VIEW raw_news AS
SELECT * FROM read_parquet('data/raw/news/year=*/month=*/*.parquet', hive_partitioning=true);
