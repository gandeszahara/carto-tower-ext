-- This is the sample code for the Snowflake dryrun.
---------------------------------------------------------

EXECUTE IMMEDIATE '
CREATE TABLE IF NOT EXISTS ' || :output_table || '
AS SELECT *, 100 as distance
FROM ' || :input_main_table || '
WHERE 1 = 0;
';