-- This is the sample code for the Snowflake dryrun.
---------------------------------------------------------

EXECUTE IMMEDIATE '
CREATE TABLE IF NOT EXISTS ' || :output_table || '
AS SELECT *, 
"POINT (174.7768961833 -41.2840625333)" as second_geom,
"xx" as second_id,
100 as distance
FROM ' || :input_main_table || '
WHERE 1 = 0;
';