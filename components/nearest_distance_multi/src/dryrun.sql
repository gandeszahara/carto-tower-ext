-- This is the sample code for the Snowflake dryrun.
---------------------------------------------------------

EXECUTE IMMEDIATE '
CREATE TABLE IF NOT EXISTS ' || :output_table || '
AS SELECT *, 
''POINT'' AS SECOND_GEOM,
''xx'' AS SECOND_ID,
100 AS DISTANCE
FROM ' || :input_main_table || '
WHERE 1 = 0;
';