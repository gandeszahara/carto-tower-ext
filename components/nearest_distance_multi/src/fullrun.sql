-- This is the sample code for the Snowflake fullrun.
---------------------------------------------------------

EXECUTE IMMEDIATE '
CREATE TABLE IF NOT EXISTS ' || :output_table || '
AS (

WITH nearest AS (
    SELECT                          
        a.' || :geom_main_table|| ' AS MAIN_GEOM,                               -- Geometry of the main table
        b.' || :geom_second_table|| ' AS SECOND_GEOM,                           -- Geometry of the second table
        a.' || :id_main_table|| ' AS MAIN_ID                                    -- ID of the main table
        b.' || :id_second_table|| ' AS SECOND_ID,                               -- ID of the second table
        a.* EXCLUDE(' || :geom_main_table|| ', ' || :id_main_table|| '),        -- Attributes from the main table
        b.* EXCLUDE(' || :geom_second_table|| ', ' || :id_second_table|| '),    -- Attributes from the second table
        ST_DISTANCE(a.' || :geom_main_table|| ', b.' || :geom_second_table|| ' AS DISTANCE -- Calculate the distance between first and second table
    FROM 
        ' || :input_main_table || ' a
    JOIN 
        ' || :input_second_table || ' b 
    ON 
        ST_DWITHIN(a.' || :geom_main_table|| ', b.' || :geom_second_table|| ', ' || :radius|| ') -- Ensures the distance between address and flood point is within defined radius
),
rank AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY MAIN_ID ORDER BY DISTANCE) AS RANK_NUM -- Rank second points by distance
    FROM 
        nearest
)
SELECT 
    * EXCLUDE (RANK_NUM)
FROM 
    rank
WHERE 
    RANK_NUM <= ' || :number_result|| ' -- Select only the X nearest secondary points for each main points
ORDER BY 
    MAIN_ID, RANK_NUM; -- Order by address and distance rank
);
';