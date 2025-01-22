-- This is the sample code for the Snowflake fullrun.
---------------------------------------------------------

EXECUTE IMMEDIATE '
    CREATE TABLE IF NOT EXISTS ' || :output_table || ' AS
    WITH nearest AS (
        SELECT
            a.' || :geom_main_table|| ' AS MAIN_GEOM,
            b.' || :geom_second_table|| ' AS SECOND_GEOM,
            a.' || :id_main_table|| ' AS MAIN_ID,
            b.' || :id_second_table|| ' AS SECOND_ID,
            a.* EXCLUDE(' || :geom_main_table|| ', ' || :id_main_table|| '),
            b.* EXCLUDE(' || :geom_second_table|| ', ' || :id_second_table|| '),
            ST_DISTANCE(a.' || :geom_main_table|| ', b.' || :geom_second_table|| ') AS DISTANCE
        FROM ' || :input_main_table || ' a
        JOIN ' || :input_second_table || ' b
        ON ST_DWITHIN(a.' || :geom_main_table|| ', b.' || :geom_second_table|| ', ' || :radius|| ')
    ),
    rank AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY MAIN_ID ORDER BY DISTANCE) AS RANK_NUM
        FROM
            nearest
    )
    SELECT
        * EXCLUDE (RANK_NUM)
    FROM
        rank
    WHERE
        RANK_NUM <= ' || :number_result|| '
    ORDER BY
        MAIN_ID, RANK_NUM';