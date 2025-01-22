-- This is the sample code for the Snowflake fullrun.
---------------------------------------------------------

EXECUTE IMMEDIATE '
  CREATE TABLE IF NOT EXISTS OR REPLACE TABLE ' || :output_path || ' AS
    WITH nearest AS (
        SELECT
            a.' || :geom_main_table|| ',
            b.' || :geom_second_table|| ' AS SECOND_GEOM,
            a.' || :id_main_table|| ',
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
            ROW_NUMBER() OVER (PARTITION BY nearest.' || :id_main_table|| ' ORDER BY DISTANCE) AS RANK_NUM
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
       rank.' || :id_main_table|| ', RANK_NUM';