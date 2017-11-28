M = LOAD '$M' USING PigStorage(',') AS (t1: long , t2: long , t3: double );
N = LOAD '$N' USING PigStorage(',') AS (t1: long , t2: long , t3: double );

MN = JOIN M BY t2, N by t1; 

MN_INTR = FOREACH MN GENERATE M::t1 as row: long, N::t2 as column: long, (M::t3 * N::t3) as val: double;

MN_INTR_GRP = GROUP MN_INTR BY (row,column);

RESULT = FOREACH MN_INTR_GRP GENERATE group.row AS row, group.column AS column, SUM(MN_INTR.val) as value: double;

RESULT_ORDERED = ORDER RESULT BY row, column ;

RESULT_PRINT = FOREACH RESULT_ORDERED GENERATE FLATTEN((row, column, value));

STORE RESULT_PRINT INTO '$O' USING PigStorage(',');