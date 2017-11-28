drop table MMatrix;
drop table NMatrix;
drop table intermmediate;

create table MMatrix (
  row_val int,
  column_val int,
  val double)
row format delimited fields terminated by ',' stored as textfile;

create table NMatrix (
  row_val int,
  column_val int,
  val double)
row format delimited fields terminated by ',' stored as textfile;

CREATE TABLE intermmediate (
  row_val INT,
  column_val INT,
   val double)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';


load data local inpath '${hiveconf:M}' overwrite into table MMatrix;

load data local inpath '${hiveconf:N}' overwrite into table NMatrix;

INSERT OVERWRITE table intermmediate select m.row_val, n.column_val,m.val*n.val
from MMatrix as m join NMatrix as n on m.column_val = n.row_val ;

INSERT OVERWRITE table intermmediate select i.row_val,i.column_val, SUM(val) from intermmediate i group by  i.row_val,i.column_val;

select count(*),AVG(i.val) from intermmediate i;