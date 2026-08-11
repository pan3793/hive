set hive.map.aggr.hash.percentmemory = 0.3;
set hive.mapred.local.mem = 384;

add file ../../data/scripts/dumpdata_script.py;

select count(distinct subq.key) from
(FROM src MAP src.key USING 'python3 dumpdata_script.py' AS key WHERE src.key = 10) subq;
