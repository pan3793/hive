-- the sums are rounded: double summation order is not deterministic,
-- so unrounded trailing digits flip between runs
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;


-- Clearly decimal
explain
select round(sum(l_extendedprice), 2) from lineitem q0 where l_discount
between cast('0.05' as decimal(3,2)) and cast('0.07' as decimal(3,2));
select round(sum(l_extendedprice), 2) from lineitem q0 where l_discount
between cast('0.05' as decimal(3,2)) and cast('0.07' as decimal(3,2));

-- Unspecified - should be decimal
explain
select round(sum(l_extendedprice), 2) from lineitem q1 where l_discount
between 0.06 - 0.01 and 0.06 + 0.01;
select round(sum(l_extendedprice), 2) from lineitem q1 where l_discount
between 0.06 - 0.01 and 0.06 + 0.01;
select round(sum(l_extendedprice), 2) from lineitem q2 where l_discount
between 0.05 and 0.07;

-- Just for kicks, to see that it works
select round(sum(l_extendedprice), 2) from lineitem q3 where l_discount
between (cast('0.06' as decimal(3,2)) - cast('0.01' as decimal(3,2)))
  and (cast('0.06' as decimal(3,2)) + cast('0.01' as decimal(3,2)));

-- This will go to double and produce a different result
select round(sum(l_extendedprice), 2) from lineitem q4 where l_discount
between (cast('0.06' as decimal(3,2)) - cast('0.01' as double))
  and (cast('0.06' as decimal(3,2)) + cast('0.01' as double));




set hive.cbo.enable=false;
set hive.optimize.constant.propagation=false;

explain
select round(sum(l_extendedprice), 2) from lineitem q10 where l_discount
between cast('0.05' as decimal(3,2)) and cast('0.07' as decimal(3,2));
select round(sum(l_extendedprice), 2) from lineitem q10 where l_discount
between cast('0.05' as decimal(3,2)) and cast('0.07' as decimal(3,2));

explain
select round(sum(l_extendedprice), 2) from lineitem q11 where l_discount
between 0.06 - 0.01 and 0.06 + 0.01;
select round(sum(l_extendedprice), 2) from lineitem q11 where l_discount
between 0.06 - 0.01 and 0.06 + 0.01;
select round(sum(l_extendedprice), 2) from lineitem q12 where l_discount
between 0.05 and 0.07;

select round(sum(l_extendedprice), 2) from lineitem q13 where l_discount
between (cast('0.06' as decimal(3,2)) - cast('0.01' as decimal(3,2)))
  and (cast('0.06' as decimal(3,2)) + cast('0.01' as decimal(3,2)));

select round(sum(l_extendedprice), 2) from lineitem q14 where l_discount
between (cast('0.06' as decimal(3,2)) - cast('0.01' as double))
  and (cast('0.06' as decimal(3,2)) + cast('0.01' as double));
