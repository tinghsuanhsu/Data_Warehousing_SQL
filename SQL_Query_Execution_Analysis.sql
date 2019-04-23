/* 
############################################################################
Task C.4 - Query Execution Plan Analysis and Optimisation
############################################################################ 
*/ 

-------- 1) What are the sub-total and total agent profits of airports and airlines? 

-- Original plan -- 

select 
    decode(grouping (s.sourcename), 1, 'All Airports', s.sourcename) as airport,
    decode(grouping (a.airlinename), 1, 'All Airlines', a.airlinename) as airline,
    sum(t.total_agent_profit) as total
from airlinedim_v1 a, transaction_fact_v1 t, sourceairportdim_v1 s
where t.airlineid = a.airlineid
and t.sourceairportid = s.sourceid
group by cube(s.sourcename, a.airlinename)
order by s.sourcename, a.airlinename;


-- Specified execution plan: Use nested loop -- 

select /*+ USE_NL(a s)*/
    decode(grouping (s.sourcename), 1, 'All Airports', s.sourcename) as airport,
    decode(grouping (a.airlinename), 1, 'All Airlines', a.airlinename) as airline,
    sum(t.total_agent_profit) as total
from airlinedim_v1 a,  sourceairportdim_v1 s, transaction_fact_v1 t
where t.airlineid = a.airlineid
and t.sourceairportid = s.sourceid
group by cube(s.sourcename, a.airlinename)
order by s.sourcename, a.airlinename;


/* 
#########
Analysis
#########

The original queries are more efficient as it uses HASH JOIN to join two tables together. 
As GROUP BY CUBE will always have 3 operations -- SORT GROUP BY, GENERATE CUBE, SORT GROUP BY, the main part we can optimise is the join operation. 
The tables that we want to join both have thousands of records. 
Therefore, the use of nested loop will produce over 46 million of records. 
This results in high cost and longer processing time.
*/

-------- 2) What are the total and cumulative monthly total sales of Gold membership in 2009?


-- Original plan -- 

select 
    j.month, 
    to_char(sum(m.total_sales),'9,999,999.99') as "Monthly Sales",
    to_char(sum(sum(m.total_sales)) over(order by j.month rows unbounded preceding), '9,999,999.99') AS "Cumulative Monthly Sales",
    to_char(sum(sum(m.total_sales)) over(order by j.month rows 3 preceding), '9,999,999.99') AS "Moving 3 Months Sales"
from membershiptypedim_v1 mb, membership_sales_fact_v1 m, jointimedim_v1 j
where mb.membershipname = 'Gold'
    and m.timeid = j.timeid
    and mb.membershiptypeid = m.membershiptypeid
    and year = '2009'
group by j.month;

-- Specified execution plan: Use nested loop and no merge (to not combine the outer query and the inline view query) -- 

select 
    iq.month,
    iq.monthly_sales,
    to_char(sum(iq.monthly_sales) over(order by iq.month rows unbounded preceding), '9,999,999.99') AS "Cumulative Monthly Sales",
    to_char(sum(iq.monthly_sales) over(order by iq.month rows 3 preceding), '9,999,999.99') AS "Moving 3 Months Sales"
from 
((select /*+ USE_NL (j mb) no_merge*/ 
    j.month as month,
    sum(m.total_sales) as monthly_sales
    from membershiptypedim_v1 mb, jointimedim_v1 j, membership_sales_fact_v1 m
    where mb.membershipname = 'Gold'
    and m.timeid = j.timeid
    and mb.membershiptypeid = m.membershiptypeid
    and j.year = '2009'
    group by j.month)iq);

/*
#########
Analysis
#########

The queries with 2 hints (USE_NL and no_merge) is better due to the type of operations the system utilises. 
The execution time and cost are the same as the original queries, yet the system uses operations that are more efficient by default.
First, buffer sort on jointimedim_v1 is eliminated as there is no need to sort this table before joining it with membershiptypedim_v1. 
Instead, these two small dimensions are combined into one table prior to sorting. 
Secondly, sorting is executed after the aggregation with this approach. 
HASH GROUP BY is used on 182 rows while SORT GROUP BY is used for the same records in the original queries. 
Hash group by visits all records once and only does aggregation whereas sort group by is could potentially visit the 
record more than once as it has to sort and aggregate the records at the same time. 
After HASH GROUP BY is executed, we only have 8 records that needed to be sorted.
*/





-------- 3) What are the city ranks by total number of incoming routes in each country?

-- Original plan -- 

select 
    d.country, 
    d.city,
    count(r.total_num_routes) as num_incoming_routes,
    dense_rank() over (partition by d.country order by count(r.total_num_routes) desc) as city_rank
from destairportdim_v1 d, route_facts_v1 r
where d.destid = r.destairportid
group by d.city, d.country;


-- Specified execution plan: Use merge (sort merge join) -- 

select /*+ USE_MERGE (d r) */
    d.country, 
    d.city,
    count(r.total_num_routes) as num_incoming_routes,
    dense_rank() over (partition by d.country order by count(r.total_num_routes) desc) as city_rank
from destairportdim_v1 d, route_facts_v1 r
where d.destid = r.destairportid
group by d.city, d.country;


/* 
#########
Analysis
#########

The original queries are better because there are 2 less operations involved and use operations that are more efficient.
The hint USE_MERGE instructs the system to use sort merge join. 
Both tables are first sorted by SORT JOIN before merge join is performed. 
This is inefficient as tables do not have to be sorted to be joined together. 
Furthermore, the final sorted results are handled by other operation in the later stage of the execution. 
ORDER BY in PARTITION in DENSE_RANK will handle WINDOW SORT to sort query results based on country.
*/
