# Database-Energy-Efficiency
Follow my steps to figure out how energy efficient your database queries are and if they are not energy efficient, I provided some solutions for energy efficiency and raising the database's performance and speed :
step 1 : Download a benchmark to test on (in my case : TPCH with 22 queries)
step 2 : use my calculate_efficiency code base to calculate CPU, MEMORY, DISK, DURATION of each query and upload them into a csv
step 3 : analyze the most important queries. Ps : the queries that consumed many resources and last long
step 4 : i used PostgreSQL "explain analyze" to know where is the bottelneck of each query alone
Solution 1 : Indexing, i tried to index on the most important column of the joined tables and the resource consumption dropped dramastically
Solution 2 : Materialized view, I tested the same query with materializing its subquery, it worked and it is more efficient as we don't need to recompute intermediate results
Solution 3 : Query reordering, pushing down selections, aggregations and filters and reordering joins (starting with tables that eliminate many data and going down through tables relations), and rewriting subqueries.
