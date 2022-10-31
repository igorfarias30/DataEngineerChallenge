-- From the two most commonly appearing regions, which is the latest datasource?

SELECT datasource, datetime 
FROM trips WHERE region IN 
( 
  select subquery.region FROM 
  (
    select region, count(region) as amount
    FROM trips GROUP BY region
    ORDER BY amount
    DESC LIMIT 2
  ) as subquery
)
ORDER BY datetime DESC
LIMIT 1;