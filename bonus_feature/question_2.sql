-- What regions has the "cheap_mobile" datasource appeared in?

SELECT DISTINCT region FROM trips WHERE datasource = 'cheap_mobile';