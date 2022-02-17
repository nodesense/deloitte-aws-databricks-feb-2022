## Athena Queries

```sql
SELECT r.movieId, title, avg(rating) as avg_rating, count(userId) as total_rating FROM "AwsDataCatalog"."gk_db"."tbl_ratings"  r
  JOIN "AwsDataCatalog"."gk_db"."tbl_movies" m  ON r.movieId = m.movieId
  GROUP BY r.movieId, title 
  ORDER BY total_rating DESC;
   
```
