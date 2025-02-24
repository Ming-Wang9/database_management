SELECT COUNT(*) 
FROM (
    SELECT ItemID, COUNT(DISTINCT Cat) AS c_count
    FROM Categories
    GROUP BY ItemID
    HAVING c_count = 4
) AS Cat_count;
