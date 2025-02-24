WITH s_and_b AS (
    SELECT DISTINCT I.SellerID
    FROM Items I
    JOIN Bids B ON I.SellerID = B.BidderID
)

SELECT COUNT(*)
FROM s_and_b
