WITH Seller_only As (
    SELECT DISTINCT U.UserID, U.Rating
    FROM Users U
    JOIN Items I ON U.UserID = I.SellerID
    WHERE U.Location IS NOT NULL AND U.Country IS NOT NULL
)
SELECT COUNT(*)
FROM Seller_only 
WHERE Seller_only.Rating > 1000;

