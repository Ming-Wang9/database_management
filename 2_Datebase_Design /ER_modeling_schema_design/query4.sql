SELECT ItemID
FROM (
    SELECT I.ItemID, MAX(CAST(I.Currently AS NUMBER))
    FROM Items I
);
