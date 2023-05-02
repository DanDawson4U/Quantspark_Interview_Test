DROP TABLE IF EXISTS Drink_Demand;
CREATE TEMPORARY TABLE Drink_Demand AS
SELECT
    t1.drink_id,
    t1.drink,
    t1.location,
    t1.avg_drinks_per_day,
    t2.avg_drinks_per_hour,
    t3.drinks_ordered
FROM
    (SELECT
        drink_id,
        drink,
        location,
        COUNT(*) / COUNT(DISTINCT DATE(timestamp)) AS avg_drinks_per_day
    FROM Transactions
    GROUP BY drink_id, location) AS t1
    JOIN
    (SELECT
        drink_id,
        drink,
        location,
        COUNT(*) / COUNT(DISTINCT strftime('%Y-%m-%d %H', timestamp)) AS avg_drinks_per_hour
    FROM Transactions
    GROUP BY drink_id, location) AS t2
    ON t1.drink_id = t2.drink_id AND t1.location = t2.location
    JOIN
    (SELECT
        drink_id,
        COUNT(*) AS drinks_ordered,
        location
    FROM Transactions
    WHERE DATE(timestamp) = (SELECT DATE(MAX(timestamp), '-1 day') FROM Transactions)
    GROUP BY drink_id, location) AS t3
    ON t1.drink_id = t3.drink_id AND t1.location = t3.location;
SELECT * FROM Drink_Demand limit 100;

DROP TABLE IF EXISTS Glass_Supply;
CREATE TEMPORARY TABLE Glass_Supply AS
    Select DIM_Glasses.glass_id,
    DIM_Glasses.glass,
    drink_id,
    location,
        stock
    from Bar_Data
    JOIN DIM_Glasses
    on DIM_Glasses.glass_id = Bar_Data.glass_id;
SELECT * FROM Glass_Supply limit 100;

CREATE VIEW drink_supply_demand AS
SELECT
    A.Glass,
    MAX(A.Stock) AS stock,
    SUM(B.avg_drinks_per_day) AS total_drinks_per_day,
    SUM(B.avg_drinks_per_hour) AS total_drinks_per_hour,
    SUM(B.drinks_ordered) AS total_drinks_ordered
FROM
    Glass_Supply A
    JOIN Drink_Demand B
        ON A.drink_id = B.drink_id AND A.location = B.location
GROUP BY
    A.Glass,
    A.location;

