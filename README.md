# SQL_Bicycle_Manufacturer_Exploring
## I. Introduction
This project contains a bicycle manufacturer dataset that I will explore using SQL on Google BigQuery. The dataset is based on the Adventure Works public dataset.
## II. Requirements
- Google Cloud Platform account
- Project on Google Cloud Platform
- Google BigQuery API enabled
- SQL query editor or IDE
## III. Dataset Access
The bicycle manufacturer dataset is stored in a public Google BigQuery dataset. To access the dataset, follow these steps:

- Log in to your Google Cloud Platform account and create a new project.
- Navigate to the BigQuery console and select your newly created project.
- In the navigation panel, select "Add Data" and then "Star a project by name".
- Enter the project name **"adventureworks2019"** and click "Star".
- Click on the **"adventureworks2019"** table to open it.
## IV. Exploring the Dataset
In this project, I will write 08 query in Bigquery base on Adventure Works dataset
### Query 01: Calc Quantity of items, Sales value & Order quantity by each Subcategory in L12M
```
SELECT 
      FORMAT_DATETIME("%b %Y", a.ModifiedDate)    period
      ,c.name                                     Name
      ,SUM(a.OrderQty)                            qty_item
      ,SUM(a.LineTotal)                           total_sales
      ,COUNT(DISTINCT a.SalesOrderID)             order_cnt

FROM `adventureworks2019.Sales.SalesOrderDetail`              a
LEFT JOIN `adventureworks2019.Production.Product`             b
ON a.ProductID = b.ProductID
LEFT JOIN `adventureworks2019.Production.ProductSubcategory`  c
ON CAST(b.ProductSubcategoryID AS INT) = c.ProductSubcategoryID

WHERE DATE(a.ModifiedDate) >= (SELECT DATE_SUB(MAX(DATE(ModifiedDate)) , 
INTERVAL 12 month) FROM `adventureworks2019.Sales.SalesOrderDetail`)
GROUP BY period, Name
ORDER BY period DESC, Name;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/a27e1914-204f-4961-9262-9d5b7ac51695)
### Query 02: Calc % YoY growth rate by SubCategory & release top 3 cat with highest grow rate
```
WITH 
   qty AS (      
      SELECT 
            c.name                                      Name
            ,FORMAT_DATE("%Y", a.ModifiedDate)          period
            ,SUM(a.OrderQty)                            qty_item
      FROM `adventureworks2019.Sales.SalesOrderDetail`  a
      LEFT JOIN `adventureworks2019.Production.Product` b
      ON a.ProductID = b.ProductID
      LEFT JOIN `adventureworks2019.Production.ProductSubcategory` c
      ON CAST(b.ProductSubcategoryID AS INT) = c.ProductSubcategoryID
      GROUP BY c.name, period
      ORDER BY period)

 ,prv AS (
      SELECT 
         Name
         ,qty_item
         ,period
         ,LAG(qty_item) 
         OVER(PARTITION BY Name 
         ORDER BY period)                                 prv_qty
      FROM qty)

   SELECT 
      Name
      ,qty_item
      ,prv_qty
      ,ROUND((qty_item - prv_qty)/prv_qty,2)             qty_diff
   FROM prv 
   GROUP BY Name, qty_item, prv_qty
   ORDER BY qty_diff DESC
   LIMIT 3;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/1b1dbe0c-deee-453b-a37a-fdeb1d518055)
### Query 03: Ranking Top 3 TeritoryID with biggest Order quantity of every year
```
WITH 
  orders AS (
    SELECT 
      FORMAT_TIMESTAMP("%Y", a.ModifiedDate)              yr
      ,b.TerritoryID                        
      ,SUM(a.OrderQty)                                    order_cnt
    FROM `adventureworks2019.Sales.SalesOrderDetail`      a
    LEFT JOIN `adventureworks2019.Sales.SalesOrderHeader` b
    ON a.SalesOrderID = b.SalesOrderID
    GROUP BY yr, b.TerritoryID)

  ,rks AS (
    SELECT 
        yr
        ,TerritoryID    
        ,order_cnt
        ,DENSE_RANK() OVER(PARTITION BY yr 
        ORDER BY order_cnt DESC)                           rk
    FROM orders
    GROUP BY yr, TerritoryID, order_cnt)

  SELECT *
  FROM rks
  WHERE rk <=3
  ORDER BY yr DESC;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/f248d9ff-f02e-4300-9511-c399b87b7624)
### Query 04: Calc Total Discount Cost belongs to Seasonal Discount for each SubCategory
```
SELECT 
    FORMAT_TIMESTAMP("%Y", ModifiedDate) Year
    ,Name
    ,SUM(disc_cost)                      total_cost
FROM (
      SELECT DISTINCT a.*
      , c.Name
      , d.DiscountPct, d.Type
      , a.OrderQty * d.DiscountPct * UnitPrice  disc_cost 
      FROM `adventureworks2019.Sales.SalesOrderDetail`                  a
      LEFT JOIN `adventureworks2019.Production.Product`                 b 
      ON a.ProductID = b.ProductID
      LEFT JOIN `adventureworks2019.Production.ProductSubcategory`      c 
      ON CAST(b.ProductSubcategoryID AS INT) = c.ProductSubcategoryID
      LEFT JOIN `adventureworks2019.Sales.SpecialOffer`                 d 
      ON a.SpecialOfferID = d.SpecialOfferID
      WHERE LOWER(d.Type) LIKE '%seasonal discount%' 
)
GROUP BY Year,Name;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/8cc4c83d-1b89-4577-935c-2e2a83be642c)
### Query 05: Retention rate of Customer in 2014 with status of Successfully Shipped (Cohort Analysis)
```
WITH 
    info AS (
        SELECT
            EXTRACT (Year FROM ModifiedDate)     yr
            ,EXTRACT (Month FROM ModifiedDate)   mth_order
            ,CustomerID
            ,COUNT(DISTINCT SalesOrderID) 
        FROM `adventureworks2019.Sales.SalesOrderHeader`
        WHERE Status = 5 AND EXTRACT (Year FROM ModifiedDate) = 2014
        GROUP BY yr,mth_order,CustomerID)

    ,row_num AS (    
        SELECT 
            *
            ,ROW_NUMBER() OVER(PARTITION BY CustomerID ORDER BY mth_order)    row_nb
        FROM info)

    ,first_order AS (    
        SELECT DISTINCT 
            mth_order AS mth_join
            , yr
            , CustomerID
        FROM row_num
        WHERE row_nb = 1)

    ,all_join AS (   
        SELECT DISTINCT
            i.mth_order
            ,i.yr
            ,i.CustomerID
            ,f.mth_join
            ,CONCAT('M - ',i.mth_order - f.mth_join)   mth_diff
        FROM info                   i
        LEFT JOIN first_order       f
        ON i.CustomerID = f.CustomerID)
        
  SELECT 
      mth_join
      ,mth_diff
      ,COUNT(DISTINCT CustomerID)
  FROM all_join
  GROUP BY mth_join, mth_diff
  ORDER BY mth_join;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/4514fbb6-1055-4b8c-a170-5e19075b4f74)
### Query 06: Trend of Stock level & MoM diff % by all product in 2011
```
WITH 
  dates AS ( 
    SELECT 
       b.Name
      ,EXTRACT(Month FROM a.ModifiedDate)               mth
      ,EXTRACT(Year FROM a.ModifiedDate)                yr
      ,a.StockedQty                             
    FROM `adventureworks2019.Production.WorkOrder`      a
    LEFT JOIN `adventureworks2019.Production.Product`   b
    ON a.ProductID = b.ProductID
    GROUP BY a.ModifiedDate, b.Name, StockedQty )

  ,stocks AS (  
    SELECT
        Name
        ,mth
        ,yr
        ,SUM(StockedQty)                                stock_qty
    FROM dates 
    WHERE yr = 2011
    GROUP BY Name,mth,yr)

  ,prv AS (
  SELECT *
        ,LEAD(stock_qty) 
        OVER(PARTITION BY Name ORDER BY Name, mth DESC) stock_prv
  FROM stocks)
  
  SELECT *
        ,COALESCE(ROUND((stock_qty /stock_prv -1)*100,1),0) AS diff
  FROM prv
  ORDER BY Name, mth DESC;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/4ecb28e4-c8d9-4806-bbe1-444ca5b7b994)
### Query 07: Calc Ratio of Stock / Sales in 2011 by product name, by month
```
WITH 
  sale AS (  
    SELECT 
        EXTRACT (Month FROM a.ModifiedDate)              mth
        ,EXTRACT (Year FROM a.ModifiedDate)              yr
        ,b.ProductID
        ,b.Name
        ,SUM(OrderQty)                                   sales  
    FROM `adventureworks2019.Sales.SalesOrderDetail`     a
    LEFT JOIN `adventureworks2019.Production.Product`    b 
    ON a.ProductID = b.ProductID
    WHERE EXTRACT (Year FROM a.ModifiedDate) = 2011
    GROUP BY mth, yr, ProductID, Name)

  ,stock AS (  
    SELECT
        EXTRACT (Month FROM ModifiedDate)                mth
        ,EXTRACT (Year FROM ModifiedDate)                yr
        ,ProductID
        ,SUM(StockedQty)                                 stocks                                              
    FROM `adventureworks2019.Production.WorkOrder`
    WHERE EXTRACT (Year FROM ModifiedDate) = 2011
    GROUP BY mth, yr, ProductID )

    SELECT 
        sale.mth
        ,sale.yr
        ,sale.Name
        ,COALESCE(sale.sales,0)                          sales
        ,COALESCE(stock.stocks,0)                        stocks
        ,ROUND(COALESCE(stock.stocks,0)/sale.sales,1)    ratio
    FROM sale
    FULL JOIN stock
    ON sale.ProductID = stock.ProductID
    AND sale.mth = stock.mth
    ORDER BY mth DESC, ratio DESC;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/5eae86a1-f26c-4f08-968d-1488ea1b60bd)
### Query 08: No of order and value at Pending status in 2014
```
SELECT
    EXTRACT(Year FROM ModifiedDate)       Year
    ,Status
    ,COUNT(DISTINCT PurchaseOrderID)      order_Cnt 
    ,SUM(TotalDue)                        value
FROM `adventureworks2019.Purchasing.PurchaseOrderHeader`
WHERE Status = 1 AND EXTRACT(Year FROM ModifiedDate) = 2014
GROUP BY Year, Status;
```
- **Query results:**

![image](https://github.com/lthhoangkhoa225/SQL_Bicycle_Manufacturer_Exploring/assets/168264791/8402b39b-0b80-434b-81c3-8525ea1908cb)
## V. Conclusion
