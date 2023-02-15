WITH order_types AS (
SELECT  
  orders.SalesOrderID,
  CAST(orders.OrderDate as DATE) as order_date,
  CAST(orders.DueDate as DATE) as due_date,
  CAST(orders.ShipDate as DATE) as ship_date,
  orders.CustomerID,
  orders.ContactID,
  orders.SubTotal,
  orders.TaxAmt,
  orders.Freight,
  orders.TotalDue,
  CASE 
    WHEN orders.SalesPersonID IS NULL 
    THEN 'Online'
    ELSE 'Offline' 
    END as order_type,
  product.Name as product,
  category.Name as product_category,
  subcategory.Name as product_subcategory,
  product.StandardCost as cost,
  product.ListPrice as price,
  salesterritory.Name as region,
  salesterritory.CountryRegionCode as country,
  CASE 
    WHEN orders.SalesPersonID IS NOT NULL 
    THEN CONCAT(contact.FirstName," ",contact.LastName) 
    ELSE "Online"
    END as sales_person,
  CASE WHEN RANK() OVER (PARTITION BY orders.CustomerID ORDER BY orders.OrderDate) > 1 THEN 1 ELSE 0 END as returned
FROM 
  `tc-da-1.adwentureworks_db.salesorderheader` as orders
LEFT JOIN 
  `tc-da-1.adwentureworks_db.salesorderdetail` as orderdetail
  ON orders.SalesOrderID = orderdetail.SalesOrderDetailID
LEFT JOIN 
  `tc-da-1.adwentureworks_db.product` as product
  ON orderdetail.ProductID = product.ProductID
LEFT JOIN  
  `tc-da-1.adwentureworks_db.productsubcategory` as subcategory
  ON product.ProductSubcategoryID = subcategory.ProductSubcategoryID
LEFT JOIN 
  `tc-da-1.adwentureworks_db.productcategory` as category
  ON subcategory.ProductCategoryID = category.ProductCategoryID
LEFT JOIN 
  `tc-da-1.adwentureworks_db.salesterritory` as salesterritory 
  ON orders.TerritoryID = salesterritory.TerritoryID 
LEFT JOIN 
  `tc-da-1.adwentureworks_db.salesperson` as salesperson
  ON orders.SalesPersonID = salesperson.SalesPersonID
LEFT JOIN 
  `tc-da-1.adwentureworks_db.employee` as employee
  ON salesperson.SalesPersonID = employee.EmployeeId
LEFT JOIN 
  `tc-da-1.adwentureworks_db.contact` as contact
  ON contact.ContactId = employee.EmployeeId
WHERE 
    orders.OrderDate < '2004-07-01' 
ORDER BY
 orders.OrderDate 
) 
SELECT
  *
FROM 
  order_types
WHERE
  order_type = "Offline"
;