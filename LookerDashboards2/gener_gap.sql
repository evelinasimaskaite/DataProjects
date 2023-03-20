WITH 
  employee_main as (
SELECT  
    employee.EmployeeId,
    CAST(employee.HireDate as DATE) as hire_date,
    employee.ContactID,
    employee.ManagerID,
    employee.Title,
    DATE_DIFF(CAST('2004-07-31' AS DATE),CAST(employee.BirthDate AS DATE), Year) as age,
    employee.MaritalStatus,
    employee.Gender,
    ROUND(DATE_DIFF(CAST('2004-07-31' AS DATE),CAST(employee.HireDate AS DATE), MONTH)/12,1) as years_with_company,
    employee.SalariedFlag,
    employee.VacationHours,
    employee.SickLeaveHours,
    pay_history.Rate,
    department.Name as department,
    department.GroupName as unit,
    shift.Name as shift
  FROM 
    `tc-da-1.adwentureworks_db.employee` employee
    JOIN (SELECT 
            EmployeeID,
            Rate,
            RANK() OVER (PARTITION BY EmployeeID ORDER BY RateChangeDate DESC) as rank
          FROM `tc-da-1.adwentureworks_db.employeepayhistory`) pay_history
      ON employee.EmployeeId = pay_history.EmployeeID
    JOIN `tc-da-1.adwentureworks_db.employeedepartmenthistory` department_history
      ON employee.EmployeeId = department_history.EmployeeID
    JOIN `tc-da-1.adwentureworks_db.department` department
      ON department_history.DepartmentID = department.DepartmentID
    JOIN `tc-da-1.adwentureworks_db.shift` shift
      ON department_history.ShiftID = shift.ShiftID
  WHERE
    department_history.EndDate IS NULL
    AND
    pay_history.rank = 1
  ),

dep_change as (
  SELECT  
    department_history.EmployeeId,
    COUNT(department_history.DepartmentID) as change_times,
    CASE WHEN COUNT(department_history.DepartmentID) > 1 THEN 1 ELSE 0 END as dep_change_flag,
  FROM 
    `tc-da-1.adwentureworks_db.employeedepartmenthistory` department_history
  GROUP BY
    1
),

rate_change AS (
  SELECT
    pay_history.EmployeeID,
    COUNT(pay_history.EmployeeID) as change_times,
    CASE WHEN COUNT(pay_history.EmployeeID) > 1 THEN 1 ELSE 0 END as rate_change_flag
  FROM
    `tc-da-1.adwentureworks_db.employeepayhistory` pay_history
  GROUP BY 
    1
)

SELECT
  employee_main.*,
  dep_change.change_times,
  dep_change.dep_change_flag,
  rate_change.change_times,
  rate_change.rate_change_flag
FROM
  employee_main
  JOIN
  dep_change
    ON
    employee_main.EmployeeID = dep_change.EmployeeID
  JOIN
  rate_change
    ON
    employee_main.EmployeeID = rate_change.EmployeeID
;



  

  

  
