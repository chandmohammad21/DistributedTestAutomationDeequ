# DistributedTestAutomationDeequ (Unit Tests included)
This is a distributed automation framework written in Amazon Deequ using Spark and can be used on prem and well as on cloud. The code contains implementation on local and on Azure Databricks.

Databricks Scala style guide and plugins are used.

Logging added will work on local as well as Azure appinsights.

### Library and versions:
- Spark 3.x
- dbutils-api_2.11 0.0.4
- gson 2.8.0
- spark-testing-base 2.4.5_0.14.0
- deequ 1.2.2-spark-3.0

### Benefits
 - Data validation at scale
 - Unit test cases on Data
 - E2E test cases for your various data files

 Test data used can be foud in: src/main/resources/files/DISTRIBUTOR/DISTRIBUTOR.csv

 Test cases that are covered in this short application:
 - Validate all string columns are trimmed
 - Validate all/required string columns are uppercase
 - Not Null or completeness tests for mandatory columns
 - Non negetive validation for fields which cannot be negative
 - Fields with specific values only, example Gender should have only M and F 
 - Column values based on other column values, like Salary should be 0 in case age < 15 (Labor regulations)

The functionality is not limited to these test cases and can be a lot more.

To enhance this for your files, simply add a class for a file with its own member fields and run test cases.

(There are various functions/methods that are unused in code, Will try to add their usage in near future)

