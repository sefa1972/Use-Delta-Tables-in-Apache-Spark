### Use Delta Tables in Apache Spark

Tables in Microsoft Fabric Lakehouse are based on the open-source Delta Lake format. Delta Lake adds support for relational semantics for both batch and streaming data. In this exercise, you'll create Delta tables and examine the data using SQL queries.

✅ Create a Workspace
Open your browser and go to Microsoft Fabric Home and sign in.

In the left sidebar, select Workspaces.

Create a new workspace with a name you choose and ensure it uses Fabric trial, Premium, or Fabric capacity.

Once created, the workspace should appear empty.

✅ Create a Lakehouse and Upload Data
In the left menu, select Create → under Data Engineering, choose Lakehouse.

Give your Lakehouse a unique name.

Download the dataset: products.csv.

Back in the Lakehouse, in the Explorer panel:

Click the ... next to the Files folder → create a new subfolder called products.

In the products folder → click ... → upload the products.csv file from your computer.

Verify the file appears in the folder.

✅ Explore the Data in a DataFrame
 - Create a new notebook.

 - Convert the first cell to Markdown, and use the following content:

# Delta Lake tables
Use this notebook to explore Delta Lake functionality
 - Add a code cell and paste this code:
   from pyspark.sql.types import StructType, IntegerType, StringType, DoubleType

# define the schema
schema = StructType() \
    .add("ProductID", IntegerType(), True) \
    .add("ProductName", StringType(), True) \
    .add("Category", StringType(), True) \
    .add("ListPrice", DoubleType(), True)

df = spark.read.format("csv").option("header", "true").schema(schema).load("Files/products/products.csv")
display(df)

✅ Create Delta Tables
### Managed Delta Table

df.write.format("delta").saveAsTable("managed_products")

Refresh the Tables folder to verify the new managed_products table.

### External Delta Table
 - In Explorer, copy the ABFS path from the Files folder.

 - Paste it in a new code cell and adjust the following code:
   df.write.format("delta").saveAsTable("external_products", path="abfss://workspace@tenant-onelake.dfs.fabric.microsoft.com/lakehousename.Lakehouse/Files/external_products")

✅ Compare Managed and External Tables
%%sql
DESCRIBE FORMATTED managed_products;

%%sql
DESCRIBE FORMATTED external_products;
 - Observe the storage paths. Managed tables are stored under /Tables, external tables under /Files.

✅ Drop Both Tables
%%sql
DROP TABLE managed_products;
DROP TABLE external_products;

✅ Create a Delta Table Using SQL
%%sql
CREATE TABLE products
USING DELTA
LOCATION 'Files/external_products';

Then:
%%sql
SELECT * FROM products;

✅ Explore Table Versioning
%%sql
UPDATE products
SET ListPrice = ListPrice * 0.9
WHERE Category = 'Mountain Bikes';

%%sql
DESCRIBE HISTORY products;

delta_table_path = 'Files/external_products'

# current version
current_data = spark.read.format("delta").load(delta_table_path)
display(current_data)

# original version
original_data = spark.read.format("delta").option("versionAsOf", 0).load(delta_table_path)
display(original_data)

✅ Analyze Delta Table Data Using SQL
%%sql
-- Create a temporary view
CREATE OR REPLACE TEMPORARY VIEW products_view AS
SELECT Category, COUNT(*) AS NumProducts, MIN(ListPrice) AS MinPrice, MAX(ListPrice) AS MaxPrice, AVG(ListPrice) AS AvgPrice
FROM products
GROUP BY Category;

SELECT * FROM products_view
ORDER BY Category;

%%sql
SELECT Category, NumProducts
FROM products_view
ORDER BY NumProducts DESC
LIMIT 10;

✅ Analyze With PySpark
from pyspark.sql.functions import col, desc

df_products = spark.sql("SELECT Category, MinPrice, MaxPrice, AvgPrice FROM products_view").orderBy(col("AvgPrice").desc())
display(df_products.limit(6))

✅ Use Delta Tables for Streaming (IoT Simulation – Optional)
Delta Lake supports streaming data via Spark Structured Streaming API, which allows Delta tables to act as both sinks and sources.
