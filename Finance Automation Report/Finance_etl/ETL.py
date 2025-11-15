import os
import sys
#
# from Tools.scripts.generate_opcode_h import header

# ENVIRONMENT SETUP
os.environ['JAVA_HOME'] = 'C:/Program Files/Java/jdk-21'
os.environ['SPARK_HOME']= 'C:/Users/asish/OneDrive/Desktop/Revature_Training/Revature_Project1/Finance/.venv/Lib/site-packages/pyspark'
# os.environ['HADOOP_HOME'] = "C:/hadoop"
os.environ['PYSPARK_PYTHON']= sys.executable
os.environ['PYSPARK_DRIVER_PYTHON']= sys.executable

# Session Setup ->Extract -> Clean -> Validate -> Transform -> Load (BigQuery or local)

# --------------------------
# STEP 1: Setup PySpark Session
# --------------------------
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Bank Dataset").getOrCreate()

# --------------------------
# STEP 2: EXTRACT the datasets
# --------------------------
branch_df=spark.read.csv('../data/Bank_Branch_Data.csv', header=True, inferSchema=True)
branch_df.show(5)
customer_df=spark.read.csv("../data/Bank_Customer_Data.csv", header=True, inferSchema=True)
customer_df.show(5)
loan_df=spark.read.csv("../data/Bank_Loan_Data.csv", header=True, inferSchema=True)
loan_df.show(5)

# --------------------------
# STEP 3: clean
# --------------------------
# first convert all the columns to lower snake_case( consistency)
def to_snake(df):
    return df.toDF(*[c.strip().lower().replace(" ", "_").replace("-", "_") for c in df.columns])
branch_df = to_snake(branch_df)
customer_df = to_snake(customer_df)
loan_df = to_snake(loan_df)

# if primary key id is null then delete the row
branch_df = branch_df.dropna(subset=["branch_id"])
customer_df=customer_df.dropna(subset=["customer_id"])
loan_df=loan_df.dropna(subset=["loan_id","customer_id", "branch_id"])

#if I find any null value in my data then make it unknown
branch_df = branch_df.fillna({
    "branch_name": "Unknown",
    "branch_state": "Unknown"
})
customer_df = customer_df.fillna({
    "first_name": "Unknown",
    "last_name": "Unknown",
    "city": "Unknown",
    "phone_number": "Unknown",
    "occupation": "Unknown"
})
loan_df = loan_df.fillna({"loan_amount": 0})

# ---------------------------
# Step 4: Data Validation
#----------------------------
# Delete Duplicates(pk>1)
# Branch PK check
branch_df.groupBy("branch_id").count().filter("count > 1").show()
branch_df = branch_df.dropDuplicates(["branch_id"])
print("Branch DataFrame\n")
branch_df.show()
# Customer PK check
customer_df.groupby("customer_id").count().filter("count > 1").show()
customer_df = customer_df.dropDuplicates(["customer_id"])
print("Customer Dataframe\n")
customer_df.show()
# Loan PK check
loan_df.groupBy("loan_id").count().filter("count > 1").show()
loan_df = loan_df.dropDuplicates(["loan_id"])
print("Loan DataFrame\n")
loan_df.show()

# now FK validation
# checking for customer_id and loan_id relation establishing
invalid_customer_fk = loan_df.join(
    customer_df,
    on="customer_id",
    how="left"
).filter(customer_df.customer_id.isNull())
invalid_customer_fk.show()
# checking for loan_id and branch_id relation establishing
invalid_branch_fk = loan_df.join(
    branch_df,
    on="branch_id",
    how="left"
).filter(branch_df.branch_id.isNull())
invalid_branch_fk.show()

# -------------------------------------------
# Step 5 - Transform : (join all 3 datasets )
# --------------------------------------------
# 1st join the loan with customer
loan_customer_df=loan_df.join(
    customer_df,
    on="customer_id",
    how="inner"
)
# then now join the branch_df with the result_df
final_df=loan_customer_df.join(
    branch_df,
    on="branch_id",
    how="inner"
)
print("final Table\n")
final_df.show(30)
print("Total records\n",final_df.count())

# Aggregation
from pyspark.sql.functions import sum
# 1. Total Loan Amount per Branch
loan_per_branch = final_df.groupby("branch_id","branch_name").agg(sum("loan_amount").alias("total_loan_amount"))
print("loan_per_branch\n")
loan_per_branch.show()

# 2. Total Loan Amount per Customer
loan_per_customer=final_df.groupby("customer_id", "first_name", "last_name").agg(sum("loan_amount").alias("total_loan_amount"))
print("loan_per_customer\n")
loan_per_customer.show()

from pyspark.sql.functions import avg
# 3. Avg Loan Amount Per branch
avg_loan_branch = final_df.groupBy("branch_id", "branch_name").agg(
    avg("loan_amount").alias("avg_loan_amount")
)
avg_loan_branch.show()

# 4. Average Loan Amount per Customer Occupation
avg_loan_customer = final_df.groupby("occupation").agg(avg("loan_amount").alias("avg_loan_amount"))
avg_loan_customer.show()

# 5. State-wise Loan Distribution
loan_per_state = final_df.groupby("branch_state").agg(sum("loan_amount").alias("state_total_loan_amount"))
loan_per_state.show()

# 6. Top 5 Customers with Highest Loan Amount
top_customers= loan_per_customer.orderBy("total_loan_amount", ascending=False).limit(5)
top_customers.show()

# ------------------------------------
# STEP 7: LOAD USING PANDAS
# ------------------------------------
# ste the folder and allow to rewrite
os.makedirs("../output-data", exist_ok=True)

# Convert Spark DF to Pandas DF
final_df_pd = final_df.toPandas()
loan_customer_pd = loan_customer_df.toPandas()
loan_per_branch_pd = loan_per_branch.toPandas()
loan_per_customer_pd = loan_per_customer.toPandas()
avg_loan_branch_pd = avg_loan_branch.toPandas()
avg_loan_customer_pd = avg_loan_customer.toPandas()
loan_per_state_pd = loan_per_state.toPandas()
top_customers_pd = top_customers.toPandas()

# Save using Pandas
final_df_pd.to_csv("../output-data/final_table.csv", index=False)
loan_customer_pd.to_csv("../output-data/loan_per_customer.csv", index=False)
loan_per_branch_pd.to_csv("../output-data/loan_per_branch.csv", index=False)
loan_per_customer_pd.to_csv("../output-data/loan_per_customer_amount.csv", index=False)
avg_loan_branch_pd.to_csv("../output-data/avg_loan_per_branch.csv", index=False)
avg_loan_customer_pd.to_csv("../output-data/avg_loan_per_occupation.csv", index=False)
loan_per_state_pd.to_csv("../output-data/loan_per_state.csv", index=False)
top_customers_pd.to_csv("../output-data/top_5_customers.csv", index=False)

# ---------------------------------
# Step 8 -> Visualization
# ---------------------------------

import matplotlib.pyplot as plt

# Loan Per Branch (Bar Chart)
plt.figure(figsize=(16,5))
plt.bar(loan_per_branch_pd["branch_name"],
        loan_per_branch_pd["total_loan_amount"],
        color="skyblue", edgecolor="black")
plt.xticks(rotation=90, fontsize=8)
plt.title("Total Loan Amount Per Branch")
plt.tight_layout()
plt.show()


# Loan Per Customer (Top 10 Customers)
top10 = loan_per_customer_pd.nlargest(10, "total_loan_amount")

plt.figure(figsize=(10,5))
plt.bar(top10["first_name"] + " " + top10["last_name"],
        top10["total_loan_amount"],
        color="orange", edgecolor="black")
plt.xticks(rotation=45, ha="right")
plt.title("Top 10 Customers by Loan Amount")
plt.tight_layout()
plt.show()


# Average Loan Per Occupation
plt.figure(figsize=(12,5))
plt.bar(avg_loan_customer_pd["occupation"],
        avg_loan_customer_pd["avg_loan_amount"],
        color="purple", edgecolor="black")
plt.xticks(rotation=45, ha="right")
plt.title("Average Loan Amount per Occupation")
plt.tight_layout()
plt.show()


# Loan Distribution Per State (Pie Chart)

plt.figure(figsize=(8,8))
plt.pie(loan_per_state_pd["state_total_loan_amount"],
        labels=loan_per_state_pd["branch_state"],
        autopct="%1.1f%%")
plt.title("Loan Distribution by State")
plt.tight_layout()
plt.show()


print("\nAll tasks completed successfully!")