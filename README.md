# **Bank Loan ETL & Visualization Project Report**

## **1. Abstract**

This project builds a complete **ETL (Extract, Transform, Load) pipeline** for bank loan analytics using **PySpark** and **Python**. It cleans, validates, and integrates branch, customer, and loan datasets into a unified master table. The pipeline standardizes financial data, generates analytical insights, and prepares the output for reporting and automated financial analysis.

---

## **2. Technologies Used**

* Python
* PySpark
* Pandas
* Matplotlib
* CSV Files
* Java JDK (required for Spark)

---

## **3. Dataset Description**

This project uses three CSV datasets:

* **Bank_Branch_Data.csv** – Branch details (branch_id, branch_name, branch_state)
* **Bank_Customer_Data.csv** – Customer demographic information
* **Bank_Loan_Data.csv** – Loan records linked to customers and branches

---

## **4. ETL Workflow**

The pipeline includes the following steps:

1. **Extract** – Load datasets into Spark DataFrames
2. **Clean** – Standardize column names, handle missing values, drop invalid rows
3. **Validate** – Check primary key duplicates and foreign key relationships
4. **Transform** – Join datasets and build the final master table
5. **Load** – Save processed outputs using Pandas
6. **Visualize** – Create charts with Matplotlib

---

## **5. Data Cleaning**

* Converted all column names to **snake_case** for consistency
* Removed rows containing null primary keys (`branch_id`, `customer_id`, `loan_id`)
* Filled remaining null fields with default values (`"Unknown"` or `0`)
* Ensured all fields were clean and ready for transformation

---

## **6. Data Validation**

* Checked and removed duplicate values in primary keys
* Performed foreign key validation using left joins
* Ensured all loan entries map correctly to valid customer and branch records

---

## **7. Transformations**

Performed the following joins to prepare the master table:

* `loan_df` **JOIN** `customer_df` ON `customer_id`
* Result **JOIN** `branch_df` ON `branch_id`

The final output, **final_df**, contains complete loan information with customer and branch details combined.

---

## **8. Business Insights Generated**

Using aggregations such as `groupBy()`, `sum()`, and `avg()`, the system produces:

* Total loan amount per branch
* Total loan amount per customer
* Average loan amount per branch
* Average loan amount per occupation
* State-wise loan distribution
* Top 5 customers with the highest loan amounts

---

## **9. Loading Output**

All Spark DataFrames were converted to Pandas using `toPandas()` and saved as CSV files using `to_csv()`.
The output files are stored in the **output-data** folder.

---

## **10. Visualizations**

Charts generated using Matplotlib:

* **Bar Chart** – Loan distribution per branch
* **Bar Chart** – Top 10 customers by loan amount
* **Bar Chart** – Average loan amount by occupation
* **Pie Chart** – State-wise loan distribution

**Figure 1:** Loan Distribution per Branch
**Figure 2:** Top 10 Customers by Total Loan Amount
**Figure 3:** Average Loan Amount by Occupation
**Figure 4:** State-wise Loan Distribution

---

## **11. Conclusion**

This ETL project successfully processed raw banking data, applied cleaning and validation, performed analytical transformations, and generated meaningful visual insights. The final reports and charts help understand loan distribution patterns, branch performance, and customer behavior.
This project demonstrates strong skills in **PySpark, Pandas, data validation, transformation workflows, and analytical reporting**.
