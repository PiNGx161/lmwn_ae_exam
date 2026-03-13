# Analytics Engineering Exam

You are an analytics engineer working in a food delivery platform company. Different business teams request reports to monitor their operations, campaign performance, customer behavior, and service quality. 

## 🎯 Your Goal

* Understand the business requirement of each report.  
* Use the provided data files as your data source.
  * DuckDB file named: `ae_exam_db.duckdb`
* **Build a data model** however you like (e.g., star schema, normalized, modular staging). You can create as many layers as you think are appropriate to support reporting needs.   
  * Write all models you created back into a single DuckDB file named: `ae_exam_db.duckdb` 
  * Each model must use the following naming convention: `model_<layer>_<your_model_name>` 
* Draw and submit a data relationship diagram (ERD or lineage diagram).  
* **Generate the final result** of the reports based on the requirements.
  * Write all reports you created back into a single DuckDB file named: `ae_exam_db.duckdb` 
  * Each report must use the following naming convention: `report_<report_name>`
 
## 📬 How to submit your work

* Push your complete project to a **public Git repository**
  * Repository name: `lmwn_ae_exam`
* Paste the Git repository link in the **submission form** (you’ll find the form link in the email)
* Export the full Git project (including the .git folder and commit history) as a ZIP file
* Attach the ZIP file to the submission form (same form as above)
* _**Both the Git repo and the ZIP file must contain the exact same version of your work**_

## 💡 Hints & Bonus Points

- You may use **any tool or language** to build your data model (e.g. SQL, dbt, Python, notebooks).
- **Bonus**: If you use [dbt](https://www.getdbt.com/) to build your data model, your submission will be eligible for **bonus points**.
- If you include **tests, data validation, or documentation**, that will also be considered a strong plus.
