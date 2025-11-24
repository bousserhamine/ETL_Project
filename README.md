📋 Project Overview

This project implements a full open-source ETL (Extract, Transform, Load) pipeline designed to automate and centralize data workflows for multiple core business services.
The solution replaces manual data handling with a robust, automated architecture that ingests data from disparate sources, transforms it into actionable insights, and ensures data integrity through automated testing.

🎯 Business Domains Covered

•	📞 Phoning: Call center metrics, agent performance, and volume tracking.
•	🚜 Field Services: On-site operations data and resource allocation.
•	⚖️ Litigation: Legal case tracking and compliance reporting

🏗️ Architecture & Tech Stack

The pipeline is built on a modern data stack, fully containerized for consistency across environments.


Orchestration	( Apache Airflow )	: Schedules DAGs and manages dependencies between extraction and transformation tasks.
Extraction	( Python ) :	Custom scripts to pull data from APIs/Databases and load into the staging area.
Storage	( PostgreSQL ): 	Acts as the central Data Warehouse for raw and transformed data.
Transformation	( dbt (Core) )	: Handles SQL transformations, lineage documentation, and data modeling (T in ELT).
Quality	 ( dbt Tests )	: Automated schema and logic tests (not null, unique, referential integrity).
Infrastructure	( Docker )	: Ensures the entire stack runs in isolated, reproducible containers.

✨ Key Features

•	Automated Workflows:  Airflow DAGs run on scheduled intervals, removing the need for manual intervention.
•	Data Quality & Reliability:  Integrated dbt test suites run before data is promoted to production tables, stopping bad data at the source.
•	Traceability:  Full data lineage graphs allow the team to trace any metric back to its raw source.
•	Scalability:  The modular design allows for easy addition of new business services (e.g., Marketing or Finance) without refactoring the core pipeline.


