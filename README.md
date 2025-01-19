# Built a Data Warehouse & ELT Pipeline with DBT and Luigi

# Requirements

- OS:
    - Linux
    - WSL (Windows Subsystem For Linux)
      
- Tools:
    - Dbeaver (using postgreSQL)
    - Docker
    - DBT
    - Cron
      
- Programming Language:
    - Python
    - SQL
      
- Python Libray:
    - Luigi
    - Pandas


---

# Preparations

- ## Get the dataset

  Clone or download this repository to obtain the populated data for the source database.

  ```
  git clone https://github.com/Kurikulum-Sekolah-Pacmann/pactravel-dataset.git
  ```

---

- ## Setup project environment

  Set up and activate a Python environment to isolate the project's dependencies.
  
  ```
  python3 -m venv your_project_name         
  source your_project_name/bin/activate    
  ```

---
- ## Install _requirements.txt_**
  
  Install the dependencies listed in the _requirements.txt_ file within the newly created environment.
  
  ```
  pip install -r requirements.txt
  ```

---

- ## Create env file** in project root directory :
  ```
  # Source
  SRC_POSTGRES_DB=...
  SRC_POSTGRES_HOST=...
  SRC_POSTGRES_USER=...
  SRC_POSTGRES_PASSWORD=...
  SRC_POSTGRES_PORT=...

  # DWH
  DWH_POSTGRES_DB=...
  DWH_POSTGRES_HOST=...
  DWH_POSTGRES_USER=...
  DWH_POSTGRES_PASSWORD=...
  DWH_POSTGRES_PORT=...


  # DIRECTORY
  # Adjust with your directory. make sure to write full path
  DIR_ROOT_PROJECT=...     # <project_dir>
  DIR_TEMP_LOG=...         # <project_dir>/pipeline/temp/log
  DIR_TEMP_DATA=...        # <project_dir>/pipeline/temp/data
  DIR_EXTRACT_QUERY=...    # <project_dir>/pipeline/src_query/extract
  DIR_LOAD_QUERY=...       # <project_dir>/pipeline/src_query/load
  DIR_DBT_TRANSFORM=...  # <project_dir>/<your_dbt_project>
  DIR_LOG=...              # <project_dir>/logs/
    ```

- # Run Data Sources & Data Warehouses** :
  ```
  docker compose up -d
  ```

# Orchestrate ELT Pipeline
- Create schedule to run pipline every one hour.
  ```
  0 * * * * <project_dir>/elt_run.sh
  ```
---

# P.S

Well, anyway, you can check out [this article](https://medium.com/@fajariana.tm/dimensional-modeling-and-elt-automation-for-online-travel-agencies-a-case-study-with-dbt-and-luigi-f3a4061e9bba) where I explain how I design the data warehouse. I’m a bit short on time to whip up a polished README.md right now, but don’t worry—I’ll come back and give you a fancy version later. So, please, bear with me on this one!