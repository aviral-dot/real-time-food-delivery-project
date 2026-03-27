![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/aws_project_architecture.drawio.png?raw=true)


THE ABOVE IS THE ARCHITECTURE OF REAL TIME FODD DELIVERY DATA PIPELINE WHICH INVOLVES THE USE OF AWS KINESIS , AWS EMR , AWS REDSHIFT , AWS S3 BUCKET , AWS CODEBUILD AND MOCK DATA GENERATOR.

**⚡️ Amazon Kinesis — Real-Time Data Streaming on AWS**

Amazon Kinesis is a fully managed service from AWS (Amazon Web Services) designed to collect, process, and analyze real-time streaming data. It's useful when you need to handle large volumes of data as it arrives, especially in real time.

THE KINESIS USES KINESIS STREAMS IN THE ABOVE PROJECT TAHT TAKES IN REAL-TIME FOOD DELIVERY DATA WHICH HAS 
        'OrderID'
        'CustomerID' 
        'RestaurantID'
        'RiderID'
        'OrderDate'
        'DeliveryTime'
        'OrderValue'
        'DeliveryFee'
        'TipAmount'
        'OrderStatus'
AS THE IMPORTANT DATA IN IT.THE KINESIS STORES THE REAL TIME DATA IN THE KINESIS SHARDS.

Shard
A shard is a unit of capacity and storage in Kinesis.

Each shard:

Can ingest up to 1 MB/sec or 1000 records/sec.

Can emit up to 2 MB/sec to consumers.

Stores data for a default of 24 hours (can be increased to 7 days)

 Record
Each record in Kinesis contains:

Data blob: Your actual data (e.g., JSON, CSV, etc., up to 1 MB).

Partition key: Used to group data (and determine shard placement).

Sequence number: Unique ID per record for ordering.

Example Record:
json
Copy
Edit
{
  "partitionKey": "user-1234",
  "data": "base64-encoded-data",
  "sequenceNumber": "49590338271490256608559692538361571095921575989136588898"
}

![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/emr.png?raw=true)

**✅ Amazon EMR (Elastic MapReduce)**


Amazon EMR (Elastic MapReduce) is a big data processing service provided by AWS. It allows you to easily run large-scale distributed data processing jobs using frameworks like Apache Spark, Hadoop, Hive, Presto, and more — without managing infrastructure.

A PySpark script is a Python program that uses Apache Spark (via the PySpark library) to process and analyze big data in a distributed computing environment.

**🔥 What is PySpark?**

PySpark = Python + Apache Spark
It allows you to write Spark applications in Python, instead of Java or Scala.

Apache Spark is a fast, in-memory data processing engine used for big data workloads like:

ETL (Extract, Transform, Load)

Machine learning

Real-time stream processing

SQL queries

🧾 What Does a PySpark Script Look Like?
A PySpark script typically:

Initializes a Spark session

Reads data (CSV, JSON, Parquet, from S3, HDFS, etc.)

Transforms data (filter, map, join, group, aggregate, etc.)

Performs actions (show, save to file, write to DB, etc.)



AMAZON EMR USED IN THIS PROJECT TO CLEAN,TRANFORM THE DATA COMING FROM KINESIS WHICH IS THEN STORED IN AMZON REDSHIFT DATAWAREHOUSE TO BE USED FOR GENERATING INSIGHTS FROM THAT DATA. 
A PYSPARK STREAMING SCRIPT IS DEVELOPED WHICH IS USED IN THE EMR FOR CLEANING.TRANFOMRATION AND PROCESSING

![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/emr2.png?raw=true)

THE ABOVE IMAGE SHOWS THAT AMAZON EMR IS FULL READY AND IN ACTIVE STATE ALONG WITH IT ON THE PRIMARY NODE IT OFFERS VARIOUS SERVICES WHICH WERE CHOSEN DURING THE MAKING OF AMAZON EMR.
THE FOLLOWINF SERVICES -
HDFS NODE
HUE 
LIVY
RESOURCE MANAGER
SPARK HISTORY SERVER

![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/Screenshot%202025-01-18%20195416.png?raw=true)

**📊 Amazon Redshift — AWS Data Warehouse Service**

Amazon Redshift is a fully managed cloud-based data warehouse provided by AWS. 
It allows you to run high-performance SQL analytics on large volumes of structured and semi-structured data (in terabytes or even petabytes).

AMAZON REDSHIFT IS USED IN THIS PROJECT TO STORE DATA THAT COMES FROM AAMAZON EMR TO STORE THEM IN DATA WAREHOUSE TO BE USED BY BUSINESSES TO GENERATE INSIGHTS FROM THE DATA .
THE DATA IS STORED IN REDSHIFT IN STAR SCHEMA WHICH HAS FACT AND DIMENSIONS TABLE.


![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/Screenshot%202025-01-18%20195427.png)

An Amazon Airflow environment refers to a managed workflow orchestration environment provided by Amazon MWAA (Managed Workflows for Apache Airflow). It allows you to run and scale Apache Airflow workflows on AWS without having to manage the underlying infrastructure.

**✅ What is Apache Airflow?**
Apache Airflow is an open-source platform used to programmatically author, schedule, and monitor workflows (DAGs). It is widely used in data engineering, ETL pipelines, ML workflows, etc.

**✅ What is Amazon MWAA?**
Amazon MWAA (Managed Workflows for Apache Airflow) is a fully managed service that makes it easy to run Apache Airflow on AWS.

APACHE AIRFLOW IS USED IN THIS PROJECT TO CREATE TWO DAGS AND ORCHESTRATE THEM :
1) **airflow_to_emr.py**

THIS DAG IS USED TO SPIN THE SPARK CLUSTER IN THE ACTIVE AMAZON EMR SERVICE SO THAT PROCESSING OF REAL TIME INCOMING DATA CAN BE DONE.
THIS CREATE A SPARK CLUSTER ALONG WITH THE FOLLOWING CONFIGURATION.
step_adder = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id='j-2FN0DTES2KKFF',
    aws_conn_id='aws_default',
    steps=[{
        'Name': 'Run PySpark Streaming Script',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--num-executors', '3',
                '--executor-memory', '6G',
                '--executor-cores', '3',
                '--packages', packages_list,
                '--jars', jdbc_jar_s3_path,
                's3://food-important-files/pyspark_script/pyspark_streaming.py',
                '--redshift_user', redshift_user,
                '--redshift_password', redshift_password,
                '--aws_access_key', aws_access_key,
                '--aws_secret_key', aws_secret_key,
            ],
        },
    }],
    dag=dag,
)

**2)dim_load_dag.py:**

THIS DAG OF AIRFLOW IS USED TO LOAD THE DIMENSION TABLE OF dimCustomers , dimRestaurants , dimDeliveryRiders , WITH DATA AND factOrders TABLE WILL KEEP THE RECORD OF REAL TIME DATA ACCORDING TO THE STAR SCHEMA STRUCTURE.

**⭐ What is Star Schema?**
A Star Schema organizes data into fact and dimension tables:

Fact Table: Central table that stores quantitative data (metrics/measures).

Dimension Tables: Surrounding tables that store descriptive attributes (who, what, when, where, how).

The schema resembles a star shape, hence the name.

🧱 Structure:
lua
Copy
Edit
               +-------------+
               |  Dim_Date   |
               +-------------+
                     |
                     |
+------------+   +------------+   +------------+   +------------+
| Dim_Customer | | Dim_Product | | Dim_Region  | | Dim_SalesRep|
+------------+   +------------+   +------------+   +------------+
       \             |              |              /
        \            |              |             /
                  +----------------------+
                  |      Fact_Sales      |
                  +----------------------+
                  | date_key             |
                  | customer_key         |
                  | product_key          |
                  | region_key           |
                  | salesrep_key         |
                  | total_sales          |
                  | quantity_sold        |
                  +----------------------+
                  

![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/Screenshot%202025-01-18%20230754.png?raw=true)

THE ABOVE IMAGE SHOWS THE SPINNING UP OF BOTH THE DAG.

![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/Screenshot%202025-01-18%20230912.png?raw=true)

THE ABOVE IMAGE SHOWS THE GRAPH OF THE DAGS THAT ARE SPINNED UP.
In Apache Airflow, a Graph refers to the graphical representation of a DAG (Directed Acyclic Graph) — a workflow made up of tasks with dependencies. This is visualized in the Graph View inside the Airflow UI.

**🧭 What is a DAG?**
A DAG (Directed Acyclic Graph) is a collection of tasks (nodes) connected by dependencies (edges). The tasks are executed in a specific order defined by the graph.

In Airflow:

Each task is a node.

Dependencies are arrows between tasks.

![image alt](https://github.com/aviral-dot/real-time-food-delivery-project/blob/main/Screenshot%202025-01-18%20230831.png?raw=true)

THE ABOVE IMAGE SHOWS THE CODEBUILD RUNNING AND BUILDING THE TASK. WE HAVE BUILDSPE.YML FILE IN THE CODEBUILD

**🚀 What is Amazon CodeBuild?**

Amazon CodeBuild is a fully managed continuous integration (CI) service by AWS that compiles source code, runs tests, and produces build artifacts. It eliminates the need to provision, manage, and scale your own build servers.

BUIDSPEC.YML DIRECTS THE JOB TO BE DONE OF CI/CD SIDE.

**AIRFLOW OVERVIEW------**
<img width="1625" height="918" alt="Screenshot 2026-03-26 220708" src="https://github.com/user-attachments/assets/2336b670-f671-4567-94f3-920edb4d1cc7" />



<img width="1621" height="922" alt="Screenshot 2026-03-26 220746" src="https://github.com/user-attachments/assets/83505418-e037-49e8-9a7b-8c8ea3ba0a0e" />

**📊 Data Modeling Pipeline (Airflow DAG Description)**

This workflow represents a data modeling pipeline orchestrated using Apache Airflow, where raw data is transformed into structured dimensional and fact tables, and finally triggers a downstream streaming process.

**🔹 1. Schema Initialization**
The pipeline begins with the task:
create_schema
This ensures that the required database schema is created before any tables are built.
**🔹 2. Dimension Tables Creation Flow**

For each dimension table (Delivery Riders, Restaurants, Customers), the pipeline follows a consistent pattern:

Step 1: Drop Existing Table
drop_dimDeliveryRiders
drop_dimRestaurants
drop_dimCustomers

👉 Ensures idempotency by removing old tables if they exist.

Step 2: Create New Table
create_dimDeliveryRiders
create_dimRestaurants
create_dimCustomers

👉 Defines fresh table structures.

Step 3: Load Data
load_dimDeliveryRiders
load_dimRestaurants
load_dimCustomers

👉 Loads processed data (likely from S3 to Redshift using S3ToRedshiftOperator).

**🔹 3. Fact Table Creation Flow**

After dimension tables are ready:

Step 1: Drop Existing Fact Table
drop_factOrders
Step 2: Create Fact Table
create_factOrders

👉 Builds the central fact table (orders) that references dimension tables.

**🔹 4. Final Trigger (Streaming Layer)**
trigger_spark_streaming_dag

👉 Once all tables are successfully created and loaded:

A downstream Spark Streaming DAG is triggered
This likely handles real-time data processing or updates

<img width="1625" height="912" alt="Screenshot 2026-03-26 220823" src="https://github.com/user-attachments/assets/694e987a-0d9d-4b39-8291-73282930dd05" />




<img width="1628" height="915" alt="Screenshot 2026-03-26 220851" src="https://github.com/user-attachments/assets/843443b0-76f3-42e7-995e-fd9c6a26b74f" />

**EMR OVERVIEW-------**
**EMR PROCESSING THE APACHE SPARK STREMAING SCRIPT.**
<img width="1623" height="911" alt="Screenshot 2026-03-26 220949" src="https://github.com/user-attachments/assets/b5170b05-a242-4098-bbad-0747e98a8112" />


**🔐 SSH Tunneling Setup for Amazon EMR**

**To establish a secure SSH connection to the EMR master node and enable port forwarding (SSH tunneling) for accessing internal services like:**

**Spark UI**
**Hadoop Resource Manager**
**Jupyter / Zeppelin (if configured)**
<img width="1627" height="910" alt="Screenshot 2026-03-26 221114" src="https://github.com/user-attachments/assets/0664e499-37ab-431c-9ce5-5c07a7a06fce" />

**🔐 Successful SSH Connection to EMR Cluster**

**This image shows a successful SSH connection established to the master node of an Amazon EMR cluster using PuTTY.**

<img width="1621" height="917" alt="Screenshot 2026-03-26 221325" src="https://github.com/user-attachments/assets/f6f92178-1125-4db3-abd2-28981a19ab07" />

**HADOOP OVERVIEW-----**

**⚡ Spark Job Execution on Hadoop YARN**

**This image shows a PySpark streaming job successfully running on a Hadoop cluster managed by Amazon EMR, monitored through the Apache Hadoop YARN Resource Manager UI.**

<img width="1622" height="915" alt="Screenshot 2026-03-26 221408" src="https://github.com/user-attachments/assets/1a41472a-39e1-4b7a-ae35-fdba3100e6fc" />



<img width="1624" height="911" alt="Screenshot 2026-03-26 221457" src="https://github.com/user-attachments/assets/c8778eb5-1be2-4581-a8ed-0ae9673fbbb0" />


**PROCESSING IN HADOOP GOING ON**
<img width="1622" height="914" alt="Screenshot 2026-03-26 221523" src="https://github.com/user-attachments/assets/37e1ff37-de03-4d10-af21-b144c706d8ca" />

**KINESIS OVERVIEW----**
**SENDING DATA TO KINESIS THROUGH VS CODE**

<img width="1628" height="909" alt="Screenshot 2026-03-26 221759" src="https://github.com/user-attachments/assets/5357e7b1-42b9-4d53-8201-d6e1dfec852a" />



<img width="1626" height="915" alt="Screenshot 2026-03-26 221821" src="https://github.com/user-attachments/assets/f71b9ebd-2f95-4472-978e-8f2837c01056" />

**📡 Real-Time Data Streaming with Kinesis Shards**

**This image shows real-time data being ingested into an Amazon Kinesis Data Streams stream, where incoming records are distributed across shards for scalable processing.**

<img width="1632" height="919" alt="Screenshot 2026-03-26 221950" src="https://github.com/user-attachments/assets/9ffbd780-f8a6-4fb5-b9ae-8cfea0125c40" />

**REDSHIFT OVERVIEW-----**

**Cluster selected: redshift-cluster-1**
**Database: dev**
**Under the database, there's a schema/folder named food_delivery_datamart (highlighted/expanded).**
**Inside it, a Tables section lists several tables:**
dimcustomers
dimdeliveryriders
dimrestaurants
factorders (this is the one the cursor is hovering over or recently selected)

<img width="1624" height="923" alt="Screenshot 2026-03-26 222337" src="https://github.com/user-attachments/assets/70498fac-454f-4cad-93cc-a787eb588a98" />

**QUERYING THE DATA TO GENERATE INSIGHTS**


<img width="1621" height="917" alt="Screenshot 2026-03-26 222405" src="https://github.com/user-attachments/assets/98c82acf-5791-4dd2-b9bd-f2a988935899" />

**QUICKSIGHT OVERVIEW ----**
<img width="1625" height="918" alt="Screenshot 2026-03-26 222859" src="https://github.com/user-attachments/assets/2170a56d-bac5-4c3b-b20a-9b82d27f1cc0" />
