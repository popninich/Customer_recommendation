# Customer Segmentation & Recommender Pipeline (Distributed Production Version)

## Project Overview

ระบบนี้เป็นชุดสมบูรณ์ของ Customer Segmentation และ Customer Recommender ซึ่งออกแบบมาเพื่อรองรับข้อมูลขนาดใหญ่ (Big Data scale) ระดับหลายสิบล้านถึงหลักร้อยล้านเรคอร์ด โดยทำงานแบบ Distributed เต็มรูปแบบบน Spark Cluster

โครงการนี้ประกอบด้วยสองส่วนหลัก

- Stage 1: Distributed Customer Clustering
- Stage 2: Distributed Customer Recommender

ทั้งสอง stage ทำงานร่วมกันเป็นpipelineเดียวต่อเนื่องเพื่อสร้างระบบ personalization ที่ขยายไปสู่ production ได้จริง

---

# Stage 1: Distributed Customer Clustering

## เป้าหมายของ Stage 1

- Load ข้อมูล customer features
- แบ่งชุด feature ออกเป็น 3 ระดับ (Tier 1, Tier 2, Tier 3)
- ทำ Distributed Clustering ด้วย Spark MLlib KMeans
- ใช้ Silhouette Score เพื่อหา optimal number of clusters (auto k search)
- Export ผลลัพธ์เป็นไฟล์ clustered customer เก็บไว้ใน Distributed Storage

## Pipeline Flow Summary - Stage 1

1. Load customer feature dataset (Parquet format)
2. กำหนด Tier Feature Set (Tier 1-3)
3. Apply VectorAssembler สร้าง feature vector
4. Apply StandardScaler สำหรับ normalize features
5. Loop ค้นหา optimal k (KMeans + Silhouette Score)
6. Export clustered customer data (customer_id + cluster) เป็น Parquet

## Output Structure - Stage 1

| customer_id | cluster |
|--------------|---------|
| C00001       | 2       |
| C00002       | 1       |
| ...          | ...     |

## Deployment Instruction - Stage 1

1. เตรียม Spark Cluster พร้อม Distributed Storage (S3, GCS, HDFS)
2. Load dataset (customer_data.parquet) เข้า Spark
3. รัน clustering pipeline

spark-submit --master yarn --deploy-mode cluster customer_clustering_pipeline.py

4. Export ผลลัพธ์ไปเก็บที่ s3://your-bucket/customer_clustered/  

---

# Stage 2: Distributed Customer Recommender

## เป้าหมายของ Stage 2

- โหลดผลลัพธ์ clustered customer จาก Stage 1
- เชื่อมโยง cluster_id แต่ละกลุ่มเข้ากับกลุ่มสินค้า (item catalog)
- Apply recommendation logic แบบ Distributed
- Export output สำหรับใช้งาน downstream ต่อไปได้เลย

## Pipeline Flow Summary - Stage 2

1. Load clustered customer data จาก S3 (Parquet format)
2. สร้าง Item Catalog ให้แต่ละ Cluster (mockup หรือ generate จาก transaction log จริงใน production)
3. สร้าง Spark UDF สำหรับ apply logic การแนะนำสินค้า
4. Apply recommender function แบบ distributed บน Spark
5. Export output กลับไปยัง Distributed Storage (S3)

## Output Structure - Stage 2

| customer_id | cluster | recommendations |
|--------------|---------|-------------------|
| C00001       | 2       | ['Thai Food A', 'Pad Thai B'] |
| C00002       | 1       | ['Pizza A', 'Burger B'] |
| ...          | ...     | ... |

## Deployment Instruction - Stage 2

1. ให้แน่ใจว่าไฟล์ clustered customer (จาก Stage 1) พร้อมแล้วใน S3
2. อัพโหลดโค้ด recommender pipeline เข้า Spark Cluster
3. รันด้วย spark-submit

spark-submit --master yarn --deploy-mode cluster customer_recommender_pipeline.py

4. ตรวจสอบ output ที่ s3://your-bucket/customer_recommendations/

---

# ที่มาของ Item Catalog ใน Production

- ใน production จริง item_catalog ไม่ควร manual แต่ควร generate จาก transaction log จริง
- สามารถทำด้วย Spark SQL aggregate:

SELECT cluster, item_name, COUNT(*) 
FROM transaction_log
GROUP BY cluster, item_name
ORDER BY cluster, COUNT(*) DESC

- นำ top-N ที่นิยมที่สุดในแต่ละ cluster มาสร้าง item catalog อัตโนมัติ

---

# สาเหตุที่ต้องใช้ Spark Distributed ทั้งระบบ

- ข้อมูลขนาดใหญ่เกิน capacity ของ single machine
- Spark MLlib รองรับ Distributed ML โดยตรง
- Spark SQL & UDF สามารถ transform data ได้แบบกระจายทุกขั้นตอน
- Parquet format ช่วยให้ downstream query ทำงานเร็วในระบบ production

---

# Technology Stack

- Distributed Compute: Spark MLlib (PySpark)
- Storage: AWS S3 (Parquet format)
- Data Processing: Spark UDF, Spark SQL
- Deployment: spark-submit (Databricks, EMR, Dataproc)
- Monitoring: Airflow, MLflow, Prometheus, Grafana (Optional)

---

# จุดสำคัญเพิ่มเติมใน Production

- สามารถเชื่อมต่อ Feature Store, Metadata Store ในขั้นตอน feature engineering ได้ใน Stage 1
- สามารถเชื่อมต่อกับ Marketing Automation, Personalization API ใน downstream ได้ใน Stage 2
- UDF ควร migrate เป็น Pandas UDF สำหรับ performance ที่ดีกว่าใน cluster ขนาดใหญ่ขึ้น
- ใช้ Airflow scheduler สำหรับ orchestrate ทั้ง pipeline ให้ทำงานอัตโนมัติ

---



# Customer Segmentation & Recommender Pipeline (Distributed Production Version)

## Project Overview

This project implements a complete Customer Segmentation and Recommender system designed to handle large-scale datasets (Big Data scale), processing tens of millions to hundreds of millions of records efficiently using a fully distributed Spark Cluster.

The project is divided into two main stages:

- Stage 1: Distributed Customer Clustering
- Stage 2: Distributed Customer Recommender

Both stages work together as a unified pipeline to power a production-grade personalization system.

---

# Stage 1: Distributed Customer Clustering

## Objectives of Stage 1

- Load customer feature datasets
- Support multiple feature sets (Tier 1, Tier 2, Tier 3)
- Perform distributed clustering using Spark MLlib KMeans
- Use Silhouette Score to automatically select the optimal number of clusters (auto k search)
- Export the clustering results as customer-cluster files into Distributed Storage

## Pipeline Flow Summary - Stage 1

1. Load customer feature dataset (Parquet format)
2. Select Tier Feature Set (Tier 1-3)
3. Apply VectorAssembler to create feature vectors
4. Apply StandardScaler to normalize features
5. Loop through k values to find optimal k (KMeans + Silhouette Score)
6. Export clustered customer data (customer_id + cluster) as Parquet files

## Output Structure - Stage 1

| customer_id | cluster |
|--------------|---------|
| C00001       | 2       |
| C00002       | 1       |
| ...          | ...     |

## Deployment Instructions - Stage 1

1. Prepare Spark Cluster with Distributed Storage (S3, GCS, HDFS)
2. Load dataset (customer_data.parquet) into Spark
3. Run the clustering pipeline

spark-submit --master yarn --deploy-mode cluster customer_clustering_pipeline.py

4. Export output to: s3://your-bucket/customer_clustered/

---

# Stage 2: Distributed Customer Recommender

## Objectives of Stage 2

- Load clustered customer data from Stage 1
- Map each cluster_id to its corresponding item catalog
- Apply distributed recommendation logic using Spark
- Export results for downstream consumption

## Pipeline Flow Summary - Stage 2

1. Load clustered customer data from S3 (Parquet format)
2. Generate Item Catalog for each cluster (mockup or generated from transaction logs in production)
3. Create Spark UDF to apply recommendation logic
4. Apply the recommender function in distributed mode on Spark
5. Export results back to Distributed Storage (S3)

## Output Structure - Stage 2

| customer_id | cluster | recommendations |
|--------------|---------|-------------------|
| C00001       | 2       | ['Thai Food A', 'Pad Thai B'] |
| C00002       | 1       | ['Pizza A', 'Burger B'] |
| ...          | ...     | ... |

## Deployment Instructions - Stage 2

1. Ensure clustered customer file (from Stage 1) is available on S3
2. Upload recommender pipeline code to Spark Cluster
3. Execute using spark-submit

spark-submit --master yarn --deploy-mode cluster customer_recommender_pipeline.py

4. Verify output at: s3://your-bucket/customer_recommendations/

---

# Generating Item Catalog for Production

- In real production, the item_catalog should not be manually created but generated from historical transaction logs.
- This can be achieved using Spark SQL aggregation as follows:

SELECT cluster, item_name, COUNT(*) 
FROM transaction_log
GROUP BY cluster, item_name
ORDER BY cluster, COUNT(*) DESC

- Use the top-N most popular items per cluster to automatically generate the item catalog.

---

# Why Full Spark Distributed Pipeline is Required

- Dataset sizes exceed the capacity of single machine memory.
- Spark MLlib fully supports Distributed Machine Learning natively.
- Spark SQL and UDF enable efficient distributed transformations.
- Parquet format accelerates downstream queries for production scale.

---

# Technology Stack

- Distributed Compute: Spark MLlib (PySpark)
- Storage: AWS S3 (Parquet format)
- Data Processing: Spark UDF, Spark SQL
- Deployment: spark-submit (Databricks, EMR, Dataproc)
- Monitoring: Airflow, MLflow, Prometheus, Grafana (Optional)

---

# Key Production Considerations

- Feature Store or Metadata Store can be integrated in Stage 1 for feature engineering.
- Marketing Automation or Personalization APIs can integrate downstream after Stage 2.
- UDF should be upgraded to Pandas UDF for better performance on larger clusters.
- Use Airflow scheduler to orchestrate and automate the full pipeline.

---


