from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import pandas as pd

# 1. Start Spark Session
spark = SparkSession.builder \
    .appName("Distributed Customer Clustering") \
    .getOrCreate()

# 2. Load dataset from distributed storage
# แนะนำให้แปลง customer_data.csv เป็น parquet ก่อนเพื่อ speed
# สมมุติใช้ parquet format แล้วอยู่บน S3 หรือ HDFS

df = spark.read.parquet("s3://your-bucket/customer_data_parquet/")

# 3. Handle categorical feature encoding (area)
if 'area' in df.columns and dict(df.dtypes)['area'] == 'string':
    indexer = StringIndexer(inputCol='area', outputCol='area_index')
    df = indexer.fit(df).transform(df).drop('area').withColumnRenamed('area_index', 'area')

# 4. Handle missing values (fill nulls)
df = df.fillna(0)

# 5. Define Tier feature sets
TIER_FEATURES = {
    1: [
        'total_orders', 'avg_order_value',
        'thai_pct', 'japanese_pct', 'chinese_pct', 'western_pct',
        'morning_pct', 'lunch_pct', 'dinner_pct',
        'area', 'age'
    ],
    2: [
        'total_orders', 'avg_order_value',
        'thai_pct', 'japanese_pct', 'chinese_pct', 'western_pct',
        'morning_pct', 'lunch_pct', 'dinner_pct',
        'area', 'age',
        'loyalty', 'recency', 'discount_usage', 'diversity'
    ],
    3: [
        'total_orders', 'avg_order_value',
        'thai_pct', 'japanese_pct', 'chinese_pct', 'western_pct',
        'morning_pct', 'lunch_pct', 'dinner_pct',
        'area', 'age',
        'loyalty', 'recency', 'discount_usage', 'diversity',
        'full_timeseries_feature', 'context_feature', 'promo_response_feature', 'seasonality_feature'
    ]
}

# 6. Hyperparameters
K_MIN = 2
K_MAX = 50  #สามารถเพิ่มได้ถ้า cluster ใหญ่

results = []

for TIER in [1, 2, 3]:
    print(f"\n=== Evaluating Tier {TIER} ===")
    selected_columns = TIER_FEATURES[TIER]

    for col in selected_columns:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in dataset")

    assembler = VectorAssembler(inputCols=selected_columns, outputCol="features")
    df_vector = assembler.transform(df)

    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    scaler_model = scaler.fit(df_vector)
    df_scaled = scaler_model.transform(df_vector)

    best_score = -1
    best_k = 0

    for k in range(K_MIN, K_MAX+1):
        kmeans = KMeans(k=k, seed=42, featuresCol="scaledFeatures")
        model = kmeans.fit(df_scaled)
        predictions = model.transform(df_scaled)

        evaluator = ClusteringEvaluator(featuresCol="scaledFeatures", metricName="silhouette")
        score = evaluator.evaluate(predictions)

        if k <= 10 or k % 10 == 0:
            print(f"Tier {TIER} | k={k} | silhouette={score:.4f}")

        if score > best_score:
            best_score = score
            best_k = k

    results.append({'Tier': TIER, 'Best_k': best_k, 'Silhouette': best_score})

# 7. Summary Output
result_df = pd.DataFrame(results)
print("\n===== SUMMARY =====")
print(result_df)

best_tier = result_df.sort_values(by='Silhouette', ascending=False).iloc[0]
print(f"\n===== RECOMMENDED TIER =====")
print(f"Tier {int(best_tier['Tier'])} is recommended with Silhouette={best_tier['Silhouette']:.3f} and k={int(best_tier['Best_k'])}")

# 8. Optional: save output result to S3 or local for next pipeline
result_df.to_csv("customer_clustered.csv", index=False)

# 9. Stop spark session
spark.stop()
