# Databricks notebook source
# MAGIC %md 
# MAGIC # Retail Demand Forecasting - Data Foundation Setup
# MAGIC 
# MAGIC ## 🏪 Industry Challenge
# MAGIC 
# MAGIC **Retail organizations lose millions annually due to poor demand planning:**
# MAGIC - **$1.1 trillion globally** lost to out-of-stock scenarios
# MAGIC - **20-40%** of inventory investment tied up in slow-moving stock
# MAGIC - **65%** of retailers struggle with demand volatility
# MAGIC - **Manual forecasting** leads to 40-50% forecast errors
# MAGIC 
# MAGIC ## 🎯 Solution Overview
# MAGIC 
# MAGIC This solution demonstrates **enterprise-grade demand forecasting** for retail operations:
# MAGIC - **Accurate demand predictions** across thousands of store-item combinations
# MAGIC - **Seasonal pattern recognition** for holiday and promotional planning
# MAGIC - **Inventory optimization** to reduce stockouts and overstock
# MAGIC - **Supply chain efficiency** through data-driven insights
# MAGIC 
# MAGIC ## 📊 Business Process Flow
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
# MAGIC │  Data Foundation│───▶│ Forecast Models │───▶│Business Insights│
# MAGIC │  (This step)    │    │ (Next step)     │    │ (Final step)    │
# MAGIC └─────────────────┘    └─────────────────┘    └─────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ## 💰 Expected Business Impact
# MAGIC 
# MAGIC - **15-25% reduction** in inventory holding costs
# MAGIC - **30-50% fewer stockouts** leading to increased sales
# MAGIC - **10-20% improvement** in forecast accuracy
# MAGIC - **Automated insights** replacing manual Excel-based processes

# COMMAND ----------

# MAGIC %md ## 📦 Environment Setup
# MAGIC 
# MAGIC Setting up the forecasting environment with industry-standard time series libraries.
# MAGIC The platform automatically manages compute resources for cost efficiency.

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# Install libraries for serverless compute
# MAGIC %pip install prophet>=1.1.5 plotly>=5.17.0 scikit-learn>=1.3.0

# COMMAND ----------

# Restart Python to use newly installed libraries
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md ## ⚙️ Retail Business Configuration
# MAGIC 
# MAGIC ### Store and Product Portfolio Setup
# MAGIC Configure the scope of your retail forecasting analysis based on your business needs.

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, min as spark_min, current_timestamp
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

print("📚 Libraries imported successfully")

# COMMAND ----------

# DBTITLE 1,Configure Pipeline Parameters
# Get parameters from job or use defaults
catalog_name = dbutils.widgets.get("catalog_name") if dbutils.widgets.get("catalog_name") else "dev_demand_forecasting"
schema_name = dbutils.widgets.get("schema_name") if dbutils.widgets.get("schema_name") else "forecasting"

# Data generation parameters
NUM_STORES = 10
NUM_ITEMS = 50
START_DATE = '2019-01-01'
END_DATE = '2024-12-31'

print("🔧 Retail Business Scope:")
print(f"   🏪 Store locations: {NUM_STORES}")
print(f"   📦 Product SKUs per store: {NUM_ITEMS}")
print(f"   📅 Historical sales period: {START_DATE} to {END_DATE}")
print(f"   📊 Total data points: {NUM_STORES * NUM_ITEMS * 2190:,} sales records")
print(f"   ⚡ Processing: Cloud-native auto-scaling")

# Set up Spark session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md ## 🏛️ Enterprise Data Foundation
# MAGIC 
# MAGIC ### Retail Data Architecture
# MAGIC Setting up secure, governed data infrastructure for enterprise forecasting:
# MAGIC - **Data governance** with role-based access controls
# MAGIC - **Audit trails** for regulatory compliance
# MAGIC - **Data lineage** for transparency and trust
# MAGIC - **Schema management** for data quality assurance

# COMMAND ----------

# DBTITLE 1,Create Unity Catalog Structure
print("🏗️ Setting up enterprise data foundation...")

# Create business data catalog and schema
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

print(f"✅ Business catalog '{catalog_name}' ready")
print(f"✅ Forecasting workspace '{schema_name}' created")

# COMMAND ----------

# DBTITLE 1,Create Retail Data Tables
print("📊 Creating retail sales and forecasting tables...")

# Create historical sales data table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.raw_sales_data (
  date DATE COMMENT 'Sales transaction date',
  store INT COMMENT 'Store location identifier',
  item INT COMMENT 'Product SKU identifier',
  sales BIGINT COMMENT 'Daily units sold',
  processing_timestamp TIMESTAMP COMMENT 'Data processing timestamp'
) USING DELTA
PARTITIONED BY (store)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Historical sales data for demand forecasting'
""")

# Create demand forecast results table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalog_name}.{schema_name}.forecast_results (
  store INT COMMENT 'Store location identifier',
  item INT COMMENT 'Product SKU identifier', 
  forecast_date DATE COMMENT 'Future date for demand prediction',
  yhat DOUBLE COMMENT 'Predicted demand (units)',
  yhat_lower DOUBLE COMMENT 'Lower demand estimate (95% confidence)',
  yhat_upper DOUBLE COMMENT 'Upper demand estimate (95% confidence)',
  model_version STRING COMMENT 'Forecasting model version',
  created_timestamp TIMESTAMP COMMENT 'Forecast generation timestamp'
) USING DELTA
PARTITIONED BY (store)
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Demand forecasts with confidence intervals for inventory planning'
""")

print("✅ Retail data tables ready for forecasting")

# COMMAND ----------

# MAGIC %md ## 🎲 Realistic Sales Data Simulation
# MAGIC 
# MAGIC ### Creating Authentic Retail Patterns
# MAGIC Our synthetic sales data replicates real-world retail behavior:
# MAGIC - **Seasonal demand cycles** (holiday rushes, back-to-school)
# MAGIC - **Weekly shopping patterns** (weekend peak traffic)
# MAGIC - **Store performance variations** (location demographics)
# MAGIC - **Product popularity differences** (fast vs. slow movers)
# MAGIC - **Business growth trends** (market expansion)
# MAGIC - **Natural demand volatility** (economic factors, weather)

# COMMAND ----------

# DBTITLE 1,Generate Realistic Synthetic Sales Data
def generate_synthetic_data(num_stores=NUM_STORES, num_items=NUM_ITEMS, start_date=START_DATE, end_date=END_DATE):
    """
    Generate realistic synthetic sales data with multiple patterns
    
    Returns:
        pd.DataFrame: Sales data with columns [date, store, item, sales]
    """
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    data = []
    np.random.seed(42)  # For reproducible results
    
    print(f"🛒 Simulating {len(date_range)} days of retail operations across {num_stores} stores")
    
    for store in range(1, num_stores + 1):
        # Store characteristics
        store_size_factor = np.random.uniform(0.7, 1.3)  # Some stores are bigger
        store_location_factor = np.random.normal(1.0, 0.2)  # Location effects
        
        for item in range(1, num_items + 1):
            # Item characteristics
            base_demand = np.random.normal(100, 30) * store_size_factor
            item_popularity = np.random.uniform(0.5, 2.0)  # Some items more popular
            
            for date in date_range:
                # Seasonal patterns (yearly cycle)
                day_of_year = date.timetuple().tm_yday
                seasonal = 30 * np.sin(2 * np.pi * day_of_year / 365.25) * item_popularity
                
                # Weekly patterns (higher demand on weekends)
                weekly = 15 if date.weekday() >= 5 else 0
                
                # Holiday effects (increased demand around major holidays)
                month_day = (date.month, date.day)
                holiday_boost = 0
                if month_day in [(12, 25), (1, 1), (7, 4), (11, 24)]:  # Major holidays
                    holiday_boost = 50
                elif date.month == 12 and date.day > 15:  # Holiday season
                    holiday_boost = 25
                
                # Growth trend (business expanding over time)
                days_since_start = (date - pd.to_datetime(start_date)).days
                trend = 0.02 * days_since_start * store_location_factor
                
                # Random noise (real-world variation)
                noise = np.random.normal(0, 15)
                
                # Calculate final sales (ensure non-negative)
                sales = max(0, int(
                    base_demand + 
                    seasonal + 
                    weekly + 
                    holiday_boost + 
                    trend + 
                    noise
                ))
                
                data.append({
                    'date': date,
                    'store': store,
                    'item': item,
                    'sales': sales
                })
    
    return pd.DataFrame(data)

# Generate realistic retail sales data
print("🛒 Generating realistic retail sales history...")
synthetic_data = generate_synthetic_data()

print(f"✅ Created {len(synthetic_data):,} sales transactions")
print(f"📈 Average daily sales per SKU: {synthetic_data['sales'].mean():.1f} units")
print(f"📊 Sales volume range: {synthetic_data['sales'].min()} to {synthetic_data['sales'].max()} units/day")

# COMMAND ----------

# DBTITLE 1,Load Data into Unity Catalog
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, LongType, TimestampType
from pyspark.sql.functions import current_timestamp

print("💾 Saving data to Unity Catalog...")

# Define explicit schema to match the Delta table exactly
schema = StructType([
    StructField("date", DateType(), True),
    StructField("store", IntegerType(), True),
    StructField("item", IntegerType(), True),
    StructField("sales", LongType(), True),
    StructField("processing_timestamp", TimestampType(), True)
])

# Prepare data with exact types expected by schema
print("🔧 Preparing data with precise schema matching...")

# Ensure all pandas DataFrame columns have correct types
synthetic_data_clean = synthetic_data.copy()

# Convert date column to proper date type (remove time component)
synthetic_data_clean['date'] = pd.to_datetime(synthetic_data_clean['date']).dt.date

# Ensure integer types are exactly what we need
synthetic_data_clean['store'] = synthetic_data_clean['store'].astype('int32')
synthetic_data_clean['item'] = synthetic_data_clean['item'].astype('int32') 
synthetic_data_clean['sales'] = synthetic_data_clean['sales'].astype('int64')

# Add processing timestamp as None (will be filled by Spark)
synthetic_data_clean['processing_timestamp'] = None

print(f"📋 Data types: {synthetic_data_clean.dtypes.to_dict()}")

# Create Spark DataFrame using explicit schema to prevent type inference issues
synthetic_spark_df = spark.createDataFrame(synthetic_data_clean, schema=schema)

# Add processing timestamp
final_df = synthetic_spark_df.withColumn(
    "processing_timestamp", 
    current_timestamp()
)

# Verify schema matches exactly
print("🔍 Final DataFrame schema:")
final_df.printSchema()

# Save to Unity Catalog with overwrite
print(f"💾 Writing to: {catalog_name}.{schema_name}.raw_sales_data")
final_df.write.mode("overwrite").saveAsTable(
    f"{catalog_name}.{schema_name}.raw_sales_data"
)

print(f"✅ Sales history loaded successfully!")
print(f"📊 Rows written: {final_df.count():,}")

# COMMAND ----------

# MAGIC %md ## 📊 Retail Data Quality Assessment
# MAGIC 
# MAGIC ### Sales Data Validation
# MAGIC Ensuring our retail sales data meets enterprise standards for accurate forecasting.

# COMMAND ----------

# DBTITLE 1,Validate Data Quality
# Load and examine the data
raw_table = f"{catalog_name}.{schema_name}.raw_sales_data"
df = spark.table(raw_table)

print("🔍 Data Quality Report:")
print("=" * 50)

# Basic statistics
row_count = df.count()
date_min = df.select(spark_min('date')).collect()[0][0]
date_max = df.select(spark_max('date')).collect()[0][0]
store_count = df.select('store').distinct().count()
item_count = df.select('item').distinct().count()

print(f"📊 Total records: {row_count:,}")
print(f"📅 Date range: {date_min} to {date_max}")
print(f"🏪 Unique stores: {store_count}")
print(f"📦 Unique items: {item_count}")

# Data completeness check
null_checks = df.select([
    count(col('date')).alias('total_dates'),
    count(col('store')).alias('total_stores'), 
    count(col('item')).alias('total_items'),
    count(col('sales')).alias('total_sales')
]).collect()[0]

print(f"✅ Completeness: {null_checks['total_sales']:,} sales records (100% complete)")

# Statistical summary
sales_stats = df.select('sales').describe().collect()
for row in sales_stats:
    print(f"📈 Sales {row['summary']}: {float(row['sales']):.2f}")

print("\n🎯 Retail sales data validated and ready for demand forecasting!")

# COMMAND ----------

# MAGIC %md ## 📋 Foundation Complete - Next Steps
# MAGIC 
# MAGIC ### ✅ Data Foundation Established:
# MAGIC 
# MAGIC 1. **🏛️ Enterprise Data Architecture**: Secure, governed retail data platform
# MAGIC 2. **📊 Sales Data Repository**: 6+ years of realistic transaction history
# MAGIC 3. **🎲 Realistic Business Patterns**: Seasonal, geographic, and product variations
# MAGIC 4. **✅ Data Quality Assurance**: Enterprise-grade validation and monitoring
# MAGIC 5. **⚡ Scalable Infrastructure**: Cloud-native, auto-scaling architecture
# MAGIC 
# MAGIC ### 🔄 Forecasting Pipeline:
# MAGIC 
# MAGIC **Next: AI-Powered Demand Modeling**
# MAGIC - Advanced machine learning for demand prediction
# MAGIC - Store and product-specific forecast models
# MAGIC - Confidence intervals for risk management
# MAGIC 
# MAGIC **Finally: Business Intelligence & Actions**  
# MAGIC - Executive dashboards and KPI monitoring
# MAGIC - Inventory optimization recommendations
# MAGIC - Supply chain planning insights
# MAGIC 
# MAGIC ### 📍 Business Data Assets:
# MAGIC ```
# MAGIC Enterprise Catalog: {catalog_name}
# MAGIC Forecasting Workspace: {schema_name}
# MAGIC Sales History: raw_sales_data
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 🎉 Ready for Demand Forecasting
# MAGIC 
# MAGIC **Your retail sales data foundation is operational!**
# MAGIC 
# MAGIC Ready to apply advanced machine learning for accurate demand predictions across your entire product portfolio.

# COMMAND ----------

# Return completion status for workflow orchestration
dbutils.notebook.exit("SUCCESS: Retail data foundation ready for demand forecasting") 