# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, md5, concat_ws
from pyspark.sql.types import *
from datetime import datetime

class SimpleETLPersistence:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("SimpleETLPersistence") \
            .master("spark://ad2dd992072d:7077") \
            .getOrCreate()
        
        self.hdfs_base = "hdfs://namenode:9000/healthcare"
        self.raw_path = self.hdfs_base + "/raw"
        self.clean_path = self.hdfs_base + "/clean"
        self.metadata_path = self.hdfs_base + "/metadata"
    
    def create_data_id(self, df, id_columns):
        """Create unique ID for records"""
        return df.withColumn("data_id", md5(concat_ws("_", *[col(c) for c in id_columns])))
    
    # ============================================
    # CENSUS DATA ETL
    # ============================================
    
    def etl_census_data(self):
        """Complete Census ETL: Extract, Transform, Load"""
        print("\n" + "=" * 70)
        print("CENSUS DATA ETL PIPELINE")
        print("=" * 70)
        
        # EXTRACT
        print("\n[1/3] EXTRACTING raw census data...")
        try:
            raw_df = self.spark.read.parquet(self.raw_path + "/acs_demographics/batch_*")
            print("Extracted: " + str(raw_df.count()) + " records")
        except Exception as e:
            print("ERROR: " + str(e))
            return False
        
        # TRANSFORM - Separate DATA from METADATA
        print("\n[2/3] TRANSFORMING census data...")
        
        # CLEAN DATA - Only business columns
        clean_df = raw_df.select(
            col("NAME").alias("state_name"),
            col("state").alias("state_code"),
            col("B01001_001E").alias("total_population"),
            col("B01001_002E").alias("male_population"),
            col("B01001_026E").alias("female_population"),
            col("population_65_plus"),
            col("pct_65_plus").alias("elderly_percentage"),
            col("B19013_001E").alias("median_household_income"),
            col("B17001_002E").alias("poverty_count"),
            col("poverty_rate"),
            col("B25077_001E").alias("median_home_value"),
            col("B08301_010E").alias("public_transit_users"),
            col("public_transit_pct").alias("public_transit_percentage")
        )
        
        # Handle nulls
        clean_df = clean_df.na.fill({
            'elderly_percentage': 0.0,
            'poverty_rate': 0.0,
            'public_transit_percentage': 0.0
        })
        
        # Create unique ID
        clean_df = self.create_data_id(clean_df, ["state_code"])
        
        print("Cleaned: " + str(clean_df.count()) + " records")
        clean_df.show(5, False)
        
        # METADATA - Separate extraction info
        metadata_df = raw_df.select(
            col("state").alias("state_code"),
            col("ingestion_timestamp"),
            col("batch_id"),
            col("data_source")
        )
        
        # Add processing info
        metadata_df = metadata_df.withColumn("processing_timestamp", current_timestamp())
        metadata_df = metadata_df.withColumn("table_name", col("state_code"))
        metadata_df = self.create_data_id(metadata_df, ["state_code"])
        
        print("Metadata: " + str(metadata_df.count()) + " records")
        
        # LOAD - Save separately
        print("\n[3/3] LOADING to HDFS...")
        
        # Save CLEAN DATA
        data_path = self.clean_path + "/census_demographics"
        clean_df.write.mode("overwrite").parquet(data_path)
        print("SUCCESS: Data saved to " + data_path)
        
        # Save METADATA (separate location)
        meta_path = self.metadata_path + "/census_demographics_meta"
        metadata_df.write.mode("overwrite").parquet(meta_path)
        print("SUCCESS: Metadata saved to " + meta_path)
        
        print("\nCENSUS ETL COMPLETED!")
        return True
    
    # ============================================
    # COVID TRENDS ETL
    # ============================================
    
    def etl_covid_trends(self):
        """Complete COVID Trends ETL: Extract, Transform, Load"""
        print("\n" + "=" * 70)
        print("COVID TRENDS ETL PIPELINE")
        print("=" * 70)
        
        # EXTRACT
        print("\n[1/3] EXTRACTING raw trends data...")
        try:
            # Simple approach: read each batch individually and union
            print("Reading batches individually...")
            dfs = []
            
            # Try to read up to 20 batches (more than we have)
            for batch_num in range(1, 21):
                try:
                    batch_path = self.raw_path + "/covid_trends/batch_" + str(batch_num)
                    df = self.spark.read.parquet(batch_path)
                    print("Batch " + str(batch_num) + ": " + str(df.count()) + " records")
                    dfs.append(df)
                except:
                    # No more batches
                    break
            
            if len(dfs) == 0:
                print("ERROR: No batch data found")
                return False
            
            print("Found " + str(len(dfs)) + " batches")
            
            # Union all batches
            raw_df = dfs[0]
            for df in dfs[1:]:
                raw_df = raw_df.union(df)
            
            print("Extracted: " + str(raw_df.count()) + " records total")
        except Exception as e:
            print("ERROR: " + str(e))
            import traceback
            print(traceback.format_exc())
            return False
        
        # TRANSFORM - Separate DATA from METADATA
        print("\n[2/3] TRANSFORMING trends data...")
        
        # CLEAN DATA - Only business columns
        clean_df = raw_df.select(
            col("date").alias("search_date"),
            col("search_term"),
            col("search_volume"),
            col("week").alias("week_number"),
            col("month"),
            col("year")
        )
        
        # Data quality
        clean_df = clean_df.filter(col("search_volume") > 0)
        
        # Create unique ID
        clean_df = self.create_data_id(clean_df, ["search_date", "search_term"])
        
        print("Cleaned: " + str(clean_df.count()) + " records")
        clean_df.show(5, False)
        
        # METADATA - Separate extraction info
        metadata_df = raw_df.select(
            col("date").alias("search_date"),
            col("search_term"),
            col("ingestion_timestamp"),
            col("batch_id")
        )
        
        # Add processing info
        metadata_df = metadata_df.withColumn("processing_timestamp", current_timestamp())
        metadata_df = metadata_df.withColumn("table_name", col("search_date"))
        metadata_df = self.create_data_id(metadata_df, ["search_date", "search_term"])
        
        print("Metadata: " + str(metadata_df.count()) + " records")
        
        # LOAD - Save separately with partitioning
        print("\n[3/3] LOADING to HDFS...")
        
        # Save CLEAN DATA (partitioned for fast queries)
        data_path = self.clean_path + "/covid_search_trends"
        clean_df.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(data_path)
        print("SUCCESS: Data saved to " + data_path)
        
        # Save METADATA (separate location, partitioned)
        meta_path = self.metadata_path + "/covid_trends_meta"
        metadata_df.write \
            .mode("overwrite") \
            .partitionBy("batch_id") \
            .parquet(meta_path)
        print("SUCCESS: Metadata saved to " + meta_path)
        
        print("\nTRENDS ETL COMPLETED!")
        return True
    
    # ============================================
    # VERIFICATION
    # ============================================
    
    def verify_data(self):
        """Verify clean data and metadata are properly separated"""
        print("\n" + "=" * 70)
        print("VERIFYING DATA SEPARATION")
        print("=" * 70)
        
        # Check Census
        print("\n--- CENSUS DATA ---")
        try:
            census_data = self.spark.read.parquet(self.clean_path + "/census_demographics")
            print("Data records: " + str(census_data.count()))
            print("Columns: " + str(census_data.columns))
            census_data.show(3, False)
            
            census_meta = self.spark.read.parquet(self.metadata_path + "/census_demographics_meta")
            print("\nMetadata records: " + str(census_meta.count()))
            print("Columns: " + str(census_meta.columns))
            census_meta.show(3, False)
        except Exception as e:
            print("ERROR: " + str(e))
        
        # Check Trends
        print("\n--- COVID TRENDS DATA ---")
        try:
            trends_data = self.spark.read.parquet(self.clean_path + "/covid_search_trends")
            print("Data records: " + str(trends_data.count()))
            print("Columns: " + str(trends_data.columns))
            trends_data.show(3, False)
            
            trends_meta = self.spark.read.parquet(self.metadata_path + "/covid_trends_meta")
            print("\nMetadata records: " + str(trends_meta.count()))
            print("Columns: " + str(trends_meta.columns))
            trends_meta.show(3, False)
        except Exception as e:
            print("ERROR: " + str(e))
        
        print("\n" + "=" * 70)
    
    # ============================================
    # MAIN PIPELINE
    # ============================================
    
    def run_all(self):
        """Run complete ETL pipeline"""
        # FIX: Added quotes around "\n"
        print("\n" + "#" * 70)
        print("# STARTING SIMPLE ETL PIPELINE (HDFS ONLY)")
        print("#" * 70)
        
        try:
            # Run Census ETL
            census_ok = self.etl_census_data()
            
            # Run Trends ETL
            trends_ok = self.etl_covid_trends()
            
            # Verify results
            if census_ok and trends_ok:
                self.verify_data()
            
            # Summary
            # FIX: Added quotes around "\n"
            print("\n" + "#" * 70)
            print("# ETL SUMMARY")
            print("#" * 70)
            print("Census ETL: " + ("SUCCESS" if census_ok else "FAILED"))
            print("Trends ETL: " + ("SUCCESS" if trends_ok else "FAILED"))
            print("#" * 70)
            
            # FIX: Added quotes around "\n"
            print("\nDATA STRUCTURE:")
            print("  Clean Data:  /healthcare/clean/")
            print("    - census_demographics/")
            print("    - covid_search_trends/")
            print("\n  Metadata:    /healthcare/metadata/")
            print("    - census_demographics_meta/")
            print("    - covid_trends_meta/")
            
        except Exception as e:
            print("ERROR: " + str(e))
            import traceback
            print(traceback.format_exc())
        
        finally:
            self.spark.stop()

if __name__ == "__main__":
    etl = SimpleETLPersistence()
    etl.run_all()