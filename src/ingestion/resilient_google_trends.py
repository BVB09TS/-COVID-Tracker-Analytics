# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, current_timestamp, lit
import random
from datetime import datetime, timedelta
import json

class ResilientGoogleTrendsFetcher:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("GoogleTrendsIngestion") \
            .master("spark://ad2dd992072d:7077") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .getOrCreate()
        self.hdfs_base = "hdfs://namenode:9000"
        self.raw_path = self.hdfs_base + "/healthcare/raw/covid_trends"
        self.checkpoint_path = self.hdfs_base + "/healthcare/checkpoints/covid_trends"
        self.metadata_path = self.hdfs_base + "/healthcare/metadata/covid_trends"
        
    def load_checkpoint(self):
        """Load last successful checkpoint to resume from interruption"""
        try:
            checkpoint_file = self.metadata_path + "/checkpoint.json"
            # Try to read checkpoint from HDFS
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(checkpoint_file)
            
            if fs.exists(path):
                input_stream = fs.open(path)
                content = input_stream.readLine()
                input_stream.close()
                checkpoint = json.loads(content)
                print("CHECKPOINT FOUND: Resuming from " + str(checkpoint.get('last_date', 'start')))
                return checkpoint
        except Exception as e:
            print("NO CHECKPOINT: Starting fresh ingestion - " + str(e))
        
        return {'last_date': None, 'last_batch_id': 0, 'total_records': 0}
    
    def save_checkpoint(self, checkpoint_data):
        """Save checkpoint for recovery"""
        try:
            checkpoint_file = self.metadata_path + "/checkpoint.json"
            checkpoint_str = json.dumps(checkpoint_data)
            
            # Save to HDFS
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(checkpoint_file)
            
            # Create parent directories
            fs.mkdirs(path.getParent())
            
            # Write checkpoint
            output = fs.create(path, True)
            output.writeBytes(checkpoint_str)
            output.close()
            
            print("CHECKPOINT SAVED: batch_id=" + str(checkpoint_data['last_batch_id']))
        except Exception as e:
            print("WARNING: Could not save checkpoint - " + str(e))
    
    def create_batch_metadata(self, batch_id, record_count, start_date, end_date):
        """Create metadata for batch tracking"""
        return {
            'batch_id': batch_id,
            'ingestion_timestamp': datetime.now().isoformat(),
            'record_count': record_count,
            'start_date': start_date,
            'end_date': end_date,
            'source': 'google_trends_simulator',
            'status': 'success'
        }
    
    def create_covid_trends_batch(self, start_week, num_weeks, batch_id):
        """Create a batch of COVID trends data with retry logic"""
        print("CREATING batch " + str(batch_id) + ": weeks " + str(start_week) + " to " + str(start_week + num_weeks - 1))
        
        search_terms = [
            'COVID symptoms', 'fever', 'cough', 'shortness of breath', 
            'loss of taste', 'COVID test', 'rapid test', 'vaccine near me',
            'paxlovid', 'ICU beds', 'hospital capacity', 'ventilator',
            'mask', 'booster shot', 'telehealth', 'emergency room'
        ]
        
        trends_data = []
        base_date = datetime(2024, 1, 1)
        
        try:
            for week_offset in range(start_week, start_week + num_weeks):
                current_date = base_date + timedelta(days=week_offset * 7)
                
                for term in search_terms:
                    # Simulate volume with realistic patterns
                    base_value = 30
                    if 'symptom' in term.lower():
                        base_value = 45
                    elif 'test' in term.lower():
                        base_value = 55
                    elif 'hospital' in term.lower() or 'ICU' in term:
                        base_value = 40
                    
                    noise = random.randint(-10, 10)
                    search_volume = max(10, base_value + noise)
                    
                    trends_data.append((
                        current_date.strftime('%Y-%m-%d'),
                        term,
                        int(search_volume),
                        week_offset + 1,
                        current_date.month,
                        current_date.year,
                        batch_id,
                        datetime.now().isoformat()
                    ))
            
            print("GENERATED " + str(len(trends_data)) + " records for batch " + str(batch_id))
            return trends_data
            
        except Exception as e:
            print("ERROR creating batch " + str(batch_id) + ": " + str(e))
            raise
    
    def save_raw_batch_to_hdfs(self, trends_data, batch_id):
        """Save raw data batch to HDFS with partitioning"""
        print("SAVING batch " + str(batch_id) + " to HDFS...")
        
        schema = StructType([
            StructField("date", StringType(), True),
            StructField("search_term", StringType(), True),
            StructField("search_volume", IntegerType(), True),
            StructField("week", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("year", IntegerType(), True),
            StructField("batch_id", IntegerType(), True),
            StructField("ingestion_timestamp", StringType(), True)
        ])
        
        try:
            rdd = self.spark.sparkContext.parallelize(trends_data)
            spark_df = self.spark.createDataFrame(rdd, schema)
            
            # Add processing metadata
            spark_df = spark_df.withColumn("processing_date", current_timestamp())
            
            # Save with partitioning by year and month for efficient querying
            batch_path = self.raw_path + "/batch_" + str(batch_id)
            spark_df.write \
                .mode("overwrite") \
                .partitionBy("year", "month") \
                .parquet(batch_path)
            
            print("SUCCESS: Batch " + str(batch_id) + " saved to " + batch_path)
            
            # Save batch metadata
            start_date = min(row[0] for row in trends_data)
            end_date = max(row[0] for row in trends_data)
            metadata = self.create_batch_metadata(
                batch_id, 
                len(trends_data), 
                start_date, 
                end_date
            )
            
            self.save_batch_metadata(metadata)
            
            return True
            
        except Exception as e:
            print("ERROR saving batch " + str(batch_id) + ": " + str(e))
            return False
    
    def save_batch_metadata(self, metadata):
        """Save batch metadata for tracking"""
        try:
            metadata_file = self.metadata_path + "/batch_" + str(metadata['batch_id']) + ".json"
            metadata_str = json.dumps(metadata, indent=2)
            
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jsc.hadoopConfiguration()
            )
            path = self.spark._jvm.org.apache.hadoop.fs.Path(metadata_file)
            fs.mkdirs(path.getParent())
            
            output = fs.create(path, True)
            output.writeBytes(metadata_str)
            output.close()
            
            print("METADATA saved for batch " + str(metadata['batch_id']))
        except Exception as e:
            print("WARNING: Could not save metadata - " + str(e))
    
    def run_batch_ingestion(self, total_weeks=52, batch_size=4, max_retries=3):
        """Run batch ingestion with resilience"""
        print("STARTING RESILIENT Google Trends Batch Ingestion")
        print("=" * 60)
        print("Configuration: " + str(total_weeks) + " weeks, batch size=" + str(batch_size))
        print("=" * 60)
        
        # Load checkpoint
        checkpoint = self.load_checkpoint()
        start_batch_id = checkpoint['last_batch_id']
        total_records = checkpoint['total_records']
        
        num_batches = (total_weeks + batch_size - 1) // batch_size
        
        for batch_num in range(start_batch_id, num_batches):
            batch_id = batch_num + 1
            start_week = batch_num * batch_size
            weeks_in_batch = min(batch_size, total_weeks - start_week)
            
            print("\n--- PROCESSING BATCH " + str(batch_id) + "/" + str(num_batches) + " ---")
            
            retry_count = 0
            success = False
            
            while retry_count < max_retries and not success:
                try:
                    # Create batch data
                    trends_data = self.create_covid_trends_batch(
                        start_week, 
                        weeks_in_batch, 
                        batch_id
                    )
                    
                    # Save to HDFS
                    success = self.save_raw_batch_to_hdfs(trends_data, batch_id)
                    
                    if success:
                        # Update checkpoint
                        total_records += len(trends_data)
                        last_date = max(row[0] for row in trends_data)
                        
                        checkpoint = {
                            'last_date': last_date,
                            'last_batch_id': batch_id,
                            'total_records': total_records,
                            'last_update': datetime.now().isoformat()
                        }
                        self.save_checkpoint(checkpoint)
                        
                        print("BATCH " + str(batch_id) + " completed successfully")
                        print("Total records ingested: " + str(total_records))
                    
                except Exception as e:
                    retry_count += 1
                    print("RETRY " + str(retry_count) + "/" + str(max_retries) + " for batch " + str(batch_id) + ": " + str(e))
                    
                    if retry_count >= max_retries:
                        print("FAILED: Batch " + str(batch_id) + " after " + str(max_retries) + " retries")
                        print("Pipeline can be resumed from this point")
                        return False
        
        print("\n" + "=" * 60)
        print("COMPLETED: All batches ingested successfully!")
        print("Total records: " + str(total_records))
        print("Total batches: " + str(num_batches))
        print("=" * 60)
        
        return True
    
    def verify_ingestion(self):
        """Verify ingested data"""
        print("\n--- VERIFYING INGESTION ---")
        
        try:
            # Read all batches
            df = self.spark.read.parquet(self.raw_path + "/batch_*")
            
            print("Total records in HDFS: " + str(df.count()))
            print("Date range: " + str(df.agg({'date': 'min'}).collect()[0][0]) + " to " + str(df.agg({'date': 'max'}).collect()[0][0]))
            print("Unique search terms: " + str(df.select('search_term').distinct().count()))
            print("Batches: " + str(df.select('batch_id').distinct().count()))
            
            print("\nSample data:")
            df.select("date", "search_term", "search_volume", "batch_id").show(10)
            
            return True
        except Exception as e:
            print("VERIFICATION FAILED: " + str(e))
            return False
    
    def run(self):
        """Main execution"""
        try:
            success = self.run_batch_ingestion(
                total_weeks=52,  # Full year
                batch_size=4,    # 4 weeks per batch
                max_retries=3
            )
            
            if success:
                self.verify_ingestion()
            
        except Exception as e:
            print("PIPELINE ERROR: " + str(e))
            import traceback
            print(traceback.format_exc())
        
        finally:
            self.spark.stop()

if __name__ == "__main__":
    fetcher = ResilientGoogleTrendsFetcher()
    fetcher.run()