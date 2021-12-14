import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object hdfsApp extends App {

    def temperatureTable (spark: SparkSession, inputPath: String, hdfsOutDir: String ) : Unit ={

        var df: DataFrame = spark.read.text(inputPath)
        df=df.withColumn("value", regexp_replace(col("value"), " +", " "))
        df=df.withColumn("value", trim(col("value")))
        df=df.select(
            split(col("value")," ").getItem(0).cast("int").as("year"),
            split(col("value")," ").getItem(1).cast("int").as("month"),
            split(col("value")," ").getItem(2).cast("int").as("day"),
            split(col("value")," ").getItem(3).cast("decimal(6,2)").as("morning_temp"),
            split(col("value")," ").getItem(4).cast("decimal(6,2)").as("noon_temp"),
            split(col("value")," ").getItem(5).cast("decimal(6,2)").as("evning_temp"),
            split(col("value")," ").getItem(6).cast("decimal(6,2)").as("tmin"),
            split(col("value")," ").getItem(7).cast("decimal(6,2)").as("tmax"),
            split(col("value")," ").getItem(8).cast("decimal(6,2)").as("estimated_diurnal_mean")
            ).drop("value")
        
        df
            .write
            .partitionBy("year")
            .mode("append")
            .parquet(hdfsOutDir)
    }

    def barometerTable (spark: SparkSession, inputPath: String, hdfsOutDir: String, p_unit: String ) : Unit ={
        var df = spark.read.text(inputPath)
        df = df.withColumn("value", regexp_replace(col("value"), " +", " "))
        df = df.withColumn("value", trim(col("value")))
        var converter : Double = 1

        if(p_unit == "swedish_inch") {
            converter = 1.3332*29.69

            df = df.select(
            split(col("value")," ").getItem(0).cast("int").as("year"),
            split(col("value")," ").getItem(1).cast("int").as("month"),
            split(col("value")," ").getItem(2).cast("int").as("day"),
            split(col("value")," ").getItem(3).cast("decimal(6,2)").as("morning_pressure"),
            split(col("value")," ").getItem(5).cast("decimal(6,2)").as("noon_pressure"),
            split(col("value")," ").getItem(7).cast("decimal(6,2)").as("evning_pressure")
            ).drop("value")

        } else if (p_unit == "swedish_inch*0.1") {
            converter = 1.3332*29.69*0.1

            df = df.select(
            split(col("value")," ").getItem(0).cast("int").as("year"),
            split(col("value")," ").getItem(1).cast("int").as("month"),
            split(col("value")," ").getItem(2).cast("int").as("day"),
            split(col("value")," ").getItem(3).cast("decimal(6,2)").as("morning_pressure"),
            split(col("value")," ").getItem(6).cast("decimal(6,2)").as("noon_pressure"),
            split(col("value")," ").getItem(9).cast("decimal(6,2)").as("evning_pressure")
            ).drop("value")

        } else {
            if (p_unit == "mm/hg") converter = 1.3332

            df = df.select(
                split(col("value")," ").getItem(0).cast("int").as("year"),
                split(col("value")," ").getItem(1).cast("int").as("month"),
                split(col("value")," ").getItem(2).cast("int").as("day"),
                split(col("value")," ").getItem(3).cast("decimal(6,2)").as("morning_pressure"),
                split(col("value")," ").getItem(4).cast("decimal(6,2)").as("noon_pressure"),
                split(col("value")," ").getItem(5).cast("decimal(6,2)").as("evning_pressure")
            ).drop("value")
        }        
        df = df.withColumn("morning_pressure", col("morning_pressure")*converter)
        df = df.withColumn("morning_pressure", col("morning_pressure").cast("decimal(6,2)"))
        df = df.withColumn("noon_pressure", col("noon_pressure")*converter)
        df = df.withColumn("noon_pressure", col("noon_pressure").cast("decimal(6,2)"))
        df = df.withColumn("evning_pressure", col("evning_pressure")*converter)
        df = df.withColumn("evning_pressure", col("evning_pressure").cast("decimal(6,2)"))

        df
            .write
            .partitionBy("year")
            .mode("append")
            .parquet(hdfsOutDir)
    }

    override def main(args: Array[String]) {
        val spark = SparkSession.builder.master("local").appName("Simple Application").getOrCreate()
        temperatureTable(spark, "file:///Users/Ebba/Documents/ica/data/temp/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt", "file:///Users/Ebba/Documents/ica/data/temp_base")
        spark.stop()
    }

}






