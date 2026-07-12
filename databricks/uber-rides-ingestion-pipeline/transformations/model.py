from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

#_____________________ DIM PASSENGER _____________________
@dp.view
def dim_passenger_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("passenger_id", "passenger_name", "passenger_email", "passenger_phone", "booking_timestamp")
    df = df.dropDuplicates(subset=["passenger_id", "booking_timestamp"])
    return df

dp.create_streaming_table("dim_passenger")

dp.create_auto_cdc_flow(
    target="dim_passenger",
    source="dim_passenger_view",
    keys=["passenger_id"],
    sequence_by="booking_timestamp",
    stored_as_scd_type=2,
)

#_____________________ DIM DRIVER _____________________
@dp.view
def dim_driver_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("driver_id", "driver_name", "driver_rating", "driver_phone", "driver_license", "booking_timestamp")
    df = df.dropDuplicates(subset=["driver_id", "booking_timestamp"])
    return df

dp.create_streaming_table("dim_driver")

dp.create_auto_cdc_flow(
    target="dim_driver",
    source="dim_driver_view",
    keys=["driver_id"],
    sequence_by="booking_timestamp",
    stored_as_scd_type=2,
)

#_____________________ DIM VEHICLE _____________________
@dp.view
def dim_vehicle_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("vehicle_id", "vehicle_make_id", "vehicle_type_id", "vehicle_model", "vehicle_color", "license_plate", "vehicle_make", "vehicle_type", "booking_timestamp")
    df = df.dropDuplicates(subset=["vehicle_id", "booking_timestamp"])
    return df

dp.create_streaming_table("dim_vehicle")

dp.create_auto_cdc_flow(
    target="dim_vehicle",
    source="dim_vehicle_view",
    keys=["vehicle_id"],
    sequence_by="booking_timestamp",
    stored_as_scd_type=1,
)

#_____________________ DIM PAYMENT _____________________
@dp.view
def dim_payment_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("payment_method_id", "payment_method", "is_card", "requires_auth", "booking_timestamp")
    df = df.dropDuplicates(subset=["payment_method_id", "booking_timestamp"])
    return df

dp.create_streaming_table("dim_payment")

dp.create_auto_cdc_flow(
    target="dim_payment",
    source="dim_payment_view",
    keys=["payment_method_id"],
    sequence_by="booking_timestamp",
    stored_as_scd_type=1,
)

#_____________________ DIM BOOKING _____________________
@dp.view
def dim_booking_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("ride_id", "confirmation_number", "dropoff_location_id", "ride_status_id", "dropoff_city_id", "cancellation_reason_id", "dropoff_address", "dropoff_latitude", "dropoff_longitude", "booking_timestamp", "dropoff_timestamp", "pickup_address", "pickup_latitude", "pickup_longitude", "pickup_location_id")
    df = df.dropDuplicates(subset=["ride_id", "booking_timestamp"])
    return df

dp.create_streaming_table("dim_booking")

dp.create_auto_cdc_flow(
    target="dim_booking",
    source="dim_booking_view",
    keys=["ride_id"],
    sequence_by="booking_timestamp",
    stored_as_scd_type=1,
)

#_____________________ DIM LOCATION _____________________
@dp.view
def dim_location_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("pickup_city_id", "pickup_city", "city_updated_at", "region", "state")
    df = df.dropDuplicates(subset=['pickup_city_id', 'city_updated_at'])
    return df

dp.create_streaming_table("dim_location")

dp.create_auto_cdc_flow(
  target = "dim_location",
  source = "dim_location_view",
  keys = ["pickup_city_id"],
  sequence_by = "city_updated_at",
  stored_as_scd_type = 2,
)

#_____________________ FACT TABLE _____________________
@dp.view
def fact_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("ride_id", "pickup_city_id", "payment_method_id", "driver_id", "passenger_id", "vehicle_id", "distance_miles", "duration_minutes", "base_fare", "distance_fare", "time_fare", "surge_multiplier", "total_fare", "tip_amount", "rating", "base_rate", "per_mile", "per_minute", "booking_timestamp")
    return df

dp.create_streaming_table("fact")

dp.create_auto_cdc_flow(
  target = "fact",
  source = "fact_view",
  keys = ["ride_id", "pickup_city_id", "payment_method_id", "driver_id", "passenger_id", "vehicle_id"],
  sequence_by = "booking_timestamp",
  stored_as_scd_type = 1,
)

#_____________________ GOLD HOURLY CITY METRICS (STREAMING AGGREGATE) _____________________
@dp.table(name="gold_hourly_city_metrics")
def gold_hourly_city_metrics():
    df = spark.readStream.table("silver_obt")
    
    # Define 10-minute watermark on booking_timestamp for late-arriving events
    # Perform 1-hour tumbling window aggregation per city
    return (
        df.withWatermark("booking_timestamp", "10 minutes")
          .groupBy(
              window(col("booking_timestamp"), "1 hour"),
              col("pickup_city")
          )
          .agg(
              count("ride_id").alias("total_rides"),
              avg("total_fare").alias("avg_fare"),
              avg("surge_multiplier").alias("avg_surge"),
              sum(when(col("ride_status") == "Completed", 1).otherwise(0)).alias("completed_rides"),
              sum(when(col("ride_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_rides")
          )
          .select(
              col("window.start").alias("window_start"),
              col("window.end").alias("window_end"),
              col("pickup_city"),
              col("total_rides"),
              round(col("avg_fare"), 2).alias("avg_fare"),
              round(col("avg_surge"), 2).alias("avg_surge"),
              col("completed_rides"),
              col("cancelled_rides")
          )
    )
