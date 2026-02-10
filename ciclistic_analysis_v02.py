import pandas as pd
import time

# ============================================================
# 1. TRANSFORM EXCEL SHEETS TO A SINGLE CSV FILE (OPTIONAL STEP)
# ============================================================

# File paths
file_path = "ciclistic_clean_data_v03.xlsx"
output_csv = "ciclistic_clean_data.csv"

# Data types for reading the Excel sheets
dtype_map = {
    "ride_id": "string",
    "rideable_type": "category",
    "start_station_name": "string",
    "start_station_id": "string",
    "end_station_name": "string",
    "end_station_id": "string",
    "member_casual": "category",

    "start_lat": "float32",
    "start_lng": "float32",
    "end_lat": "float32",
    "end_lng": "float32",

    "distance_stations": "float32",
    "day_of_week": "int8",
}

# Columns to parse as datetime
date_cols = ["started_at", "ended_at"]

# -----------------------------------------------------------------
# NOTE: This block converts all Excel sheets into a single CSV file.
# It is kept here for reference but does not run by default.
# -----------------------------------------------------------------
"""
xls = pd.ExcelFile(file_path, engine="openpyxl")
first_sheet = True

for sheet in xls.sheet_names:
    print(f"Processing: {sheet}")
    start_time = time.time()

    df = pd.read_excel(
        xls,
        sheet_name=sheet,
        dtype=dtype_map,
        parse_dates=date_cols
    )

    df["ride_time_length"] = pd.to_timedelta(df["ride_time_length"])

    df.to_csv(
        output_csv,
        mode="w" if first_sheet else "a",
        index=False,
        header=first_sheet
    )

    first_sheet = False
    del df

    print(f"Execution time: {time.time() - start_time:.2f} seconds")
"""

# ============================================================
# 2. LOAD CLEANED DATA USING PARQUET (FAST & EFFICIENT)
# ============================================================

# If needed, convert CSV → Parquet (much faster to load)
"""
df = pd.read_csv(
    "ciclistic_clean_data.csv",
    dtype=dtype_map,
    parse_dates=date_cols
)

df["ride_time_length"] = pd.to_timedelta(df["ride_time_length"])
df.to_parquet("ciclistic_clean_data.parquet", compression="snappy")
"""

# Load the already-created parquet file
start_time = time.time()
all_trips = pd.read_parquet("ciclistic_clean_data.parquet")
print(f"Loaded Parquet file in {time.time() - start_time:.2f} seconds")

# ============================================================
# 3. INITIAL ANALYSIS & ADDITIONAL TRANSFORMATIONS
# ============================================================

# Create month and day columns
all_trips["month"] = all_trips["started_at"].dt.month
all_trips["day"] = all_trips["started_at"].dt.day

# Recalculate ride_time_length in seconds
all_trips["ride_time_length"] = (
    all_trips["ended_at"] - all_trips["started_at"]
).dt.total_seconds()

# Check for negative durations
print("Any negative ride durations?:", any(all_trips["ride_time_length"] < 0))

# Basic inspection
print("\nColumn names:", all_trips.columns)
print("\nTotal rows:", len(all_trips))
print("\nShape:", all_trips.shape)
print("\nFirst 6 rows:\n", all_trips.head())
print("\nColumn types and memory usage:")
all_trips.info(memory_usage="deep")
print("\nNumeric summary:\n", all_trips.describe())

# ============================================================
# 4. DESCRIPTIVE ANALYSIS
# ============================================================

print("\nValue counts for 'member_casual':\n",
    all_trips["member_casual"].value_counts())

print("\nRide length statistics (seconds):\n",
    all_trips["ride_time_length"].describe())

print("\nDistance statistics (km):\n", 
    all_trips["distance_stations"].describe())

# Compare ride lengths by user type
print("\nRide length by membership type:\n",
    all_trips.groupby("member_casual", observed=True)["ride_time_length"]
    .agg(["mean", "median", "max", "min"]))

# Compare distances by user type
print("\nDistance by membership type:\n",
    all_trips.groupby("member_casual", observed=True)["distance_stations"]
    .agg(["mean", "median", "max", "min"]))

# ============================================================
# 5. ANALYSIS BY DAY OF WEEK
# ============================================================

# Map numbers → day names
num_to_day = {
    1: "Sunday", 2: "Monday", 3: "Tuesday",
    4: "Wednesday", 5: "Thursday",
    6: "Friday", 7: "Saturday"
}

all_trips["day_of_week"] = all_trips["day_of_week"].map(num_to_day)

# Order categories
days_order = ["Sunday", "Monday", "Tuesday", "Wednesday",
    "Thursday", "Friday", "Saturday"]

all_trips["day_of_week"] = pd.Categorical(
    all_trips["day_of_week"],
    categories=days_order,
    ordered=True
)

print("\nAverage ride length by user type and weekday:\n",
    all_trips.groupby(["member_casual", "day_of_week"], observed=True)
    ["ride_time_length"].mean())

# Detailed summary by weekday
weekday_summary = all_trips.groupby(
    ["member_casual", "day_of_week"], observed=True
).agg(
    number_of_rides=("ride_id", "count"),
    number_of_classic_bikes=("rideable_type", lambda x: (x == "classic_bike").sum()),
    average_duration=("ride_time_length", "mean"),
    average_distance=("distance_stations", "mean"),
).reset_index()

weekday_summary["pct_classic_bikes"] = (
    weekday_summary["number_of_classic_bikes"]
    / weekday_summary["number_of_rides"]
    * 100)

print("\nSummary of rides by user type and weekday:\n",
    weekday_summary)

# ============================================================
# 6. ANALYSIS BY MONTH
# ============================================================

monthly_summary = all_trips.groupby(
    ["member_casual", "month"], observed=True
).agg(
    number_of_rides=("ride_id", "count"),
    number_of_classic_bikes=("rideable_type", lambda x: (x == "classic_bike").sum()),
    average_duration=("ride_time_length", "mean"),
    average_distance=("distance_stations", "mean")
).reset_index()

monthly_summary["pct_classic_bikes"] = (
    monthly_summary["number_of_classic_bikes"]
    / monthly_summary["number_of_rides"]
    * 100)

print("\nSummary of rides by user type and month:\n",
    monthly_summary)

# ===========================================================
# 7. EXPORT SUMMARY FILE
# ===========================================================
# Create a .csv file to visualize elsewhere
weekday_summary.to_csv('weekday_summary.csv', index=False)
monthly_summary.to_csv('monthly_summary.csv', index=False)