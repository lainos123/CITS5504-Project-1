import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np
from sqlalchemy import create_engine, text
import os

# Update the database if one already exists - for editing this script after the database has been created
DATABASE_URL = "postgresql://postgres:postgres@pgdb:5432/datawarehouse"
engine = create_engine(DATABASE_URL)

with engine.connect() as connection:
    connection.execute(text("DROP TABLE IF EXISTS fact_number CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS fact_fatalities CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS fact_crashes CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_road CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_event CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_person CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_vehicle CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_time CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_lga CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_state CASCADE;"))
    connection.execute(text("DROP TABLE IF EXISTS dim_date CASCADE;"))
    connection.commit()


# EXTRACT

data_dir = os.path.join("data", "raw")
fatalities_file = os.path.join(data_dir, "bitre_fatalities_dec2024.xlsx")
crashes_file = os.path.join(data_dir, "bitre_fatal_crashes_dec2024.xlsx")
dwellings_file = os.path.join(data_dir, "LGA (count of dwellings).csv")

fatality_df = pd.read_excel(fatalities_file, sheet_name="BITRE_Fatality", skiprows=4)
fatality_count_df = pd.read_excel(fatalities_file, sheet_name="BITRE_Fatality_Count_By_Date", skiprows=2)

crash_df = pd.read_excel(crashes_file, sheet_name="BITRE_Fatal_Crash", skiprows=4)
crash_count_df = pd.read_excel(crashes_file, sheet_name="BITRE_Fatal_Crash_Count_By_Date", skiprows=2)

dwelling_df = pd.read_csv(
    dwellings_file,
    skiprows=7,
    header=None,
    names=["lga_name", "dwelling_count", "extra"],
    usecols=["lga_name", "dwelling_count"]
)

dwelling_df = dwelling_df.iloc[2:-5].reset_index(drop=True)


# TRANSFORM

# merge the crash and fatality dataframes on 'Crash ID'
crashxfatality_df = fatality_df.merge(crash_df, on="Crash ID", how="left")
crashxfatality_df = crashxfatality_df.reset_index(drop=True)

# Clean the column names (remove \n)
cleaned_cols = crashxfatality_df.columns.str.replace("\n", "", regex=False)

# Clean duplicate names
seen = {}
final_cols = []

for idx, col in enumerate(cleaned_cols):
    if col in seen:
        # Compare the new column with the original one
        orig_idx = seen[col]
        col_data_1 = crashxfatality_df.iloc[:, orig_idx]
        col_data_2 = crashxfatality_df.iloc[:, idx]
        
        if col_data_1.equals(col_data_2):
            # If they're the same, mark this one to drop by putting None
            final_cols.append(None)
            print(f"Dropping duplicate identical column: {col}")
        else:
            # If they're different, rename this one (add _dup or _2)
            new_col = f"{col}_dup"
            print(f"Renaming column '{col}' to '{new_col}' due to conflict")
            final_cols.append(new_col)
    else:
        seen[col] = idx
        final_cols.append(col)

crashxfatality_df.columns = final_cols

crashxfatality_df = crashxfatality_df.loc[:, crashxfatality_df.columns.notna()]

# Verify that the columns in crashxfatality_df are clean duplicates
for col in crashxfatality_df.columns:
    if col.endswith('_x'):
        base = col[:-2]
        col_y = base + '_y'
        if col_y in crashxfatality_df.columns:
            diffs = crashxfatality_df[crashxfatality_df[col] != crashxfatality_df[col_y]]
            if not diffs.empty:
                print(f"\nDifferences found in column: {base}")
                print(diffs[['Crash ID', col, col_y]])

# Clean up duplicate columns (accounting for _x and _y)
cols_to_drop = [col for col in crashxfatality_df.columns if col.endswith('_x')]
crashxfatality_df = crashxfatality_df.drop(columns=cols_to_drop)

crashxfatality_df.columns = [
    col[:-2] if col.endswith('_y') else col for col in crashxfatality_df.columns
]

# Drop 'Time of day' column because it is a duplicate of 'Time of Day'
crashxfatality_df = crashxfatality_df.drop(columns=['Time of day'])

# merge and clean up crashxfatality_count_df
crashxfatality_count_df = fatality_count_df.merge(crash_count_df, on="Date", how="left")

crashxfatality_count_df.columns = crashxfatality_count_df.columns.str.replace("_x", "")
crashxfatality_count_df.columns = crashxfatality_count_df.columns.str.replace("_y", "")

crashxfatality_count_df = crashxfatality_count_df.loc[:, ~crashxfatality_count_df.columns.duplicated()]

crashxfatality_count_df = crashxfatality_count_df.reset_index(drop=True)
crashxfatality_df = crashxfatality_df.reset_index(drop=True)

crashxfatality_df['State'] = crashxfatality_df['State'].str.upper()

print("Columns for both dataframes after cleaning:")
print(crashxfatality_df.columns)
print(crashxfatality_count_df.columns)

# export the data
processed_data_dir = os.path.join("data", "processed")

os.makedirs(processed_data_dir, exist_ok=True)

crashxfatality_file = os.path.join(processed_data_dir, "crashxfatality_df.csv")
crashxfatality_count_file = os.path.join(processed_data_dir, "crashxfatality_count_df.csv")

for file in [crashxfatality_file, crashxfatality_count_file]:
    if os.path.exists(file):
        os.remove(file)

crashxfatality_df.to_csv(crashxfatality_file, index=False)
crashxfatality_count_df.to_csv(crashxfatality_count_file, index=False)

# Confirmation of successful export
print(f"Files exported successfully to:")
print(f"- {crashxfatality_file}")
print(f"- {crashxfatality_count_file}")
print("Excel file with both dataframes has been saved as 'crash_fatality_data.xlsx'")

# Print absolute file paths for verification
print("Saving to:", os.path.abspath(crashxfatality_file))
print("Saving to:", os.path.abspath(crashxfatality_count_file))
print("Saving to:", os.path.abspath(crashxfatality_count_file))

# Create Dimension Tables
# --- Dim_Date ---
def create_dim_date(df, date_col):
    df = df.copy()
    df['date_id'] = pd.to_datetime(df[date_col])
    df['year'] = df['date_id'].dt.year
    df['month'] = df['date_id'].dt.month
    df['day'] = df['date_id'].dt.day
    df['day_of_week'] = df['date_id'].dt.day_name()
    df['is_weekend'] = df['day_of_week'].isin(['Saturday', 'Sunday'])
    return df[['date_id', 'year', 'month', 'day', 'day_of_week', 'is_weekend']].drop_duplicates()

dim_date = create_dim_date(crashxfatality_count_df, "Date")

dim_date = dim_date.reset_index(drop=True)

# --- Dim_State ---
dim_state = crashxfatality_df[["State"]].drop_duplicates()
dim_state = dim_state.rename(columns={"State": "state_id"})
# set the state_name as the full name of the state ie NSW = New South Wales
state_mapping = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "SA": "South Australia",
    "WA": "Western Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory"
}
dim_state["state_name"] = dim_state["state_id"].replace(state_mapping)

dim_state = dim_state[["state_id", "state_name"]]

dim_state = dim_state.reset_index(drop=True)

# --- Dim_LGA ---
dim_lga = crashxfatality_df[[
    "National LGA Name 2021", "State", "National Remoteness Areas"
]].drop_duplicates()

# Join with state dimension to get state_id
dim_lga = dim_lga.merge(
    dim_state, 
    left_on="State", 
    right_on="state_id", 
    how="left"
)

dim_lga = dim_lga.rename(columns={
    "National LGA Name 2021": "lga_name",
    "National Remoteness Areas": "national_remoteness_area"
})

try:
    dim_lga = pd.merge(dim_lga, dwelling_df, on="lga_name", how="left")
except NameError:
    dim_lga["dwelling_count"] = np.nan

# Remove all rows with NA in lga_name
dim_lga = dim_lga.dropna(subset=["lga_name"])

dim_lga = dim_lga.drop_duplicates(subset=["lga_name"])
dim_lga = dim_lga.reset_index(drop=True)
dim_lga["lga_id"] = range(0, len(dim_lga))
dim_lga['dwelling_count'] = dim_lga['dwelling_count'].astype("Int64")
dim_lga = dim_lga[["lga_id", "lga_name", "national_remoteness_area", "dwelling_count", "state_id"]]

# --- Dim_Time ---
dim_time = crashxfatality_df[[
    "Crash ID", "Time", "Time of Day"
]].rename(columns={
    "Crash ID": "crash_id",
    "Time": "crash_time",
    "Time of Day": "time_of_day"
}).drop_duplicates()

dim_time = dim_time.reset_index(drop=True)

# --- Dim_Vehicle ---
dim_vehicle = crashxfatality_df[[
    "Crash ID", "Bus Involvement", "Heavy Rigid Truck Involvement", "Articulated Truck Involvement"
]].rename(columns={"Crash ID": "crash_id"}).drop_duplicates()
dim_vehicle = dim_vehicle.replace(-9, np.nan)

dim_vehicle = dim_vehicle.reset_index(drop=True)

# --- Dim_Person ---
dim_person = crashxfatality_df[[
    "Crash ID", "Gender", "Age", "Age Group", "Road User"
]].rename(columns={
    "Crash ID": "crash_id",
    "Age Group": "age_group",
    "Road User": "road_user"
}).drop_duplicates()

# Generate a surrogate key for person_id
dim_person["person_id"] = dim_person.apply(
    lambda row: f"{row['crash_id']}_{str(row['Age'])}_{str(row['Gender'])}_{str(row['road_user'])}".replace(" ", "_"), 
    axis=1
)

dim_person = dim_person.rename(columns={
    "Gender": "gender",
    "Age": "age"
})

dim_person = dim_person[["person_id", "crash_id", "gender", "age", "age_group", "road_user"]]

# --- Dim_Event ---
dim_event = crashxfatality_df[[
    "Crash ID", "Christmas Period", "Easter Period"
]].rename(columns={"Crash ID": "crash_id"}).drop_duplicates()

dim_event = dim_event.reset_index(drop=True)

# --- Dim_Road ---
dim_road = crashxfatality_df[[
    "Crash ID", "Speed Limit", "National Road Type"
]].rename(columns={
    "Crash ID": "crash_id",
    "Speed Limit": "speed_limit",
    "National Road Type": "national_road_type"
}).drop_duplicates()

dim_road["speed_limit"] = pd.to_numeric(dim_road["speed_limit"], errors='coerce')
dim_road["speed_limit"] = dim_road["speed_limit"].astype("Int64")

dim_road["national_road_type"] = dim_road["national_road_type"].replace("Undetermined", pd.NA)
dim_road["speed_limit"] = dim_road["speed_limit"].replace(-9, np.nan)

dim_road = dim_road.reset_index(drop=True)


# Create Fact Tables
# --- Fact_Fatalities ---
if "person_id" not in dim_person.columns:
    dim_person["person_id"] = dim_person.apply(
        lambda row: f"{row['crash_id']}_{str(row['age'])}_{str(row['gender'])}_{str(row['road_user'])}".replace(" ", "_"), 
        axis=1
    )

fact_fatalities = crashxfatality_df[["Crash ID", "Gender", "Age", "Road User"]].copy()
fact_fatalities = fact_fatalities.rename(columns={"Crash ID": "crash_id"})

fact_fatalities["person_id"] = fact_fatalities.apply(
    lambda row: f"{row['crash_id']}_{str(row['Age'])}_{str(row['Gender'])}_{str(row['Road User'])}".replace(" ", "_"), 
    axis=1
)

fact_fatalities["fatality_id"] = range(0, len(fact_fatalities))

fact_fatalities = fact_fatalities[["fatality_id", "person_id", "crash_id"]]

# --- Fact_Crashes ---
fact_crashes = crashxfatality_df[[
    'Crash ID', 'Year', 'Month', 'National LGA Name 2021', 'State'
]].rename(columns={
    'Crash ID': 'crash_id',
    'National LGA Name 2021': 'lga_name',
    'State': 'state_id'
}).drop_duplicates()

fact_crashes['date_str'] = fact_crashes['Year'].astype(str) + '-' + fact_crashes['Month'].astype(str) + '-01'
fact_crashes['date_id'] = pd.to_datetime(fact_crashes['date_str'])
fact_crashes.drop(columns=['date_str', 'Year', 'Month'], inplace=True)

fact_crashes = fact_crashes.merge(
    dim_lga[['lga_id', 'lga_name']], 
    on='lga_name', 
    how='left'
)

fact_crashes['lga_id'] = fact_crashes['lga_id'].astype("Int64")

fact_crashes = fact_crashes[['crash_id', 'date_id', 'lga_id', 'state_id']]

# --- Fact_Number ---
fact_number = crashxfatality_count_df[[
    "Date", "Number Fatalities", "Number of fatal crashes"
]].copy()

fact_number["date_id"] = pd.to_datetime(fact_number["Date"])
fact_number = fact_number.drop(columns=["Date"])

fact_number = fact_number.rename(columns={
    "Number Fatalities": "total_fatalities",
    "Number of fatal crashes": "total_crashes"
})

fact_number["number_date_id"] = range(0, len(fact_number))

fact_number = fact_number[["number_date_id", "date_id", "total_fatalities", "total_crashes"]]


# LOAD
DATABASE_URL = "postgresql://postgres:postgres@pgdb:5432/datawarehouse"

# Create the SQLAlchemy engine
engine = create_engine(DATABASE_URL)

print("Connected to PostgreSQL database at", DATABASE_URL)

tables = {
    "dim_date": dim_date,
    "dim_state": dim_state,
    "dim_lga": dim_lga,
    "dim_time": dim_time,
    "dim_vehicle": dim_vehicle,
    "dim_person": dim_person,
    "dim_event": dim_event,
    "dim_road": dim_road,
    "fact_fatalities": fact_fatalities,
    "fact_crashes": fact_crashes,
    "fact_number": fact_number
}

# Iterate over the dictionary and load data into each table
for table_name, df in tables.items():
    # Load the dataframe into PostgreSQL
    df.to_sql(table_name, engine, index=False, if_exists='replace')  # 'replace' ensures that the table is replaced if it exists
    print(f"Table {table_name} has been loaded into the database.")

# Add primary and foreign key constraints
with engine.connect() as connection:
    connection.execute(text("ALTER TABLE dim_date ADD PRIMARY KEY (date_id);"))
    connection.execute(text("ALTER TABLE dim_state ADD PRIMARY KEY (state_id);"))
    connection.execute(text("ALTER TABLE dim_lga ADD PRIMARY KEY (lga_id);"))
    connection.execute(text("ALTER TABLE dim_time ADD PRIMARY KEY (crash_id);"))
    connection.execute(text("ALTER TABLE dim_vehicle ADD PRIMARY KEY (crash_id);"))
    connection.execute(text("ALTER TABLE dim_person ADD PRIMARY KEY (person_id);"))
    connection.execute(text("ALTER TABLE dim_event ADD PRIMARY KEY (crash_id);"))
    connection.execute(text("ALTER TABLE dim_road ADD PRIMARY KEY (crash_id);"))
    connection.execute(text("ALTER TABLE fact_fatalities ADD PRIMARY KEY (fatality_id);"))
    connection.execute(text("ALTER TABLE fact_crashes ADD PRIMARY KEY (crash_id);"))
    connection.execute(text("ALTER TABLE fact_number ADD PRIMARY KEY (number_date_id);"))

    connection.execute(text("ALTER TABLE fact_crashes ADD FOREIGN KEY (date_id) REFERENCES dim_date (date_id);"))
    connection.execute(text("ALTER TABLE fact_crashes ADD FOREIGN KEY (lga_id) REFERENCES dim_lga (lga_id);"))
    connection.execute(text("ALTER TABLE fact_crashes ADD FOREIGN KEY (state_id) REFERENCES dim_state (state_id);"))
    connection.execute(text("ALTER TABLE fact_fatalities ADD FOREIGN KEY (crash_id) REFERENCES fact_crashes (crash_id);"))
    connection.execute(text("ALTER TABLE fact_fatalities ADD FOREIGN KEY (person_id) REFERENCES dim_person (person_id);"))
    connection.execute(text("ALTER TABLE dim_lga ADD FOREIGN KEY (state_id) REFERENCES dim_state (state_id);"))
    connection.execute(text("ALTER TABLE fact_number ADD FOREIGN KEY (date_id) REFERENCES dim_date (date_id);"))
    connection.execute(text("ALTER TABLE dim_event ADD FOREIGN KEY (crash_id) REFERENCES fact_crashes (crash_id);"))
    connection.execute(text("ALTER TABLE dim_road ADD FOREIGN KEY (crash_id) REFERENCES fact_crashes (crash_id);"))
    connection.execute(text("ALTER TABLE dim_vehicle ADD FOREIGN KEY (crash_id) REFERENCES fact_crashes (crash_id);"))
    connection.execute(text("ALTER TABLE dim_time ADD FOREIGN KEY (crash_id) REFERENCES fact_crashes (crash_id);"))
    connection.commit()

print("Primary and foreign key constraints added.")

