import pandas as pd

# df = pd.read_csv('201810-citibike-tripdata-cleaned.csv')
# df = df.dropna(inplace=True)
# # end_station = df["end station id"].values.tolist()
# # end_station = sorted(set(end_station))

# # print(end_station)
# print(df.columns)
# df.to_csv('201810-citibike-tripdata-cleaned.csv', index=False)

import pandas as pd

# --- Step 1: Load your CSV file ---
# Replace 'input.csv' with your actual filename
df = pd.read_csv('201810-citibike-tripdata.csv')

# --- Step 2: Remove NaN values ---

# Option A: Remove any rows with NaN values
df_cleaned = df.dropna()

# (Optional) If you want to remove columns with NaN instead, use:
# df_cleaned = df.dropna(axis=1)

# --- Step 3: Save the cleaned DataFrame to a new CSV file ---
df_cleaned.to_csv('cleaned-201810-citibike-tripdata.csv', index=False)

print("✅ Cleaned CSV saved as 'cleaned_output.csv'")
