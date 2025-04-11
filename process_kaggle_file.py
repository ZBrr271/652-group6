import pandas as pd
from datetime import datetime

df = pd.read_csv('hot-100-current.csv')

df['title'] = df['title'].str.strip().str.lower()
df['performer'] = df['performer'].str.strip().str.lower()

print(f"Number of records before removing duplicates: {len(df)}")

sorted_df = df.sort_values(by=['performer', 'title', 'chart_week'], ascending=[True, True, False]).reset_index(drop=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
sorted_hot100 = f"sorted_hot100_{timestamp}.csv"
sorted_df.to_csv(sorted_hot100, index=False)


sorted_df_uniques = sorted_df.drop_duplicates(subset=['title', 'performer'], keep='first').reset_index(drop=True)
sorted_df_uniques['chart_week'] = pd.to_datetime(sorted_df_uniques['chart_week'], errors='coerce')
sorted_df_uniques.rename(columns={'chart_week': 'most_recent_chart_wk'}, inplace=True)

sorted_df_uniques.replace('', None, inplace=True)

int_columns = ['current_week', 'last_week', 'peak_pos', 'wks_on_chart']
for col in int_columns:
    sorted_df_uniques[col] = pd.to_numeric(sorted_df_uniques[col], errors='coerce')

sorted_df_uniques[int_columns] = sorted_df_uniques[int_columns].where(pd.notnull(sorted_df_uniques[int_columns]), None)

for col in int_columns:
    sorted_df_uniques[col] = sorted_df_uniques[col].astype('Int64')

print(sorted_df_uniques.head(10))

for col in int_columns:
    sorted_df_uniques[col] = sorted_df_uniques[col].replace(0, None)

def check_utf8(series):
    for index, value in series.items():
        if isinstance(value, str):
            try:
                value.encode('utf-8')
            except UnicodeEncodeError:
                print(f"Non-UTF-8 character found: {value} in record:\n{series.name} - {sorted_df_uniques.iloc[index]}")

check_utf8(sorted_df_uniques['title'])
check_utf8(sorted_df_uniques['performer'])

unique_hot100 = f"unique_hot100_{timestamp}.csv"
sorted_df_uniques.to_csv(unique_hot100, index=False)

print(f"Number of records after removing duplicates: {len(sorted_df_uniques)}")

print(f"\n{sorted_df_uniques.head(20)}")

print(sorted_df_uniques.head(10))