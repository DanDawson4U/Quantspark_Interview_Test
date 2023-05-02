import pandas as pd
import sqlite3
import requests
import gzip
from sqlalchemy import create_engine
from decimal import Decimal

# TODO implement more cleaning/reporting for bar_data.csv. There is a "36 Glasses" entry, for example
# TODO Test and Rewrite SQL if necessary
# TODO Streamline python, check for duplications of work and improve readability

# enable debug mode = 1 to print outputs in script
debug_mode = 0

# if we encounter data quality issues they are stored in this list and supplied to owner of data source
remediation_dicts = []

# ------------------------- Reading and Cleaning supplied datasets -------------------------------

# Read the data files
bar_data = pd.read_csv("Data/bar_data.csv")

# Read and clean the transaction data, ensuring two decimal places for cost
budapest_transactions = pd.read_csv(gzip.open("Data/budapest.csv.gz", 'rt'), delimiter=",")
budapest_transactions.columns = ['transaction_id', 'timestamp', 'drink', 'cost']
budapest_transactions['cost'] = budapest_transactions['cost'].apply(lambda x: Decimal(str(x)).quantize(Decimal('0.01')))
budapest_transactions['cost'] = budapest_transactions['cost'].astype('float64')
budapest_transactions['location'] = 'budapest'

london_transactions = pd.read_csv(gzip.open("Data/london_transactions.csv.gz", 'rt'), delimiter="\t", header=None)
london_transactions.columns = ['transaction_id', 'timestamp', 'drink', 'cost']
london_transactions['cost'] = london_transactions['cost'].apply(lambda x: Decimal(str(x)).quantize(Decimal('0.01')))
london_transactions['cost'] = london_transactions['cost'].astype('float64')
london_transactions['location'] = 'london'

ny_transactions = pd.read_csv(gzip.open("Data/ny.csv.gz", 'rt'), delimiter=",")
ny_transactions.columns = ['transaction_id', 'timestamp', 'drink', 'cost']
ny_transactions['cost'] = ny_transactions['cost'].apply(lambda x: Decimal(str(x)).quantize(Decimal('0.01')))
ny_transactions['cost'] = ny_transactions['cost'].astype('float64')
ny_transactions['location'] = 'new_york'

# print out headers if we want to see the outputs for debugging purposes
if debug_mode == 1:

    # Print first 5 rows of each dataset for inspection
    print("Bar Data:")
    print(bar_data.head())

    print("\nBudapest Transactions:")
    print(budapest_transactions.head())

    print("\nLondon Transactions:")
    print(london_transactions.head())

    print("\nNew York Transactions:")
    print(ny_transactions.head())

# Concatenate all transaction data
transactions = pd.concat([budapest_transactions, london_transactions, ny_transactions], ignore_index=True)

# Generate unique transaction IDs
transactions['unique_transaction_id'] = range(len(transactions))

# Remove the original 'transaction_id' column
transactions.drop('transaction_id', axis=1, inplace=True)

# Rename the bar column to location
bar_data = bar_data.rename(columns={'bar': 'location'})

if debug_mode == 1:

    print("\nConcatenated Transactions:")
    print(transactions.head())

# ------------------------- Fetching glass type data from cocktail API -------------------------------

# this is a free testing key to be replaced with a subscription in production
api_key = "1"

# Find all the glass types in our bar_data and format them for the API
unique_glass_types = bar_data['glass_type'].unique()

formatted_glass_types = [glass_type.replace(" ", "_").capitalize() for glass_type in unique_glass_types]


# Create an empty DataFrame to store the concatenated results
all_glass_types = pd.DataFrame(columns=['Drink', 'API_ID', 'Glass', 'drink_id', 'glass_id'])

# Initialize the drink_id counter
drink_id_counter = 0

for glass_type in formatted_glass_types:
    cocktails_api = f"https://www.thecocktaildb.com/api/json/v1/{api_key}/filter.php?g={glass_type}"
    response = requests.get(cocktails_api)

    try:
        glass_data = response.json()
    except ValueError:
        print(f"Failed to decode JSON for glass type: {glass_type}")
        remediation_dicts.append({"key_error": glass_type,
                                  "error_type": "mismatch to api label",
                                  "source": "bar_data.csv"})
        continue

    # Extract the drinks list from the JSON output
    drinks = glass_data['drinks']

    # Create a new DataFrame with the desired columns
    df = pd.DataFrame(columns=['Drink', 'API_ID', 'Glass', 'drink_id', 'glass_id'])

    for idx, drink in enumerate(drinks):
        drink_name = drink['strDrink']
        drink_api_id = drink['idDrink']

        # Check if the drink already exists in the DataFrame
        existing_drink = all_glass_types.loc[all_glass_types['Drink'] == drink_name]

        if len(existing_drink) > 0:
            # If the drink already exists, use its existing drink_id
            drink_id = existing_drink.iloc[0]['drink_id']
        else:
            # If the drink does not exist, increment the drink_id counter
            # and use the new drink_id
            drink_id_counter += 1
            drink_id = drink_id_counter

        # Get the glass_id for the given glass type
        glass_id = formatted_glass_types.index(glass_type) + 1

        # Add the new row to the DataFrame
        df.loc[idx] = [drink_name, drink_api_id, glass_type, drink_id, glass_id]

    # Concatenate the current DataFrame with the previous results
    all_glass_types = pd.concat([all_glass_types, df], ignore_index=True)

if debug_mode == 1:

    # Print the concatenated DataFrame
    print(all_glass_types)

# ------------------------------ Mappings Ensuring Consistent item IDs -----------------------------------------

# Create a dictionary to map formatted glass types to their corresponding glass_id
glass_id_mapping = all_glass_types[['Glass', 'glass_id']].drop_duplicates().set_index('Glass')['glass_id'].to_dict()

# Add a glass_id column to the bar_data DataFrame
bar_data['glass_id'] = bar_data['glass_type'].apply(lambda x: x.replace(" ", "_").capitalize()).map(glass_id_mapping)
bar_data['glass_id'].fillna(0, inplace=True)
bar_data['glass_id'] = bar_data['glass_id'].astype(int)

# Create a dictionary to map drink names to their corresponding drink_id
drink_id_mapping = all_glass_types.set_index('Drink')['drink_id'].to_dict()

# Add a drink_id column to the transactions DataFrame
transactions['drink_id'] = transactions['drink'].map(drink_id_mapping)
transactions['drink_id'].fillna(0, inplace=True)
transactions['drink_id'] = transactions['drink_id'].astype(int)

if debug_mode == 1:

    # print dataframes
    print(transactions.head())
    print(bar_data.head())
    # print data types
    print(all_glass_types.dtypes)
    print(transactions.dtypes)
    print(bar_data.dtypes)

# ------------------------------- SQLite Connections & Push To Tables ------------------------------------------------

# Connect to the database and create tables
engine = create_engine("sqlite:///bar_inventory_management.sqlite")
with open("data_tables.sql", "r") as f:
    create_tables = f.read()

def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Read the SQL script from the data_tables.sql file
sql_script = read_sql_file("data_tables.sql")

# Connect to the SQLite database and execute the SQL script
with sqlite3.connect("bar_inventory_management.sqlite") as conn:
    conn.executescript(sql_script)
    conn.commit()

# Import data into the database, we are replacing the data to maintain the indexing of our data
bar_data.to_sql("Bar_Data", engine, if_exists="replace", index=False)
transactions.to_sql("Transactions", engine, if_exists="replace", index=False)
all_glass_types.to_sql("DIM_Glasses", engine, if_exists="replace", index=False)

# Read the SQL script from the poc_tables.sql file
sql_script_2 = read_sql_file("poc_tables.SQL")

# Connect to the SQLite database and execute the 2nd SQL script
with sqlite3.connect("bar_inventory_management.sqlite") as conn:
    conn.executescript(sql_script_2)
    conn.commit()

print("Suggested Remediations:")
print(remediation_dicts)
