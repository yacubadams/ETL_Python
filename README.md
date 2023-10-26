# ETL_Python
Simple ETL Using Python Load to PostgreSQL
def extract():
    url = 'https://www.alphavantage.co/query?function=CASH_FLOW&symbol=IBM&apikey=demo'
    response = requests.get(url)
    data = response.json()
    return data

def transform(data):
    """
    Transform the raw data into structured DataFrames.

    Parameters:
    - data: The raw data fetched from the API.

    Returns:
    - Two Pandas DataFrames: annual_reports_df, quarterly_reports_df.
    """
    
    # Extract data for annual and quarterly reports
    annual_reports_df = pd.DataFrame(data.get('annualReports', []))
    quarterly_reports_df = pd.DataFrame(data.get('quarterlyReports', []))
    
    return annual_reports_df, quarterly_reports_df


# Fetch and transform the data
data = extract()
annual_reports_df, quarterly_reports_df = transform(data)


def save_to_csv(df, filename):
    """
    Save a DataFrame to a CSV file.

    Parameters:
    - df: The Pandas DataFrame to be saved.
    - filename: The name of the CSV file.
    """
    df.to_csv(filename, index=False)


    
# Save the transformed data to CSV files
save_to_csv(annual_reports_df, 'annual_reports.csv')
save_to_csv(quarterly_reports_df, 'quarterly_reports.csv')

print("Data saved to 'annual_reports.csv' and 'quarterly_reports.csv'")



def load_into_postgres(df, table_name):
    """
    Load a Pandas DataFrame into a PostgreSQL table.

    Parameters:
    - df: The Pandas DataFrame.
    - table_name: The name of the table to load the data into.
    """
    database_url = "postgresql://localhost:5432/alphavantagedata"
    engine = create_engine(database_url)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
# Now, you can call the function without passing the database_url:
load_into_postgres(annual_reports_df, 'annual_reports')
load_into_postgres(quarterly_reports_df, 'quarterly_reports')
