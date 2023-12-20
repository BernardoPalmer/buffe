from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta

conn_str = (
    "mssql+pyodbc://Sa:P%40ssw0rd2023%40%23%24@"
    "192.168.1.43/SOUTTOMAYOR?"
    "driver=ODBC%20Driver%2011%20for%20SQL%20Server"
)
engine = create_engine(conn_str, pool_pre_ping=True)

#selecionando os produtos e subprodutos (ficha tecnica)

# Get the current date in YYYY-MM-DD format
current_date = datetime.now().strftime('%Y-%m-%d')

# Create the SQL query with the dynamic date
query = f"""
SELECT TPAMOVTOPED.IDX_PRODUTO, TPAMOVTOPED.L_QUANTIDADE, TPAMOVTOPED.DESCRICAO, TPADOCTOPED.DTEVENTO
FROM TPAMOVTOPED
JOIN TPADOCTOPED ON TPAMOVTOPED.RDX_DOCTOPED = TPADOCTOPED.PK_DOCTOPED
JOIN TPAPRODUTO ON TPAMOVTOPED.IDX_PRODUTO = TPAPRODUTO.PK_PRODUTO
WHERE TPAMOVTOPED.TIPOPROD = 'S' 
AND TPADOCTOPED.DTEVENTO >= '{current_date}'
AND TPADOCTOPED.SITUACAO IN ('B', 'V')
AND TPAPRODUTO.IDX_NEGOCIO != 'Desativados';
"""

# Now, `query` contains the SQL command with the current date


df = pd.read_sql(query, engine)

lower_bound = datetime(1678, 1, 1)
current_date2 = datetime.now()
upper_bound = current_date2 + timedelta(days=90)

df['DTEVENTO'] = pd.to_datetime(df['DTEVENTO'], errors='coerce')
df = df[(df['DTEVENTO'] >= lower_bound) & (df['DTEVENTO'] <= upper_bound)]

# Function to categorize datetime into time periods
def categorize_time_period(dt):
    hour = dt.hour
    if 6 <= hour < 10:
        return '06am às 10am manha 1'
    elif 10 <= hour < 13:
        return '10am às 13 manha 2'
    elif 13 <= hour < 16:
        return '13 às 16 tarde 1'
    elif 16 <= hour < 19:
        return '16 às 19 tarde 2 '
    else:
        return '19 em diante noite '

# Apply the function to create a new column for time periods
df['Time_Period'] = df['DTEVENTO'].apply(categorize_time_period)

# Convert 'DTEVENTO' to date
df['DTEVENTO'] = df['DTEVENTO'].dt.date

# Extract the first word from 'DESCRICAO'
df['First_Word_DESCRICAO'] = df['DESCRICAO'].str.split().str[0]

# Modify the pivot table to include time periods
transformed_df = pd.pivot_table(df, values='L_QUANTIDADE', index='First_Word_DESCRICAO', 
                                columns=['DTEVENTO', 'Time_Period'], 
                                aggfunc='sum', fill_value=0)

# Calculate the sum for each row and sort the DataFrame by it
transformed_df['Total'] = transformed_df.sum(axis=1)
df_sorted = transformed_df.sort_values('Total', ascending=False).drop(columns=['Total'])

###segunda parte

query = f"""
SELECT TPAMOVTOPED.IDX_PRODUTO, TPAMOVTOPED.L_QUANTIDADE, TPAMOVTOPED.DESCRICAO, TPADOCTOPED.DTEVENTO
FROM TPAMOVTOPED
JOIN TPADOCTOPED ON TPAMOVTOPED.RDX_DOCTOPED = TPADOCTOPED.PK_DOCTOPED
JOIN TPAPRODUTO ON TPAMOVTOPED.IDX_PRODUTO = TPAPRODUTO.PK_PRODUTO
WHERE TPAMOVTOPED.TIPOPROD = 'S' 
AND TPADOCTOPED.DTEVENTO >= '{current_date}'
AND TPADOCTOPED.SITUACAO != 'B'
AND TPADOCTOPED.SITUACAO != 'V'
AND TPAPRODUTO.IDX_NEGOCIO != 'Desativados';

"""
# Now, `query` contains the SQL command with the current date
df2 = pd.read_sql(query, engine)

df2['DTEVENTO'] = pd.to_datetime(df2['DTEVENTO'], errors='coerce')
df2 = df2[(df2['DTEVENTO'] >= lower_bound) & (df2['DTEVENTO'] <= upper_bound)]

# Function to categorize datetime into time periods
def categorize_time_period(dt):
    hour = dt.hour
    if 6 <= hour < 10:
        return '06am às 10am manha 1'
    elif 10 <= hour < 13:
        return '10am às 13 manha 2'
    elif 13 <= hour < 16:
        return '13 às 16 tarde 1'
    elif 16 <= hour < 19:
        return '16 às 19 tarde 2 '
    else:
        return '19 em diante noite '

# Apply the function to create a new column for time periods
df2['Time_Period'] = df2['DTEVENTO'].apply(categorize_time_period)

# Convert 'DTEVENTO' to date
df2['DTEVENTO'] = df2['DTEVENTO'].dt.date

# Extract the first word from 'DESCRICAO'
df2['First_Word_DESCRICAO'] = df2['DESCRICAO'].str.split().str[0]

# Modify the pivot table to include time periods
transformed_df2 = pd.pivot_table(df2, values='L_QUANTIDADE', index='First_Word_DESCRICAO', 
                                columns=['DTEVENTO', 'Time_Period'], 
                                aggfunc='sum', fill_value=0)

# Calculate the sum for each row and sort the DataFrame by it
transformed_df2['Total'] = transformed_df2.sum(axis=1)
df_sorted2 = transformed_df2.sort_values('Total', ascending=False).drop(columns=['Total'])


Intermitentes = 104


def my_script_function():
    return df_sorted

def my_script_function3():
    return df_sorted2

def my_script_function4():
    return Intermitentes