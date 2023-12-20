from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
conn_str = (
    "mssql+pyodbc://Sa:P%40ssw0rd2023%40%23%24@"
    "192.168.1.43/SOUTTOMAYOR?"
    "driver=ODBC%20Driver%2011%20for%20SQL%20Server"
)
engine = create_engine(conn_str, pool_pre_ping=True)

current_date = datetime.now().strftime('%Y-%m-%d')

query = f"""
SELECT 
    A.IDX_PRODUTO AS IDX_PRODUTO_VENDA,
    ISNULL(A.UNIDADE, 'LT') AS UNIDADE_VENDA,
    SUM(A.L_QUANTIDADE) AS QUANTIDADE_VENDA,
    DATEADD(WEEK, DATEDIFF(WEEK, 0, C.DTEVENTO), 0) AS SEMANA
    --D.IDX_NEGOCIO,
    --D.DESCRICAO
FROM TPAMOVTOPED A
JOIN TPADOCTOPED C ON A.RDX_DOCTOPED = C.PK_DOCTOPED
LEFT JOIN TPAPRODUTO D ON A.IDX_PRODUTO = D.PK_PRODUTO
WHERE C.TPDOCTO IN ('OR', 'EC', 'DG', 'CR')
AND C.SITUACAO IN ('B', 'V')
AND DATEADD(WEEK, DATEDIFF(WEEK, 0, C.DTEVENTO), 0) >= '2023-09-04 00:00:00.000'
AND DATEADD(WEEK, DATEDIFF(WEEK, 0, C.DTEVENTO), 0) <= '{current_date}'
AND D.IDX_NEGOCIO != 'Desativados'
GROUP BY 
    DATEADD(WEEK, DATEDIFF(WEEK, 0, C.DTEVENTO), 0),
    A.IDX_PRODUTO,
    A.UNIDADE,
    D.IDX_NEGOCIO,
    D.DESCRICAO
"""

MOVTOPED2 = pd.read_sql(query, engine)

MOVTOPED = pd.read_csv('movtoped.csv')
PRODCOMPOSICAO = pd.read_csv('prodcomposicao.csv')
PRODUTOS = pd.read_csv('produtos.csv')
DESCRICAO = pd.read_csv('descricao.csv')
AJUSTE = pd.read_csv('ajuste.csv')

MOVTOPED2['IDX_PRODUTO_VENDA'] = MOVTOPED2['IDX_PRODUTO_VENDA'].str.strip().astype(int)
MOVTOPED['SEMANA'] = pd.to_datetime(MOVTOPED['SEMANA'])
MOVTOPED2['SEMANA'] = pd.to_datetime(MOVTOPED2['SEMANA'])
MOVTOPED = pd.concat([MOVTOPED, MOVTOPED2], ignore_index=True)

'''
import itertools
# Merging MOVTOPED and PRODCOMPOSICAO based on the condition IDX_PRODUTO (MOVTOPED) == RDX_PRODUTO (PRODCOMPOSICAO)
MOV_PROD = pd.merge(MOVTOPED, PRODCOMPOSICAO, how='inner', left_on='IDX_PRODUTO_VENDA', right_on='RDX_PRODUTO_RECEITA')
#FORMATTING STRINS
MOV_PROD['UNIDADE_VENDA'] = MOV_PROD['UNIDADE_VENDA'].str.replace('\x00', '')
MOV_PROD['UNIDADE_RECEITA'] = MOV_PROD['UNIDADE_RECEITA'].str.replace('\x00', '')
MOV_PROD.loc[MOV_PROD['RENDIMENTO_FOR_RDX_PRODUTO'] == 1, 'RENDIMENTO_FOR_RDX_PRODUTO'] = 100
# Filter rows where UNIDADE_VENDA is 'UN'
mask = (MOV_PROD['UNIDADE_VENDA'] == 'UN')

# Update UNIDADE_VENDA to 'CT' for those rows
MOV_PROD.loc[mask, 'UNIDADE_VENDA'] = 'CT'

# Divide QUANTIDADE_VENDA by 100 for those rows
MOV_PROD.loc[mask, 'QUANTIDADE_VENDA'] = MOV_PROD.loc[mask, 'QUANTIDADE_VENDA'] / 100

MOV_PROD['QUANTIDADE_TOTAL1'] = MOV_PROD.apply(
    lambda row: (row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA'] * 100 if row['RENDIMENTO_FOR_RDX_PRODUTO'] == 1 else row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA']) if row['UNIDADE_VENDA'] == 'CT' else 
                (row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA'] / 100 * 100 if row['RENDIMENTO_FOR_RDX_PRODUTO'] == 1 else row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA'] / 100) if row['UNIDADE_VENDA'] == 'UN' else None, 
    axis=1
)
grouped_df = MOV_PROD.groupby(['SEMANA', 'UNIDADE_RECEITA', 'IDX_PRODUTO_RECEITA','QUANTIDADE_VENDA',])['QUANTIDADE_TOTAL1'].sum().reset_index()
list_of_grouped_df = MOV_PROD.groupby(['SEMANA', 'IDX_PRODUTO_VENDA', 'UNIDADE_VENDA'])['QUANTIDADE_VENDA'].sum().reset_index()

# Perform an outer join
merged_df = pd.merge(MOV_PROD, PRODUTOS, how='outer', left_on='IDX_PRODUTO_VENDA', right_on='PK_PRODUTO', indicator=True)
# Filter to get only the rows from MOV_PROD that are not in PRODUTOS
only_in_MOV_PROD = merged_df[merged_df['_merge'] == 'left_only']
only_in_MOV_PROD = only_in_MOV_PROD.groupby(['SEMANA', 'UNIDADE_VENDA', 'IDX_PRODUTO_VENDA'])['QUANTIDADE_VENDA'].sum().reset_index()

MOV_PRODUTOS2 = pd.merge(grouped_df, PRODCOMPOSICAO, how='inner', left_on='IDX_PRODUTO_RECEITA', right_on='RDX_PRODUTO_RECEITA')
MOV_PRODUTOS2['QUANTIDADE_TOTAL2'] = MOV_PRODUTOS2.apply(
    lambda row: (row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA']) if row['RENDIMENTO_FOR_RDX_PRODUTO'] == 1 else 
                (row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA'] / 5) if row['RENDIMENTO_FOR_RDX_PRODUTO'] == 0.2 else None,
    axis=1
)
grouped_df2 = MOV_PRODUTOS2.groupby(['SEMANA', 'QUANTIDADE_VENDA', 'IDX_PRODUTO_RECEITA_y','UNIDADE_RECEITA_y',])['QUANTIDADE_TOTAL2'].sum().reset_index()
list_of_grouped_df2 = MOV_PRODUTOS2.groupby(['SEMANA', 'IDX_PRODUTO_RECEITA_x', 'UNIDADE_RECEITA_x'])['QUANTIDADE_TOTAL1'].sum().reset_index()

MOV_PRODUTOS3 = pd.merge(grouped_df2, PRODCOMPOSICAO, how='inner', left_on='IDX_PRODUTO_RECEITA_y', right_on='RDX_PRODUTO_RECEITA')
MOV_PRODUTOS3['QUANTIDADE_TOTAL3'] = MOV_PRODUTOS3['QUANTIDADE_VENDA'] * MOV_PRODUTOS3['QUANTIDADE_RECEITA']
grouped_df3 = MOV_PRODUTOS3.groupby(['SEMANA', 'QUANTIDADE_VENDA', 'IDX_PRODUTO_RECEITA','UNIDADE_RECEITA',])['QUANTIDADE_TOTAL3'].sum().reset_index()
list_of_grouped_df3 = MOV_PRODUTOS3.groupby(['SEMANA', 'IDX_PRODUTO_RECEITA_y', 'UNIDADE_RECEITA_y'])['QUANTIDADE_TOTAL2'].sum().reset_index()

MOV_PRODUTOS4 = pd.merge(grouped_df3, PRODCOMPOSICAO, how='inner', left_on='IDX_PRODUTO_RECEITA', right_on='RDX_PRODUTO_RECEITA')
MOV_PRODUTOS4['QUANTIDADE_TOTAL4'] = MOV_PRODUTOS4['QUANTIDADE_VENDA'] * MOV_PRODUTOS4['QUANTIDADE_RECEITA']
grouped_df4 = MOV_PRODUTOS4.groupby(['SEMANA', 'QUANTIDADE_VENDA', 'IDX_PRODUTO_RECEITA_y','UNIDADE_RECEITA_y',])['QUANTIDADE_TOTAL4'].sum().reset_index()
list_of_grouped_df4 = MOV_PRODUTOS4.groupby(['SEMANA', 'IDX_PRODUTO_RECEITA_x', 'UNIDADE_RECEITA_x'])['QUANTIDADE_TOTAL3'].sum().reset_index()

MOV_PRODUTOS5 = pd.merge(grouped_df4, PRODCOMPOSICAO, how='inner', left_on='IDX_PRODUTO_RECEITA_y', right_on='RDX_PRODUTO_RECEITA')
MOV_PRODUTOS5['QUANTIDADE_TOTAL5'] = MOV_PRODUTOS5['QUANTIDADE_VENDA'] * MOV_PRODUTOS5['QUANTIDADE_RECEITA']
grouped_df5 = MOV_PRODUTOS5.groupby(['SEMANA', 'QUANTIDADE_VENDA', 'IDX_PRODUTO_RECEITA','UNIDADE_RECEITA',])['QUANTIDADE_TOTAL5'].sum().reset_index()
list_of_grouped_df5 = MOV_PRODUTOS5.groupby(['SEMANA', 'IDX_PRODUTO_RECEITA', 'UNIDADE_RECEITA'])['QUANTIDADE_TOTAL4'].sum().reset_index()

MOV_PRODUTOS6 = pd.merge(grouped_df5, PRODCOMPOSICAO, how='inner', left_on='IDX_PRODUTO_RECEITA', right_on='RDX_PRODUTO_RECEITA')

# Rename the columns of each DataFrame so that they match the final structure
only_in_MOV_PROD.rename(columns={
    'IDX_PRODUTO_VENDA': 'IDX_PRODUTO',
    'UNIDADE_VENDA': 'UNIDADE',
    'QUANTIDADE_VENDA': 'QUANTIDADE'
}, inplace=True)

list_of_grouped_df.rename(columns={
    'IDX_PRODUTO_VENDA': 'IDX_PRODUTO',
    'UNIDADE_VENDA': 'UNIDADE',
    'QUANTIDADE_VENDA': 'QUANTIDADE'
}, inplace=True)

list_of_grouped_df2.rename(columns={
    'IDX_PRODUTO_RECEITA_x': 'IDX_PRODUTO',
    'UNIDADE_RECEITA_x': 'UNIDADE',
    'QUANTIDADE_TOTAL1': 'QUANTIDADE'
}, inplace=True)

list_of_grouped_df3.rename(columns={
    'IDX_PRODUTO_RECEITA_y': 'IDX_PRODUTO',
    'UNIDADE_RECEITA_y': 'UNIDADE',
    'QUANTIDADE_TOTAL2': 'QUANTIDADE'
}, inplace=True)

list_of_grouped_df4.rename(columns={
    'IDX_PRODUTO_RECEITA_x': 'IDX_PRODUTO',
    'UNIDADE_RECEITA_x': 'UNIDADE',
    'QUANTIDADE_TOTAL3': 'QUANTIDADE'
}, inplace=True)

list_of_grouped_df5.rename(columns={
    'IDX_PRODUTO_RECEITA_y': 'IDX_PRODUTO',
    'UNIDADE_RECEITA_y': 'UNIDADE',
    'QUANTIDADE_TOTAL4': 'QUANTIDADE'
}, inplace=True)

# Combine all DataFrames into one
final_df = pd.concat([
    only_in_MOV_PROD,
    list_of_grouped_df,
    list_of_grouped_df2,
    list_of_grouped_df3,
    list_of_grouped_df4,
    list_of_grouped_df5
])

# Optional: If you want to reset the index
final_df.reset_index(drop=True, inplace=True)
grouped_final_df = final_df.groupby(['SEMANA', 'UNIDADE', 'IDX_PRODUTO'])['QUANTIDADE'].sum().reset_index()

# Rename the columns for each DataFrame to match the final structure
list_of_grouped_df.rename(columns={
    'QUANTIDADE': 'QUANTIDADE_TOTAL'
}, inplace=True)

grouped_df.rename(columns={
    'UNIDADE_RECEITA': 'UNIDADE',
    'IDX_PRODUTO_RECEITA': 'IDX_PRODUTO',
    'QUANTIDADE_TOTAL1': 'QUANTIDADE_TOTAL'
}, inplace=True)

grouped_df2.rename(columns={
    'IDX_PRODUTO_RECEITA_y': 'IDX_PRODUTO',
    'UNIDADE_RECEITA_y': 'UNIDADE',
    'QUANTIDADE_TOTAL2': 'QUANTIDADE_TOTAL'
}, inplace=True)

grouped_df3.rename(columns={
    'IDX_PRODUTO_RECEITA': 'IDX_PRODUTO',
    'UNIDADE_RECEITA': 'UNIDADE',
    'QUANTIDADE_TOTAL3': 'QUANTIDADE_TOTAL'
}, inplace=True)

grouped_df4.rename(columns={
    'IDX_PRODUTO_RECEITA_y': 'IDX_PRODUTO',
    'UNIDADE_RECEITA_y': 'UNIDADE',
    'QUANTIDADE_TOTAL4': 'QUANTIDADE_TOTAL'
}, inplace=True)

grouped_df5.rename(columns={
    'IDX_PRODUTO_RECEITA': 'IDX_PRODUTO',
    'UNIDADE_RECEITA': 'UNIDADE',
    'QUANTIDADE_TOTAL5': 'QUANTIDADE_TOTAL'
}, inplace=True)

# Concatenate all the renamed DataFrames
final_grouped_df = pd.concat([
    list_of_grouped_df,
    grouped_df,
    grouped_df2,
    grouped_df3,
    grouped_df4
])

# Optional: Reset the index
final_grouped_df.reset_index(drop=True, inplace=True)
grouped_final_df2 = final_grouped_df.groupby(['SEMANA', 'UNIDADE', 'IDX_PRODUTO'])['QUANTIDADE_TOTAL'].sum().reset_index()



grouped_final_df2['SEMANA'] = pd.to_datetime(grouped_final_df2['SEMANA'])

# Identify the oldest and newest weeks in the dataset
oldest_week = grouped_final_df2['SEMANA'].min()
newest_week = grouped_final_df2['SEMANA'].max()

all_weeks = pd.date_range(start=oldest_week, end=newest_week, freq='W-MON')

# Generate all unique IDX_PRODUTO and UNIDADE combinations from the original DataFrame
unique_products = grouped_final_df2['IDX_PRODUTO'].unique()

# Generate all possible combinations of week, IDX_PRODUTO, and UNIDADE
all_combinations = list(itertools.product(all_weeks, unique_products))

# Create a new DataFrame from these combinations
df_all_combinations = pd.DataFrame(all_combinations, columns=['SEMANA', 'IDX_PRODUTO'])

# Merge the new DataFrame with the original DataFrame to fill in missing rows
df_merged = pd.merge(df_all_combinations, grouped_final_df2, on=['SEMANA', 'IDX_PRODUTO'], how='left')

# Fill NaN values in QUANTIDADE_TOTAL with 0
df_merged['QUANTIDADE_TOTAL'].fillna(0, inplace=True)
df_merged['SEMANA'] = pd.to_datetime(df_merged['SEMANA'])
df_merged.set_index('SEMANA', inplace=True)
#df_merged['IDX_PRODUTO'] = df_merged['IDX_PRODUTO'].str.strip().astype(int)

#PRODUTOS['PK_PRODUTO'] = PRODUTOS['PK_PRODUTO'].str.strip().astype(int)
missing_values = set(PRODUTOS['PK_PRODUTO'].unique()) - set(df_merged['IDX_PRODUTO'].unique())
filtered_values = [value for value in PRODUTOS['PK_PRODUTO'].unique() if value not in missing_values]

df_merged = df_merged[df_merged['UNIDADE'] != 'UN']
# Identify the oldest and newest weeks in the dataset
oldest_week = df_merged.index.min()
newest_week = df_merged.index.max()

# Generate all weeks between the oldest and newest weeks
all_weeks = pd.date_range(start=oldest_week, end=newest_week, freq='W-MON')

# Generate all unique IDX_PRODUTO values from the filtered DataFrame
unique_products = df_merged['IDX_PRODUTO'].unique()

# Generate all possible combinations of week and IDX_PRODUTO
import itertools
all_combinations = list(itertools.product(all_weeks, unique_products))

# Create a new DataFrame from these combinations
df_all_combinations = pd.DataFrame(all_combinations, columns=['SEMANA', 'IDX_PRODUTO'])

# Set 'SEMANA' as a datetime column and set it as the index
df_all_combinations['SEMANA'] = pd.to_datetime(df_all_combinations['SEMANA'])
df_all_combinations.set_index('SEMANA', inplace=True)

# Merge the new DataFrame with the filtered DataFrame to fill in missing rows
df_merged = pd.merge(df_all_combinations, df_merged.reset_index(), on=['SEMANA', 'IDX_PRODUTO'], how='left')

# Fill NaN values in any necessary columns (e.g., 'QUANTIDADE_TOTAL') with appropriate values (e.g., 0)
df_merged['QUANTIDADE_TOTAL'].fillna(0, inplace=True)

# Set 'SEMANA' as the index again if needed
df_merged.set_index('SEMANA', inplace=True)
'''
import itertools
# Merging MOVTOPED and PRODCOMPOSICAO based on the condition IDX_PRODUTO (MOVTOPED) == RDX_PRODUTO (PRODCOMPOSICAO)
MOV_PROD = pd.merge(MOVTOPED, PRODCOMPOSICAO, how='inner', left_on='IDX_PRODUTO_VENDA', right_on='RDX_PRODUTO_RECEITA')
#FORMATTING STRINS
MOV_PROD['UNIDADE_VENDA'] = MOV_PROD['UNIDADE_VENDA'].str.replace('\x00', '')
MOV_PROD['UNIDADE_RECEITA'] = MOV_PROD['UNIDADE_RECEITA'].str.replace('\x00', '')
MOV_PROD['QUANTIDADE_TOTAL1'] = MOV_PROD.apply(
    lambda row: 
        (row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA'] if row['UNIDADE_VENDA'] != 'UN' else 
         (row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA'] / 100) if row['UNIDADE_VENDA'] == 'UN' and row['QUANTIDADE_RECEITA'] == 100.0 else
         (row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA'] / 10) if row['UNIDADE_VENDA'] == 'UN' and row['QUANTIDADE_RECEITA'] == 10.0 else
         row['QUANTIDADE_VENDA'] * row['QUANTIDADE_RECEITA']), 
    axis=1
)
grouped_df = MOV_PROD.groupby(['SEMANA', 'UNIDADE_RECEITA', 'IDX_PRODUTO_RECEITA','QUANTIDADE_VENDA',])['QUANTIDADE_TOTAL1'].sum().reset_index()
list_of_grouped_df = MOV_PROD.groupby(['SEMANA', 'IDX_PRODUTO_VENDA', 'UNIDADE_VENDA'])['QUANTIDADE_VENDA'].first().reset_index()

merged_df = pd.merge(MOV_PROD, PRODUTOS, how='outer', left_on='IDX_PRODUTO_VENDA', right_on='PK_PRODUTO', indicator=True)
# Filter to get only the rows from MOV_PROD that are not in PRODUTOS
only_in_MOV_PROD = merged_df[merged_df['_merge'] == 'left_only']
only_in_MOV_PROD = only_in_MOV_PROD.groupby(['SEMANA', 'UNIDADE_VENDA', 'IDX_PRODUTO_VENDA'])['QUANTIDADE_VENDA'].sum().reset_index()

# Rename the columns of each DataFrame so that they match the final structure
only_in_MOV_PROD.rename(columns={
    'IDX_PRODUTO_VENDA': 'IDX_PRODUTO',
    'UNIDADE_VENDA': 'UNIDADE',
    'QUANTIDADE_VENDA': 'QUANTIDADE'
}, inplace=True)

list_of_grouped_df.rename(columns={
    'IDX_PRODUTO_VENDA': 'IDX_PRODUTO',
    'UNIDADE_VENDA': 'UNIDADE',
    'QUANTIDADE_VENDA': 'QUANTIDADE'
}, inplace=True)


final_df = pd.concat([
    only_in_MOV_PROD,
    list_of_grouped_df
])

# Optional: If you want to reset the index
final_df.reset_index(drop=True, inplace=True)
grouped_final_df = final_df.groupby(['SEMANA', 'UNIDADE', 'IDX_PRODUTO'])['QUANTIDADE'].sum().reset_index()

# Rename the columns for each DataFrame to match the final structure
list_of_grouped_df.rename(columns={
    'QUANTIDADE': 'QUANTIDADE_TOTAL'
}, inplace=True)

grouped_df.rename(columns={
    'UNIDADE_RECEITA': 'UNIDADE',
    'IDX_PRODUTO_RECEITA': 'IDX_PRODUTO',
    'QUANTIDADE_TOTAL1': 'QUANTIDADE_TOTAL'
}, inplace=True)


# Concatenate all the renamed DataFrames
final_grouped_df = pd.concat([
    list_of_grouped_df,
    grouped_df
])

# Optional: Reset the index
final_grouped_df.reset_index(drop=True, inplace=True)
grouped_final_df2 = final_grouped_df.groupby(['SEMANA', 'UNIDADE', 'IDX_PRODUTO'])['QUANTIDADE_TOTAL'].sum().reset_index()



grouped_final_df2['SEMANA'] = pd.to_datetime(grouped_final_df2['SEMANA'])

# Identify the oldest and newest weeks in the dataset
oldest_week = grouped_final_df2['SEMANA'].min()
newest_week = grouped_final_df2['SEMANA'].max()
all_weeks = pd.date_range(start=oldest_week, end=newest_week, freq='W-MON')
# Generate all unique IDX_PRODUTO and UNIDADE combinations from the original DataFrame
unique_products = grouped_final_df2['IDX_PRODUTO'].unique()
# Generate all possible combinations of week, IDX_PRODUTO, and UNIDADE
all_combinations = list(itertools.product(all_weeks, unique_products))
# Create a new DataFrame from these combinations
df_all_combinations = pd.DataFrame(all_combinations, columns=['SEMANA', 'IDX_PRODUTO'])
# Merge the new DataFrame with the original DataFrame to fill in missing rows
df_merged = pd.merge(df_all_combinations, grouped_final_df2, on=['SEMANA', 'IDX_PRODUTO'], how='left')
# Fill NaN values in QUANTIDADE_TOTAL with 0
df_merged['QUANTIDADE_TOTAL'].fillna(0, inplace=True)
df_merged['SEMANA'] = pd.to_datetime(df_merged['SEMANA'])
df_merged.set_index('SEMANA', inplace=True)
#df_merged['IDX_PRODUTO'] = df_merged['IDX_PRODUTO'].str.strip().astype(int)
#PRODUTOS['PK_PRODUTO'] = PRODUTOS['PK_PRODUTO'].str.strip().astype(int)
missing_values = set(PRODUTOS['PK_PRODUTO'].unique()) - set(df_merged['IDX_PRODUTO'].unique())
filtered_values = [value for value in PRODUTOS['PK_PRODUTO'].unique() if value not in missing_values]
#df_merged = df_merged[df_merged['UNIDADE'] != 'UN'] WTF DID I HAVE THAT FOR
# Identify the oldest and newest weeks in the dataset
oldest_week = df_merged.index.min()
newest_week = df_merged.index.max()

# Generate all weeks between the oldest and newest weeks
all_weeks = pd.date_range(start=oldest_week, end=newest_week, freq='W-MON')

# Generate all unique IDX_PRODUTO values from the filtered DataFrame
unique_products = df_merged['IDX_PRODUTO'].unique()

# Generate all possible combinations of week and IDX_PRODUTO
import itertools
all_combinations = list(itertools.product(all_weeks, unique_products))

# Create a new DataFrame from these combinations
df_all_combinations = pd.DataFrame(all_combinations, columns=['SEMANA', 'IDX_PRODUTO'])

# Set 'SEMANA' as a datetime column and set it as the index
df_all_combinations['SEMANA'] = pd.to_datetime(df_all_combinations['SEMANA'])
df_all_combinations.set_index('SEMANA', inplace=True)

# Merge the new DataFrame with the filtered DataFrame to fill in missing rows
df_merged = pd.merge(df_all_combinations, df_merged.reset_index(), on=['SEMANA', 'IDX_PRODUTO'], how='left')


# Fill NaN values in any necessary columns (e.g., 'QUANTIDADE_TOTAL') with appropriate values (e.g., 0)
df_merged['QUANTIDADE_TOTAL'].fillna(0, inplace=True)

# Set 'SEMANA' as the index again if needed
df_merged.set_index('SEMANA', inplace=True)

# Identify the rows where UNIDADE is 'UN'
mask_un = df_merged['UNIDADE'] == 'UN'

# Update UNIDADE to 'CT' and divide QUANTIDADE_TOTAL by 100 for these rows
df_merged.loc[mask_un, 'UNIDADE'] = 'CT'
df_merged.loc[mask_un, 'QUANTIDADE_TOTAL'] /= 100

# Sum together the QUANTIDADE_TOTAL for records that now share the same SEMANA, IDX_PRODUTO, and UNIDADE ('CT')
df_merged = df_merged.reset_index().groupby(['SEMANA', 'IDX_PRODUTO', 'UNIDADE']).agg({'QUANTIDADE_TOTAL': 'sum'}).reset_index()

# If you want to set 'SEMANA' as the index again
df_merged.set_index('SEMANA', inplace=True)

oldest_week = grouped_final_df2['SEMANA'].min()
newest_week = grouped_final_df2['SEMANA'].max()
all_weeks = pd.date_range(start=oldest_week, end=newest_week, freq='W-MON')
# Generate all unique IDX_PRODUTO and UNIDADE combinations from the original DataFrame
unique_products = grouped_final_df2['IDX_PRODUTO'].unique()
# Generate all possible combinations of week, IDX_PRODUTO, and UNIDADE
all_combinations = list(itertools.product(all_weeks, unique_products))
# Create a new DataFrame from these combinations
df_all_combinations = pd.DataFrame(all_combinations, columns=['SEMANA', 'IDX_PRODUTO'])
df_merged = pd.merge(df_all_combinations, df_merged.reset_index(), on=['SEMANA', 'IDX_PRODUTO'], how='left')


# Fill NaN values in any necessary columns (e.g., 'QUANTIDADE_TOTAL') with appropriate values (e.g., 0)
df_merged['QUANTIDADE_TOTAL'].fillna(0, inplace=True)

# Set 'SEMANA' as the index again if needed
df_merged.set_index('SEMANA', inplace=True)

AJUSTE['SEMANA'] = pd.to_datetime(AJUSTE['SEMANA'])

# Now, group by 'IDX_PRODUTO' and 'SEMANA' and sum 'QUANTIDADE'.
result_df = AJUSTE.groupby(['IDX_PRODUTO', 'SEMANA'])['QUANTIDADE'].sum().reset_index()

result_df['IDX_PRODUTO'] = result_df['IDX_PRODUTO'].astype(int)
# Reset index of df_merged to allow for merging.
df_merged_reset = df_merged.reset_index()

# Merge the DataFrames on IDX_PRODUTO and SEMANA.
df_combined = pd.merge(df_merged_reset, result_df, on=['IDX_PRODUTO', 'SEMANA'], how='left')

# Fill NaN values with 0 after the merge to avoid issues with summing.
df_combined.fillna(0, inplace=True)

# Sum the QUANTIDADE into QUANTIDADE_TOTAL.
df_combined['QUANTIDADE_TOTAL'] += df_combined['QUANTIDADE']

# Drop the extra QUANTIDADE column if you no longer need it.
df_combined.drop('QUANTIDADE', axis=1, inplace=True)

# If you want to set SEMANA back as the index.
df_combined.set_index('SEMANA', inplace=True)





import pandas as pd
import itertools
from statsmodels.tsa.stattools import adfuller, acf, pacf
from sklearn.linear_model import LinearRegression
from statsmodels.tsa.arima.model import ARIMA
import numpy as np
from statsmodels.tsa.seasonal import STL
import time
import warnings
from statsmodels.tools.sm_exceptions import ConvergenceWarning
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from sklearn.preprocessing import PolynomialFeatures
from sklearn.pipeline import make_pipeline

# Suppress Warnings
warnings.filterwarnings("ignore", category=ConvergenceWarning)
# Suppress the UserWarnings related to SARIMAX
warnings.filterwarnings("ignore", "Non-stationary starting autoregressive parameters found.")
warnings.filterwarnings("ignore", "Non-invertible starting MA parameters found.")

def forecast_seasonal(seasonal, P=52, K=4):
    t = np.arange(1, len(seasonal) + 1)
    df = pd.DataFrame({'t': t, 'seasonal': seasonal})
    for k in range(1, K + 1):
        df[f'cos_{k}'] = np.cos(2 * np.pi * k * df['t'] / P)
        df[f'sin_{k}'] = np.sin(2 * np.pi * k * df['t'] / P)
    X = df[[f'cos_{k}' for k in range(1, K + 1)] + [f'sin_{k}' for k in range(1, K + 1)]]
    y = df['seasonal']
    model = LinearRegression()
    model.fit(X, y)
    t_forecast = np.array([len(seasonal) + 1])
    X_forecast = pd.DataFrame(columns=X.columns, index=[0])
    for k in range(1, K + 1):
        X_forecast[f'cos_{k}'] = np.cos(2 * np.pi * k * t_forecast / P)
        X_forecast[f'sin_{k}'] = np.sin(2 * np.pi * k * t_forecast / P)
        
    return model.predict(X_forecast)[0]

def forecast_seasonal2(seasonal):
    # Take the last value from the seasonal series
    last_seasonal_value = seasonal.iloc[-1]
    return last_seasonal_value

def forecast_trend(trend):
    X = np.arange(len(trend)).reshape(-1, 1)
    y = trend.values
    model = LinearRegression()
    model.fit(X, y)
    return model.predict(np.array([[len(trend)]]))[0]

def forecast_trend2(trend):
    # Drop NaN values
    trend = trend.dropna()
    
    # Fit the Holt's Exponential Smoothing model
    # Here we assume an additive trend; you can also try a multiplicative trend with trend='mul'
    model = ExponentialSmoothing(trend, trend='add')
    
    # Fit the model
    model_fit = model.fit()
    
    # Forecast the next point
    forecast = model_fit.forecast(steps=1).iloc[0]
    
    return forecast

def forecast_trend3(trend):
    X = np.arange(len(trend)).reshape(-1, 1)
    y = trend.values
    
    # Degree of the polynomial, set to 5
    degree = 5
    
    # Create a pipeline with Polynomial Features and Linear Regression
    polyreg = make_pipeline(PolynomialFeatures(degree), LinearRegression())
    polyreg.fit(X, y)
    
    # Forecast the trend for the next point
    trend_forecast = polyreg.predict(np.array([[len(trend)]]))[0]
    
    return trend_forecast

def forecast_residual(residuals):
    adf_result = adfuller(residuals.dropna())
    p_value = adf_result[1]
    d_values = [0] if p_value < 0.05 else [1]
    lags_acf = acf(residuals, nlags=40)
    lags_pacf = pacf(residuals, nlags=40, method='ols')
    p_values = range(0, min([i for i, x in enumerate(lags_pacf) if abs(x) > 0.2], default=0) + 1)
    q_values = range(0, min([i for i, x in enumerate(lags_acf) if abs(x) > 0.2], default=0) + 1)
    best_aic = float("inf")
    best_order = None
    for p, d, q in itertools.product(p_values, d_values, q_values):
        try:
            model = ARIMA(residuals.dropna(), order=(p, d, q))
            results = model.fit()
            aic = results.aic
            if aic < best_aic:
                best_aic = aic
                best_order = (p, d, q)
        except:
            continue
    if best_order:
        model = ARIMA(residuals.dropna(), order=best_order)
        results = model.fit()
        return results.forecast(steps=1).iloc[0]
    else:
        return np.nan
    

from itertools import product
start_time = time.time()
forecast_mape_df = pd.DataFrame(columns=['IDX_PRODUTO', 'Forecast', 'MAPE'])
dfs = []


for random_product in filtered_values:
    product_data = df_combined[df_combined['IDX_PRODUTO'] == random_product].copy()
    product_data.sort_index(inplace=True)

    # Generate a new date range that includes all the weeks
    full_date_range = pd.date_range(start=product_data.index.min(), end=product_data.index.max(), freq='W-MON')

# Reindex the DataFrame
    product_data_reindexed = product_data.reindex(full_date_range)
    product_data_reindexed.index.freq = 'W-MON'

    # Perform STL decomposition
    stl = STL(product_data_reindexed['QUANTIDADE_TOTAL'].dropna(), seasonal=53)
    result = stl.fit()
    residuals = result.resid
    seasonal = result.seasonal
    trend = result.trend

    # Initialize variables to store the best MAPE and corresponding forecasts
    best_mape = float('inf')
    best_trend_forecast = None
    best_seasonal_forecast = None

    residual_forecast = forecast_residual(residuals) 
    
    for trend_func, seasonal_func in product([forecast_trend, forecast_trend2, forecast_trend3],  # ... add more functions as needed
                                         [forecast_seasonal, forecast_seasonal2]):  # ... add more functions as needed

    # Calculate forecasts
        trend_forecast = trend_func(trend)
        seasonal_forecast = seasonal_func(seasonal)
         # Assuming this stays the same

    # Calculate final forecast and MAPE
        final_forecast = trend_forecast + seasonal_forecast + residual_forecast
        actual = product_data['QUANTIDADE_TOTAL'].iloc[-1]
    
        mape = np.abs((final_forecast - actual) / actual) * 100 if actual != 0 else float('inf')
 # problema ta aqui

    # Update best MAPE and corresponding forecasts if this one is better
        if mape < best_mape:
            best_mape = mape
            best_trend_forecast = trend_forecast
            best_seasonal_forecast = seasonal_forecast
        if best_trend_forecast is None or best_seasonal_forecast is None:
            best_trend_forecast = 0
            best_seasonal_forecast = 0
            mape_value = float('inf')
        else:
            forecast_value = best_trend_forecast + best_seasonal_forecast + residual_forecast
            mape_value = best_mape

    # Now, best_trend_forecast and best_seasonal_forecast have the forecasts that gave the lowest MAPE
    temp_df = pd.DataFrame({'IDX_PRODUTO': [random_product], 
                    'Forecast': [best_trend_forecast + best_seasonal_forecast + residual_forecast], 
                    'MAPE': [best_mape]})
    dfs.append(temp_df)

forecast_mape_df = pd.concat(dfs, ignore_index=True)
end_time = time.time()

elapsed_time = end_time - start_time

current_date = datetime.strptime(current_date, "%Y-%m-%d")
weekday = current_date.weekday()

# Calculate the date for the Monday of the same week
monday_of_week = current_date - timedelta(days=weekday)

forecast_mape_df = forecast_mape_df.sort_values(by='MAPE', ascending=True)
df_filtered = df_combined.loc[df_combined.index == monday_of_week]
grouped_df = df_filtered.groupby(['IDX_PRODUTO','UNIDADE'])['QUANTIDADE_TOTAL'].sum().reset_index()
filtered_grouped_df = grouped_df[grouped_df['IDX_PRODUTO'].isin(filtered_values)]
final_combined_df = forecast_mape_df.merge(filtered_grouped_df, on='IDX_PRODUTO', how='left')
final_combined_df = final_combined_df.merge(
    DESCRICAO, left_on='IDX_PRODUTO', right_on='PK_PRODUTO', how='left'
).drop('PK_PRODUTO', axis=1)
# Get the second most recent week
second_most_recent_week = df_merged.index.unique()[-2]
current_year = second_most_recent_week.year

# Create an empty dictionary to store the results
average_last_trimester = {}

# Loop over each product ID
for idx in filtered_values:
    # Filter dataframe by product ID
    df_filtered = df_combined[df_combined['IDX_PRODUTO'] == idx]
    
    if second_most_recent_week.month <= 3:
        # For the first three months, average the same months from the prior year
        start_date = pd.Timestamp(f"{current_year - 1}-01-01")
        end_date = pd.Timestamp(f"{current_year - 1}-03-31")
        df_trimester = df_filtered.loc[start_date:end_date]
        
    else:
        # Get data from 12 weeks up to the second most recent week for all other months
        df_trimester = df_filtered.loc[:second_most_recent_week].tail(12)
    
    # Calculate the average quantity
    avg_quantity = df_trimester['QUANTIDADE_TOTAL'].mean()
    
    # Store the result
    average_last_trimester[idx] = avg_quantity

final_combined_df['ESTOQUE_MINIMO'] = final_combined_df['IDX_PRODUTO'].map(average_last_trimester)

result = final_combined_df[['DESCRICAO','MAPE','UNIDADE','Forecast','QUANTIDADE_TOTAL','ESTOQUE_MINIMO']]

order = ['CT', 'GR', 'KG']
# This creates a dictionary that assigns a lower number for your preferred items
order_dict = {key: i for i, key in enumerate(order)}

# Step 2: Map the custom order to a new column
# Items not in 'order' get a default high number to ensure they are sorted last
result['sort_order'] = result['UNIDADE'].map(lambda x: order_dict.get(x, 1000))

# Step 3: Sort the DataFrame by the new column
result_sorted = result.sort_values(by=['sort_order', 'UNIDADE'])

# Step 4: Optionally, drop the 'sort_order' column if it's no longer needed
result_sorted = result_sorted.drop('sort_order', axis=1)

# Filter the DataFrame for rows where 'UNIDADE' is 'CT'
ct_rows = result[result['UNIDADE'] == 'CT']

# Sum the 'QUANTIDADE_TOTAL' column of these rows
ct_total_sum = ct_rows['QUANTIDADE_TOTAL'].sum()
print(ct_total_sum)
def my_script_function5():
    return ct_total_sum

def my_script_function2():
    return result_sorted