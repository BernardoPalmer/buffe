from sqlalchemy import create_engine
import pandas as pd
conn_str = (
    "mssql+pyodbc://Sa:P%40ssw0rd2023%40%23%24@"
    "192.168.1.43/SOUTTOMAYOR?"
    "driver=ODBC%20Driver%2011%20for%20SQL%20Server"
)
engine = create_engine(conn_str, pool_pre_ping=True)

query = f"""
SELECT 
    TPDOCTO,
    DOCUMENTO,
    FORMAT(DTPREVISAO, 'HH:mm') AS Hora_Saída,  -- Format to hour and minute
    NOME
FROM 
    TPADOCTOPED
WHERE 
    YEAR(DTPREVISAO) = 2023 
    AND MONTH(DTPREVISAO) = 12
    AND DAY(DTPREVISAO) = 24
    AND TPDOCTO IN ('OR', 'EC')
    AND SITUACAO IN ('B','V','Z')

ORDER BY DTALT
"""

NATAL = pd.read_sql(query, engine)
NATAL

def my_script_function6():
    return NATAL