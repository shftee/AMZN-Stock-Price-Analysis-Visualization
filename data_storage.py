import sqlite3
import pandas as pd

conn = sqlite3.connect('stock_data.db')
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS amzn_stock (
    Date TEXT,
    Open REAL,
    High REAL,
    Low REAL,
    Close REAL,
    Adj_Close REAL,
    Volume INTEGER
)
''')

conn.commit()

# cleaned dataset
file_path = 'AMZN.csv'
data = pd.read_csv(file_path)

# inserting data
data.to_sql('amzn_stock', conn, if_exists='replace', index=False)

# query to retrieve data
query = '''
SELECT * FROM amzn_stock WHERE Date BETWEEN '2023-01-01' AND '2023-12-31'
'''
df = pd.read_sql_query(query, conn)
# print(df.head())

df['Date'] = pd.to_datetime(df['Date'])
df['MA50'] = df['Close'].rolling(window=50).mean()
df['MA200'] = df['Close'].rolling(window=200).mean()

file_path = 'AMZN_2023.xlxs'
df.to_csv(file_path, index=False)

