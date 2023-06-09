import requests
import pandas as pd
from io import StringIO
import os
from sqlalchemy import create_engine

# URL base dos arquivos CSV
base_url = 'https://dados.turismo.gov.br/dataset/chegada-de-turistas-internacionais/resource/'

# arquivos em csv
urls = {
    '2018': base_url + '7495cab5-6597-4015-95ec-d161d756ee41/download/chegadas_2018.csv',
    '2019': base_url + '24057369-a5dc-45b4-a66f-9a6a8f8e3422/download/chegadas_2019.csv',
    '2020': base_url + '92e6e604-156f-4e69-830b-9c34abcd14cb/download/chegadas_2020.csv',
    '2021': base_url + '21f188c3-5d93-4124-ae5c-135490a26acf/download/chegadas_2021.csv',
    '2022': base_url + '8df14749-5e61-4887-90d0-c9ad7af3a14c/download/chegadas_2022.csv',
}

# fazer download do arquivo CSV
def download_csv(url):
    response = requests.get(url)
    content = response.content
    return content

# pasta para salvar os arquivos CSV
folder_path = 'C:/Users/Gabriela/Downloads/Impacta/config/venv'
if not os.path.exists(folder_path):
    os.makedirs(folder_path)

# Criar conexão com o banco de dados SQL Server
username = 'impacta'
password = '1234'
database = 'dados_turismo'
engine = create_engine(f'mssql+pyodbc://{username}:{password}@LAPTOP-EI084BHV\GABIIBR/{database}?driver=ODBC+Driver+17+for+SQL+Server')

# Loop para fazer download dos arquivos CSV e concatená-los
df_concatenado = pd.DataFrame()
for ano, url in urls.items():
    csv_content = download_csv(url)
    df = pd.read_csv(StringIO(csv_content.decode('ISO-8859-1')), delimiter=';')  # Correção do encoding para 'ISO-8859-1'
    df_concatenado = pd.concat([df_concatenado, df])

# Visualização das primeiras linhas
print(df_concatenado.head())

# Concatenado no arquivo CSV
df_concatenado.to_csv(os.path.join(folder_path, 'chegada_turistas_internacionais.csv'), index=False)

# Concatenado no arquivo XLSX
df_concatenado.to_excel(os.path.join(folder_path, 'chegada_turistas_internacionais.xlsx'), index=False)

# Salvar DataFrame no SQL Server
df_concatenado.to_sql('chegada_turistas_internacionais', con=engine, if_exists='replace', index=False)
