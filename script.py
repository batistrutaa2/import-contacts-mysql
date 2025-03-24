import requests
import csv
import io
import mysql.connector
from datetime import datetime
import random
import hashlib
import time
from dotenv import load_dotenv
import os

# Carregar variáveis do arquivo .env
load_dotenv()

# Função para gerar o id_operacao de forma semelhante à sua função PHP
def generate_unique_id():
    timestamp = int(time.time())  # Obtém o timestamp atual
    random_string = hashlib.md5(str(random.random()).encode()).hexdigest()[:5]  # Gera string aleatória
    return f"{timestamp}{random_string}"

# Configuração do banco de dados a partir do .env
db_config = {
    'host': os.getenv('DB_HOST', ''),
    'user': os.getenv('DB_USER', ''),
    'password': os.getenv('DB_PASSWORD', ''),
    'database': os.getenv('DB_NAME', '')
}

# URLs das planilhas
planilhas = [
    os.getenv('PL1', ''),
    os.getenv('PL2', '')
]

def processar_planilha(url):
    response = requests.get(url)
    if response.status_code == 200:
        csv_data = response.content.decode("utf-8")
        reader = csv.reader(io.StringIO(csv_data))
        data = list(reader)
        headers = data[0]
        rows = data[1:]
        results = [dict(zip(headers, row)) for row in rows]

        try:
            conn = mysql.connector.connect(**db_config)
            cursor = conn.cursor()
            for item in results:
                try:
                    id_operacao = generate_unique_id()
                    cursor.execute("""
                        SELECT 1 FROM contatos WHERE telefone1 = %s AND empresa_id = 2 AND is_ads = 'Y' LIMIT 1
                    """, (item.get('TELEFONE', ''),))
                    
                    if cursor.fetchone():
                        print(f"Cliente {item.get('TELEFONE', '')} já existe. Ignorando inserção.")
                        continue
                    
                    cursor.execute(""" 
                        INSERT INTO contatos (
                            id_operacao,
                            empresa_id,
                            user_import_id,
                            is_ads,
                            tipo_criativo,
                            nome_base,
                            status,
                            nome_cliente,
                            telefone1,
                            email,
                            vidas,        
                            created_at,
                            updated_at                 
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                        id_operacao,
                        2,
                        1,
                        "Y",
                        item.get('CRIATIVO', ''),
                        "LAKS ANUNCIO META ADS",
                        "Y",
                        item.get('NOME', ''),
                        item.get('TELEFONE', ''),
                        item.get('EMAIL', ''),
                        item.get('VIDAS'),
                        datetime.now(),
                        datetime.now(),
                    ))
                    print(f"Dados do cliente {item.get('TELEFONE', '')} inseridos com sucesso!")
                except mysql.connector.Error as db_error:
                    print(f"Erro ao inserir o cliente {item.get('TELEFONE', '')}: {db_error}")
                    conn.rollback()
            conn.commit()
        except mysql.connector.Error as db_error:
            print(f"Erro ao conectar com o banco de dados: {db_error}")
        finally:
            cursor.close()
            conn.close()
        print("\n✅ Dados inseridos no banco com sucesso!")
    else:
        print(f"❌ Erro ao acessar a planilha! Código HTTP: {response.status_code}")

# Processar ambas as planilhas
for url in planilhas:
    processar_planilha(url)
