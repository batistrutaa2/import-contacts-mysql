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
                    telefone = item.get('TELEFONE', '')
                    # Verificar se o cliente já existe
                    cursor.execute("""
                        SELECT id FROM contatos WHERE telefone1 = %s AND empresa_id = 2 AND is_ads = 'Y' LIMIT 1
                    """, (telefone,))
                    
                    existing_client = cursor.fetchone()

                    plano_ativo = 'Y' if item.get('POSSUI PLANO', '').strip().lower() == 'sim' else 'N'
                    possui_cnpj = 'Y' if item.get('TEM CNPJ', '').strip().lower() in ['CNPJ', 'MEI', 'sim'] else 'N'

                    user_import_id = 1  # Defina este valor conforme necessário

                    if existing_client:
                        # O cliente já existe, então vamos atualizar os dados
                        client_id = existing_client[0]
                        cursor.execute("""
                            UPDATE contatos
                            SET 
                                tipo_criativo = %s,
                                nome_base = %s,
                                status = %s,
                                nome_cliente = %s,
                                email = %s,
                                vidas = %s,
                                plano_ativo = %s,
                                possui_cnpj = %s,   
                                updated_at = %s
                            WHERE id = %s
                        """, (
                            item.get('CRIATIVO', ''),
                            "LAKS ANUNCIO META ADS",
                            "Y",
                            item.get('NOME', ''),
                            item.get('EMAIL', ''),
                            item.get('VIDAS'),
                            plano_ativo,
                            possui_cnpj,
                            datetime.now(),
                            client_id
                        ))
                        print(f"Dados do cliente {telefone} atualizados com sucesso!")
                    else:
                        # Cliente não encontrado, vamos inserir os dados
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
                                plano_ativo,
                                possui_cnpj,         
                                created_at,
                                updated_at                  
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""", (
                            id_operacao,
                            2,
                            user_import_id,  # Definido aqui
                            "Y",
                            item.get('CRIATIVO', ''),
                            "LAKS ANUNCIO META ADS",
                            "Y",
                            item.get('NOME', ''),
                            telefone,
                            item.get('EMAIL', ''),
                            item.get('VIDAS'),
                            plano_ativo,
                            possui_cnpj,
                            datetime.now(),
                            datetime.now(),
                        ))
                        print(f"Dados do cliente {telefone} inseridos com sucesso!")
                except mysql.connector.Error as db_error:
                    print(f"Erro ao processar o cliente {item.get('TELEFONE', '')}: {db_error}")
                    conn.rollback()
            conn.commit()
        except mysql.connector.Error as db_error:
            print(f"Erro ao conectar com o banco de dados: {db_error}")
        finally:
            cursor.close()
            conn.close()
        print("\n✅ Dados processados com sucesso!")
    else:
        print(f"❌ Erro ao acessar a planilha! Código HTTP: {response.status_code}")

# Processar ambas as planilhas
for url in planilhas:
    processar_planilha(url)
