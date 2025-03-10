import requests
import csv
import io
import mysql.connector
from datetime import datetime
import random
import hashlib
import time

# Função para gerar o id_operacao de forma semelhante à sua função PHP
def generate_unique_id():
    timestamp = int(time.time())  # Obtém o timestamp atual
    random_string = hashlib.md5(str(random.random()).encode()).hexdigest()[:5]  # Gera string aleatória
    return f"{timestamp}{random_string}"

# Configuração do banco de dados
db_config = {
    'host': 'mysql',  # Usando o nome do serviço Docker
    'user': 'root',   # Nome do usuário do MySQL
    'password': '',   # Senha do MySQL (se houver)
    'database': 'salescontrol-new'  # Nome do banco de dados
}

# URL do Google Sheets no formato CSV
url = "https://docs.google.com/spreadsheets/d/1glvrd-KpY9p3OvsuY1YC7TRaN3BdTTeu1e0AfiSfDRU/gviz/tq?tqx=out:csv&gid=806051143"

# Fazer a requisição HTTP para obter os dados
response = requests.get(url)

if response.status_code == 200:
    # Decodificar os dados CSV
    csv_data = response.content.decode("utf-8")
    
    # Ler os dados usando a biblioteca CSV
    reader = csv.reader(io.StringIO(csv_data))
    data = list(reader)

    # Separar cabeçalhos e linhas
    headers = data[0]
    rows = data[1:]

    # Criar lista de dicionários
    results = [dict(zip(headers, row)) for row in rows]

    # Conectar ao banco de dados
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Inserir os dados na tabela existente
        for item in results:
            try:
                # Gerar o id_operacao
                id_operacao = generate_unique_id()

                # Verificar se o cliente já existe no banco (usando o telefone como critério)
                cursor.execute("""
                    SELECT 1 FROM contatos WHERE telefone1 = %s AND empresa_id = 2 AND is_ads = 'Y' LIMIT 1
                """, (item.get('TELEFONE', ''),))
                
                if cursor.fetchone():  # Se o cliente já existe
                    print(f"Cliente {item.get('TELEFONE', '')} já existe. Ignorando inserção.")
                    continue  # Pula para o próximo item

                # Mapear os campos da planilha para a tabela
                cursor.execute(""" 
                    INSERT INTO contatos (
                        id_operacao,
                        empresa_id,
                        user_import_id,
                        is_ads,
                        tipo_criativo,
                        data_inscricao,
                        nome_base,
                        status,
                        nome_cliente,
                        data_nascimento,
                        cpf,
                        plano,
                        categoria,
                        entidade,
                        telefone1,
                        telefone2,
                        telefone3,
                        email,
                        idades,
                        plano_ativo,
                        vidas,
                        valor_plano_atual,
                        valor_negociacao,
                        created_at,
                        updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    id_operacao,
                    2,
                    1,
                    "Y",
                    item.get('CRIATIVO', ''),
                    
                ))
                
                # Logar sucesso
                print(f"Dados do cliente {item.get('TELEFONE', '')} inseridos com sucesso!")

            except mysql.connector.Error as db_error:
                # Captura erro no banco de dados
                print(f"Erro ao inserir o cliente {item.get('TELEFONE', '')}: {db_error}")
                conn.rollback()  # Rollback da transação em caso de erro

        # Confirmar e fechar conexão
        conn.commit()

    except mysql.connector.Error as db_error:
        print(f"Erro ao conectar com o banco de dados: {db_error}")

    finally:
        # Fechar a conexão independentemente de erros
        cursor.close()
        conn.close()

    print("\n✅ Dados inseridos no banco com sucesso!")
else:
    print(f"❌ Erro ao acessar a planilha! Código HTTP: {response.status_code}")
