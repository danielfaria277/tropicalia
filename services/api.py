import requests
from decimal import Decimal
import locale
import mysql.connector
import json
import time
from requests_oauthlib import OAuth2Session

# Informações de autenticação
client_id = 'c8c70f924459b3312a1d8b609203552f5d7abc6d'
client_secret = 'a8718da0f7041ded52ee4480139851500602eb5b17793103a5c856402170'
token_url = 'https://www.bling.com.br/Api/v3/oauth/token'

# Dados do token
access_token = '1529eebfcf70eca9d2c4abe08f7ae4b58d349687'
token_type = 'Bearer'
expires_in = 21600
scope = ['98308', '98309', '98310', '98313', '98314', '98619', '101584', '104142', '104163', '107041', '384761', '507943', '575904', '1563512', '5990556', '6631498', '106168710', '182224097', '199272829', '200802821', '220621674', '318257547', '318257548', '318257550', '318257553', '318257555', '318257556', '318257559', '318257561', '318257562', '318257563', '318257564', '318257565', '318257568', '318257570', '318257573', '318257576', '318257577', '318257578', '318257580', '318257583', '333936575', '363921589', '363921590', '363921591', '363921592', '363921598', '363921599', '363953167', '363953556', '363953706', '380039494', '791588404', '875116881', '875116885', '1649295804', '1780272711', '1869535257', '5862218180', '6239411327', '6239420709', '13645012976', '13645012997', '13645012998', '13645013013']
refresh_token = 'bea238103acd4cb7ba1a9808facd71cc026785b4'

# Configuração do cliente OAuth2
oauth = OAuth2Session(
    client_id,
    token={'access_token': access_token, 'token_type': token_type, 'expires_in': expires_in, 'refresh_token': refresh_token, 'scope': scope}
)

# Função para renovar o token se estiver prestes a expirar
def renew_token():
    if 'expires_at' in oauth.token and time.time() > oauth.token['expires_at'] - 300:
        print('Token prestes a expirar. Renovando...')
        token = oauth.refresh_token(token_url, refresh_token=refresh_token, auth=(client_id, client_secret))
        # Atualizar a sessão OAuth2 com o novo token
        oauth.token = token
        print('Novo token obtido:', oauth.token['access_token'])

#####################################################################################
                    ##########FUNÇÕES#############
            ################# PARAMETROS DO SGBD#########
meubd = mysql.connector.connect(
    host='y6aj3qju8efqj0w1.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
    user = 'gwc2kpr6extza78g',
    password = 'eb9tth3ylrt9xx34',
    database =  'ni75bgwvrlo11xf8'
)

cursor = meubd.cursor()

cursor.execute("SELECT * FROM ni75bgwvrlo11xf8.estoque_produtos")

meu_resultado = cursor.fetchall()

# Função para fazer a solicitação à API
def make_api_request():
    api_url = 'https://www.bling.com.br/Api/v3/estoques/saldos/9168257545?idsProdutos%5B%5D=9826076834&idsProdutos%5B%5D=15902964041&idsProdutos%5B%5D=9825963216&idsProdutos%5B%5D=10115473158&idsProdutos%5B%5D=15902963468&idsProdutos%5B%5D=9826188837'

    response = oauth.get(api_url)

    # Verificar se a solicitação foi bem-sucedida
    if response.status_code == 200:
        data = response.json()
    else:
        print('Erro na solicitação à API:', response.status_code, response.text)

    return data

def produtosEstoqueTotal(cursor, meubd, estoque_produtos_api):
    """
    Processa os dados da API e os insere/atualiza no banco de dados.

    Args:
        cursor: Objeto de cursor para interação com o banco de dados.
        meubd: Conexão do banco de dados.
        estoque_produtos_api: Dados obtidos da API Bling.

    Returns:
        None
    """
    # Verificar se a solicitação foi bem-sucedida antes de continuar
    if estoque_produtos_api and 'data' in estoque_produtos_api:
        # Iterar sobre os produtos no estoque
        for produto_data in estoque_produtos_api['data']:
            produto = produto_data.get('produto', {})
            saldo_fisico = produto_data.get('saldoFisicoTotal', 0)
            saldo_virtual = produto_data.get('saldoVirtualTotal', 0)

            produto_id = produto.get('id', None)

            # Inserir ou atualizar dados no banco de dados
            cursor.execute("""
                INSERT INTO ni75bgwvrlo11xf8.estoque_produtos (id, saldoFisicoTotal, saldoVirtualTotal)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                saldoFisicoTotal = VALUES(saldoFisicoTotal),
                saldoVirtualTotal = VALUES(saldoVirtualTotal)
            """, (produto_id, saldo_fisico, saldo_virtual))

            meubd.commit()


#########################################################################
def obter_dados_pedidos(api_key, pagina_inicial=1):
    pagina = pagina_inicial
    dados_pedidos = []

    try:
        while True:
            url = f"https://bling.com.br/Api/v2/pedidos/page={pagina}/json/"
            parametros = {"apikey": api_key}
            resposta = requests.get(url, params=parametros)

            resposta.raise_for_status()  # Lança uma exceção para códigos de status diferentes de 200

            dados = resposta.json()
            pedidos = dados.get("retorno", {}).get("pedidos", [])

            if not pedidos:
                print(f"\n--- Página {pagina} sem pedidos. Encerrando a busca. ---")
                break

            for pedido in pedidos:
                situacao_pedido = pedido.get("pedido", {}).get("situacao", "")
                numero_pedido = pedido.get("pedido", {}).get("numero")

                if numero_pedido is None:
                    return dados_pedidos

                data_pedido = pedido.get("pedido", {}).get("data", "")
                total_venda = pedido.get("pedido", {}).get("totalvenda", "")

                # Cliente
                cliente_info = pedido.get("pedido", {}).get("cliente", {})
                nome = cliente_info.get("nome", "")

                # Verificar se a situação do pedido é "Atendido"
                if situacao_pedido == "Atendido":
                    itens_pedido = pedido.get("pedido", {}).get("itens", [])

                    for item in itens_pedido:
                        descricao = item.get("item", {}).get("descricao")
                        quantidade = float(item.get("item", {}).get("quantidade", 0))
                        valorunidade = float(item.get("item", {}).get("valorunidade", 0))

                        dados_item = {
                            "numero": numero_pedido,
                            "descricao": descricao,
                            "quantidade": quantidade,
                            "valorunidade": valorunidade,
                            "data": data_pedido,
                            "totalvenda": total_venda,
                            "situacao": situacao_pedido,
                            "nome": nome
                        }

                        dados_pedidos.append(dados_item)

            # Incrementa o número da página depois de processar todos os pedidos da página atual
            pagina += 1
            time.sleep(1)

    except requests.exceptions.RequestException as e:
        print(f"Falha na requisição: {e}")

    return dados_pedidos

#################################################################
def inserir_dados_no_banco(dados_pedidos):
    try:
        # Conectar ao banco de dados MySQL
        conexao_bd = mysql.connector.connect(
            host='y6aj3qju8efqj0w1.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
            user='gwc2kpr6extza78g',
            password='eb9tth3ylrt9xx34',
            database='ni75bgwvrlo11xf8',
            autocommit=True  # Adicionado para confirmar alterações automaticamente
        )

        # Criar um cursor para executar comandos SQL
        cursor = conexao_bd.cursor()

        for pedido in dados_pedidos:
            # Verificar se a linha (com base no número) já existe na tabela 'teste'
            cursor.execute("SELECT 1 FROM teste WHERE numero = %s LIMIT 1", (pedido['numero'],))

            if not cursor.fetchone():
                # A linha não existe, realizar a inserção
                sql = """
                INSERT INTO teste (valorunidade, nome_cliente, situacao, totalvenda, quantidade, descricao, data, numero)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """

                valores = (
                    pedido['valorunidade'],
                    pedido['nome'],
                    pedido['situacao'],
                    pedido['totalvenda'],
                    pedido['quantidade'],
                    pedido['descricao'],
                    pedido['data'],
                    pedido['numero']
                )

                cursor.execute(sql, valores)

            else:
                # A linha já existe, não inserir e imprimir mensagem indicando
                pass

        # Ler ou descartar os resultados antes de fechar o cursor
        cursor.fetchall()

        print("Processo de inserção concluído.")

    except mysql.connector.Error as err:
        print(f"Erro de conexão com o banco de dados: {err}")

    finally:
        # Fechar a conexão com o banco de dados
        if 'conexao_bd' in locals() and conexao_bd.is_connected():
            cursor.close()
            conexao_bd.close()


while True:
    # Exemplo de uso:
    api_key = "2351cacfff6b9e084a268fe7323f23f341b0bf3332f1e61bc05c65e95502f8cbdfd9e6f0"
    dados_pedido = obter_dados_pedidos(api_key)
    inserir_dados_no_banco(dados_pedido)

    ######produtos estoques para sgbd

    estoqueProdutos = make_api_request()
    produtosEstoqueTotal(cursor,meubd,estoqueProdutos)

    time.sleep(1200)

