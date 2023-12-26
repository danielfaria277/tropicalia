import requests
import time
import mysql.connector

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

        print("Processo de inserção concluído.")

    except mysql.connector.Error as err:
        print(f"Erro de conexão com o banco de dados: {err}")

    finally:
        # Fechar a conexão com o banco de dados
        if 'conexao_bd' in locals() and conexao_bd.is_connected():
            cursor.close()
            conexao_bd.close()

# Exemplo de uso:
api_key = "2351cacfff6b9e084a268fe7323f23f341b0bf3332f1e61bc05c65e95502f8cbdfd9e6f0"
dados_pedido = obter_dados_pedidos(api_key)
inserir_dados_no_banco(dados_pedido)
