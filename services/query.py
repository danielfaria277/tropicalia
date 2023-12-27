import mysql.connector
import locale

#TOTAL DE VENDA POR CLIENTE
def obter_total_por_cliente():
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
        cursor = conexao_bd.cursor(dictionary=True)

        # Executar a consulta diretamente no Python
        consulta = """
            SELECT nome_cliente, SUM(totalvenda) AS total_compras
            FROM (
                SELECT DISTINCT numero, nome_cliente, totalvenda
                FROM teste
            ) AS vendas_distintas
            GROUP BY nome_cliente
            ORDER BY total_compras DESC
        """
        cursor.execute(consulta)

        # Recuperar os resultados
        resultados = cursor.fetchall()

        return resultados

    except mysql.connector.Error as err:
        print(f"Erro de conexão com o banco de dados: {err}")
        return None

    finally:
        # Fechar a conexão com o banco de dados
        if 'conexao_bd' in locals() and conexao_bd.is_connected():
            cursor.close()
            conexao_bd.close()
##################################################

#TOTAL VENDA POR PRODUTO
def obter_vendas_por_produto():
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
        cursor = conexao_bd.cursor(dictionary=True)

        # Consulta para obter vendas por produto (organizado por valor total em ordem decrescente)
        consulta_produto = """
            SELECT descricao, SUM(quantidade * valorunidade) AS total_valor
            FROM teste
            WHERE descricao IN (
                'Kombucha de Hibisco e Maracuja', 'Kombucha de Uva Rose com Zimbro',
                'Kombucha de Gengibre com Limão', 'Kombucha de Melancia',
                'Kombucha de Frutas Citricas com Curcuma',
                'KIT SCOBY DE KOMBUCHA DESCEDENCIA ALEMÃ', 'KIT SCOBY DE KOMBUCHA DESCEDENCIA PORTUGUESA'
            )
            GROUP BY descricao
            ORDER BY total_valor DESC
        """
        cursor.execute(consulta_produto)

        # Recuperar os resultados
        resultados_produto = cursor.fetchall()

        return resultados_produto

    except mysql.connector.Error as err:
        print(f"Erro de conexão com o banco de dados: {err}")
        return None

    finally:
        # Fechar a conexão com o banco de dados
        if 'conexao_bd' in locals() and conexao_bd.is_connected():
            cursor.close()
            conexao_bd.close()

#################################################
#########################################
        #QYERY ESTOQE PRODUTOS#
def consultar_estoque_produtos():
    resultados = []  # Lista para armazenar os resultados

    try:
        meu_bd = mysql.connector.connect(
            host='y6aj3qju8efqj0w1.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
            user='gwc2kpr6extza78g',
            password='eb9tth3ylrt9xx34',
            database='ni75bgwvrlo11xf8'
        )
        cursor = meu_bd.cursor(dictionary=True)

        # IDs na ordem desejada
        ids_ordem_desejada = ['9826076834', '15902964041', '9825963216', '10115473158', '15902963468', '9826188837']

        # Montar a consulta SQL com a cláusula ORDER BY
        consulta_sql = f"SELECT id, saldoFisicoTotal FROM estoque_produtos WHERE id IN ({','.join(['%s']*len(ids_ordem_desejada))}) ORDER BY FIELD(id, {','.join(ids_ordem_desejada)})"

        cursor.execute(consulta_sql, ids_ordem_desejada)
        resultados = cursor.fetchall()

    except mysql.connector.Error as err:
        print(f"Erro ao consultar o estoque de produtos: {err}")

    finally:
        cursor.close()
        meu_bd.close()

    return resultados

##################################################
#FATURAMENTO TOTAL
# Defina a localidade para o Brasil

def obter_total_geral():
    try:
        # Conectar ao banco de dados MySQL
        conexao_bd = mysql.connector.connect(
            host='y6aj3qju8efqj0w1.cbetxkdyhwsb.us-east-1.rds.amazonaws.com',
            user='gwc2kpr6extza78g',
            password='eb9tth3ylrt9xx34',
            database='ni75bgwvrlo11xf8',
            autocommit=True
        )

        # Criar um cursor para executar comandos SQL
        cursor = conexao_bd.cursor()

        # Executar a consulta para obter a soma total da coluna totalvenda, usando MAX para pegar um valor por número
        consulta = "SELECT numero, MAX(totalvenda) AS total_por_numero FROM teste GROUP BY numero"
        cursor.execute(consulta)

        # Recuperar os resultados
        resultados = cursor.fetchall()

        # Somar os resultados
        total_geral = sum(resultado[1] for resultado in resultados)

        # Formatar o resultado como um número real no estilo "203.298,98"
        total_formatado = "{:,.2f}".format(total_geral).replace(',', 'temp').replace('.', ',').replace('temp', '.')

        return total_formatado

    except mysql.connector.Error as err:
        print(f"Erro de conexão com o banco de dados: {err}")

    finally:
        # Fechar a conexão com o banco de dados
        if 'conexao_bd' in locals() and conexao_bd.is_connected():
            cursor.close()
            conexao_bd.close()

##################################################
# Chamar a função
totalPorCliente = obter_total_por_cliente()
vendasPorProduto = obter_vendas_por_produto()
estoqueProdutos = consultar_estoque_produtos()
faturamentoTotal = obter_total_geral()
print(faturamentoTotal)


