from flask import Flask, render_template
from services.query import obter_total_por_cliente, obter_vendas_por_produto,consultar_estoque_produtos,obter_total_geral


#Criar instancia flask
app = Flask(__name__)

totalPorCliente = obter_total_por_cliente()
vendasPorProduto = obter_vendas_por_produto()
estoqueProdutos = consultar_estoque_produtos()
faturamentoTotal = obter_total_geral()


#Criar rota decorator
@app.route('/')
def index():
    print(faturamentoTotal)
    return render_template("index.html",
                           totalPorCliente = totalPorCliente,
                           vendasPorProduto = vendasPorProduto,
                           estoqueProdutos = estoqueProdutos,
                           faturamentoTotal = faturamentoTotal
                           )
                           

