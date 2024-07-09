import requests
from datetime import datetime

def get_cotacoes_moeda(simbolo_moeda, data_inicial, data_final):
    print(f"Método chamado: get_cotacoes_moeda(simbolo_moeda='{simbolo_moeda}', data_inicial='{data_inicial}', data_final='{data_final}')")

    url_base = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"

    url_cotacoes = f'{url_base}/CotacaoMoedaPeriodo(moeda=@moeda,dataInicial=@dataInicial,dataFinalCotacao=@dataFinalCotacao)'

    parametros = {
        "@moeda": f"'{simbolo_moeda}'",
        "@dataInicial": f"'{data_inicial}'",
        "@dataFinalCotacao": f"'{data_final}'"
    }

    print(f"Efetuando chamada à API de url: {url_cotacoes}")
    resposta = requests.get(url=url_cotacoes, params=parametros)

    lista_cotacoes = []

    if resposta.status_code == 200:
        print(f"Sucesso - Status code: {resposta.status_code}.")
        temp = resposta.json()["value"]

        for x in temp:
            item = {
                "_id": f"{simbolo_moeda} | {x['tipoBoletim']} | {x['dataHoraCotacao']}",
                "simbolo_moeda": simbolo_moeda,
                "data_referencia": x["dataHoraCotacao"],
                "cotacao_compra": x["cotacaoCompra"],
                "cotacao_venda": x["cotacaoVenda"],
                "paridade_compra": x["paridadeCompra"],
                "paridade_venda": x["paridadeVenda"],
                "tipo_boletim": x["tipoBoletim"],
                "data_atualizacao": datetime.now(),
                "importado_dw": False
            }

            lista_cotacoes.append(item)
        print(f"Total de registros recebidos: {len(lista_cotacoes)}.")
    else:
        print(f"Erro - Status code: {resposta.status_code}")

    return lista_cotacoes