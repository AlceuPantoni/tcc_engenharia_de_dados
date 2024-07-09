import requests
from datetime import datetime

def get_moedas():
    print("Método chamado: get_moedas()")

    url_base = "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata"

    url_moedas = f'{url_base}/Moedas'

    print(f"Efetuando chamada à API de url: {url_moedas}")
    resposta = requests.get(url=url_moedas)

    lista_moedas = []

    if resposta.status_code == 200:
        print(f"Sucesso - Status code: {resposta.status_code}.")
        
        lista_moedas_temp = resposta.json()["value"]

        for x in lista_moedas_temp:
            item = {
                "_id": x["simbolo"],
                "nome_moeda": x["nomeFormatado"],
                "data_atualizacao": datetime.now(),
                "importado_dw": False
            }

            lista_moedas.append(item)
        print(f"Total de registros recebidos: {len(lista_moedas)}.")
    else:
        print(f"Erro - Status code: {resposta.status_code}")
    
    return lista_moedas