import requests
from datetime import datetime

def get_valores_selic(data_inicial, data_final):
    print(f"get_valores_selic(data_inicial={data_inicial}, data_final={data_final})")

    url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.1178/dados"

    parametros = {
        "dataInicial": data_inicial,
        "dataFinal": data_final
    }

    print(f"Efetuando chamada à API de url: {url}")
    resposta = requests.get(url=url, params=parametros)

    selic_historica = []

    if resposta.status_code == 200:
        print(f"Sucesso - Status code: {resposta.status_code}.")

        temp = resposta.json()

        
        for x in temp:
            data_referencia = str(x["data"])
            data_referencia = datetime.strptime(data_referencia, "%d/%m/%Y")

            str_date = int(data_referencia.strftime("%Y%m%d"))

            item = {
                "_id": str_date,
                "data_referencia": data_referencia,
                "valor": x["valor"],
                "data_atualizacao": datetime.now(),
                "importado_dw": False
            }
            
            selic_historica.append(item)
        print(f"Total de registros recebidos: {len(selic_historica)}.")
    else:
        print(f"Erro - Status code: {resposta.status_code}")
    
    return selic_historica