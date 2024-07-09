import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from datetime import datetime

def __ajustar_nomes_campos(lista):
    lista_ajustada = []
    
    for x in lista:
        item = {
            "_id": x["_id"],
            "data_referencia": x["_source.vacina_dataAplicacao"],
            "estabelecimento_municipio_codigo": x["_source.estabelecimento_municipio_codigo"],
            "estabelecimento_municipio_nome": x["_source.estabelecimento_municipio_nome"],
            "estabelecimento_razao_social": x["_source.estabelecimento_razaoSocial"],
            "estabelecimento_uf": x["_source.estabelecimento_uf"],
            "estabelecimento_codigo": x["_source.estabelecimento_valor"],
            "estabelecimento_nome": x["_source.estalecimento_noFantasia"],
            "paciente_id": x["_source.paciente_id"],
            "paciente_data_nascimento": x["_source.paciente_dataNascimento"],
            "paciente_endereco_cep": x["_source.paciente_endereco_cep"],
            "paciente_endereco_municipio": x["_source.paciente_endereco_nmMunicipio"],
            "paciente_endereco_pais": x["_source.paciente_endereco_nmPais"],
            "paciente_endereco_uf": x["_source.paciente_endereco_uf"],
            "paciente_sexo": x["_source.paciente_enumSexoBiologico"],
            "paciente_idade": x["_source.paciente_idade"],
            "paciente_raca_codigo": x["_source.paciente_racaCor_codigo"],
            "paciente_raca_descricao": x["_source.paciente_racaCor_valor"],
            "vacina_data_aplicacao": x["_source.vacina_dataAplicacao"],
            "vacina_descricao_dose": x["_source.vacina_descricao_dose"],
            "vacina_fabricante": x["_source.vacina_fabricante_nome"],
            "vacina_lote": x["_source.vacina_lote"],
            "vacina_nome": x["_source.vacina_nome"],
            "sistema_origem": x["_source.sistema_origem"],
            "data_atualizacao": x["data_atualizacao"],
            "importado_dw": x["importado_dw"]
        }
        lista_ajustada.append(item)

    print("Nomes dos campos ajustados.")

    return lista_ajustada

def get_dados_vacina(data_inicial, data_final, uf, cidade):
    print(f"Método chamado: get_dados_vacina(data_inicial='{data_inicial}', data_final='{data_final}', uf={uf}, cidade={cidade})")

    url = "https://imunizacao-es.saude.gov.br"

    nome_usuario = "imunizacao_public"
    senha = "qlto5t&7r_@+#Tlstigi"
    timeout = "15m"
    quantidade_registros = 10000

    dados_vacinas = []

    try:
        headers = {'Content-Type': 'application/json'}
        
        auth=HTTPBasicAuth(username=nome_usuario,password=senha)
        
        body = {
            "size": quantidade_registros,
            "query": {
                "bool":{
                    "filter": {
                        "range": {
                            "vacina_dataAplicacao": {
                                "gte": data_inicial+"T00:00:00.000Z",
                                "lte": data_final+"T23:59:59.999Z"
                            }
                        }
                    },
                    "must":[
                        {
                            "term": {
                                "estabelecimento_uf": uf
                            } 
                        },
                        {
                            "term": {
                                "estabelecimento_municipio_nome" : cidade
                            }
                        }
                    ]
                }
            }
        }
        
        print(f"Efetuando chamada à API (get) de url: {url}")
        resposta = requests.get(url + "/_search?scroll=" + timeout, headers=headers, auth=auth, json=body)
        print(f"Sucesso - Status code: {resposta.status_code}.")
        
        resposta.raise_for_status()
        
        scroll_id = resposta.json()["_scroll_id"]

        dados = resposta.json()

        df = pd.json_normalize(dados["hits"]["hits"])

        df_temp = df
        
        loops = 1
        while True:
            print(f'Loop Vacinas nº {loops}.')
            
            print(f"Efetuando chamada à API (get) de url: {url}")
            resposta = requests.post(url + "/_search/scroll", headers=headers, auth=auth, json={"scroll": timeout, "scroll_id": scroll_id})
            print(f"Sucesso - Status code: {resposta.status_code}.")
            resposta.raise_for_status()

            if len(resposta.json()["hits"]["hits"]) == 0:
                break
            
            scroll_id = resposta.json()["_scroll_id"]

            dados_scroll = resposta.json()

            df_temp = pd.json_normalize(dados_scroll["hits"]["hits"])
            df = pd.concat([df, df_temp])

            loops += 1
        
        requests.delete(url + "/_search/scroll", headers=headers, auth=auth, json={"scroll_id": [scroll_id]})
        
        print("Finalizado!\n\n")
        
        if not df.empty:
            df["data_atualizacao"] = datetime.now()
            df["importado_dw"] = False

            dados_vacinas = df.to_dict(orient="records")
            dados_vacinas = __ajustar_nomes_campos(dados_vacinas)

        print(f"Total de registros recebidos: {len(dados_vacinas)}.")

        return dados_vacinas
    
    except requests.exceptions.RequestException as e:
        print("Erro ao fazer solicitação HTTP:", e)
        return dados_vacinas