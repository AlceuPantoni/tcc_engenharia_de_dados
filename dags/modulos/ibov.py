import pandas as pd
import yfinance as yf
from datetime import datetime

def get_cotacoes_ibov(data_inicial, data_final):
    print(f"get_cotacoes_ibov(data_inicial={data_inicial}, data_final={data_final})")

    ticker = "^BVSP"
    data_inicial = data_inicial
    data_final = data_final

    print(f"Fazendo chamada para o ticker {ticker}")
    ibov = yf.download(tickers=ticker, start=data_inicial, end=data_final)

    ibov = pd.DataFrame(ibov)

    print("Chamada concluída. Ajustando dados recebidos.")
    ibov = ibov.rename(columns={"Open":"abertura",
        "High":"maxima",
        "Low":"minima",
        "Close":"fechamento",
        "Adj Close":"fechamento_ajustado",
        "Volume":"volume"})

    ibov["data_referencia"] = pd.to_datetime(ibov.index).date

    ibov["data_referencia"] = pd.to_datetime(ibov["data_referencia"]).dt.normalize()

    ibov["_id"] = pd.to_datetime(ibov["data_referencia"]).dt.strftime("%Y%m%d").apply(int)

    ibov["data_atualizacao"] = datetime.now()

    ibov["importado_dw"] = False

    ibov_json = ibov.to_dict(orient="records")

    print(f"Total de registros recebidos: {len(ibov_json)}.")

    lista_dados_ibov = []
    for x in ibov_json:
        item = {
            "_id": x["_id"],
            "data_referencia": x["data_referencia"],
            "abertura": x["abertura"],
            "fechamento": x["fechamento"],
            "fechamento_ajustado": x["fechamento_ajustado"],
            "maxima": x["maxima"],
            "minima": x["minima"],
            "volume": x["volume"],
            "data_atualizacao": x["data_atualizacao"],
            "importado_dw": x["importado_dw"]
        }
        lista_dados_ibov.append(item)

    return lista_dados_ibov