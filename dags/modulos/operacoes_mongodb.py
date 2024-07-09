import pymongo

def salvar_documentos(string_conexao, banco, colecao, lista_documentos):
    print(f"Método chamado: salvar_documentos(string_conexao=*****, banco={banco}, db_colecao={colecao}, lista_documentos=*****")

    resultado = []

    lista_documentos = list(lista_documentos)
    
    if len(lista_documentos) > 0:
        cliente = pymongo.MongoClient(string_conexao)
        db = cliente[banco]
        db_colecao = db[colecao]

        op = [pymongo.UpdateOne({'_id': doc['_id']}, {'$set': doc}, upsert=True) for doc in lista_documentos]

        resultado = db_colecao.bulk_write(op)

        qtde_alteracoes = resultado.upserted_count+resultado.inserted_count+resultado.modified_count

        print(f"Resultado Upsert: {qtde_alteracoes}")
    else:
        print("Lista recebida está vazia!")

    return resultado

def buscar_documentos(string_conexao, banco, colecao, query):
    print(f"Método chamado: buscar_documentos(string_conexao=*****, banco={banco}, db_colecao={colecao}, query=*****")
    
    cliente = pymongo.MongoClient(string_conexao)
    db = cliente[banco]
    db_colecao = db[colecao]

    lista_documentos = db_colecao.find(query)

    return lista_documentos

def buscar_documentos_paginados(string_conexao, banco, colecao, query, skip, limit):
    print(f"Método chamado: buscar_documentos_paginados(string_conexao=*****, banco={banco}, db_colecao={colecao}, query=*****, skip={skip}, limit={limit}")
    
    cliente = pymongo.MongoClient(string_conexao)
    db = cliente[banco]
    db_colecao = db[colecao]

    lista_documentos = db_colecao.find(query).skip(skip).limit(limit)

    return lista_documentos

def buscar_com_aggregate(string_conexao, banco, colecao, pipeline):
    print(f"Método chamado: buscar_com_aggregate(string_conexao=*****, banco={banco}, db_colecao={colecao}, pipeline=*****")
    
    cliente = pymongo.MongoClient(string_conexao)
    db = cliente[banco]
    db_colecao = db[colecao]

    retorno = db_colecao.aggregate(pipeline)

    return retorno

def contagem_total_documentos(string_conexao, banco, colecao, query):
    cliente = pymongo.MongoClient(string_conexao)
    db = cliente[banco]
    db_colecao = db[colecao]

    qtde_documentos = db_colecao.count_documents(query)

    return qtde_documentos

def atualizar_documentos(string_conexao, banco, colecao, query, update):
    cliente = pymongo.MongoClient(string_conexao)
    db = cliente[banco]
    db_colecao = db[colecao]

    resultado = db_colecao.update_many(query,update)

    print(f"Quantidade alterados: {resultado.modified_count}")

    return resultado