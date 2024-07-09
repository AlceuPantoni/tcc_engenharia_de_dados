import modulos.financas_stg as f_stg
import modulos.operacoes_mongodb as op
import pendulum

from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

str_con_mongo = Variable.get(key="string_conn_mongodb")
destinatario_email = Variable.get(key="destinatario_email")

def enviar_email_inicio():
    assunto = "Airflow: DAG Finanças ETL iniciada"
    corpo_email = f"DAG Finanças ETL foi iniciada às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def enviar_email_conclusao():
    assunto = "Airflow: DAG Finanças ETL concluída"
    corpo_email = f"DAG Finanças ETL foi concluída às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def executar_proc_dw_dim_moedas():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_dim_moedas();")

def executar_proc_dw_dim_moedas_cotacoes():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_dim_moedas_cotacoes();")

def executar_proc_dw_dim_ibov_cotacoes():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_dim_ibov_cotacoes();")

def executar_proc_dw_fato_financeiro():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_fato_financeiro();")

def criar_tabelas_financas_stg():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conexao = pg_hook.get_conn()
    cursor = conexao.cursor()

    lista_comandos_criar_tabelas_stg = f_stg.comandos_criar_tabelas_stg()

    for create_table in lista_comandos_criar_tabelas_stg:
        cursor.execute(create_table)

    conexao.commit()
    cursor.close()
    conexao.close()
    
def inserir_moedas_stg():
    query_moedas = { "importado_dw": False }
    
    lista_moedas = op.buscar_documentos(string_conexao=str_con_mongo,banco="raw_data",
                                        colecao="moedas",query=query_moedas)
    
    lista_moedas = list(lista_moedas)
    qtde_registros = len(lista_moedas)
    
    if qtde_registros > 0:
        print(f"Serão inseridos na 'stg.tb_moedas' {qtde_registros} registros.")

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conexao = pg_hook.get_conn()
        cursor = conexao.cursor()

        pg_hook.run(sql="TRUNCATE TABLE stg.tb_moedas;", autocommit=True)

        comando_insert = "INSERT INTO stg.tb_moedas (id_moeda, nome_moeda) VALUES(%s, %s);"
        
        for i in range(qtde_registros):
            id_moeda = lista_moedas[i]["_id"]
            nome_moeda = lista_moedas[i]["nome_moeda"]

            lista_moedas[i]["importado_dw"] = True

            linha = (id_moeda, nome_moeda)

            pg_hook.run(sql=comando_insert, parameters=linha)

            print(f"Inserido registro {i} de {qtde_registros}.")

        conexao.commit()
        cursor.close()
        conexao.close()

        op.salvar_documentos(string_conexao=str_con_mongo, banco="raw_data", colecao="moedas",
                         lista_documentos=lista_moedas)
    else:
        print("Não há novos documentos 'moedas' a serem integrados.")

def inserir_moedas_cotacoes_stg():
    query_moedas_cotacoes = { "importado_dw": False }
    
    lista_moedas_cotacoes_temp = op.buscar_documentos(string_conexao=str_con_mongo,banco="raw_data",
                                        colecao="moedas_cotacoes",query=query_moedas_cotacoes)
    
    lista_moedas_cotacoes = []
    lista_atualizar_mongo = []

    for reg in lista_moedas_cotacoes_temp:
        doc = {
                "_id": reg["_id"],
                "simbolo_moeda": reg["simbolo_moeda"],
                "data_referencia": reg["data_referencia"],
                "cotacao_compra": reg["cotacao_compra"],
                "cotacao_venda": reg["cotacao_venda"],
                "paridade_compra": reg["paridade_compra"],
                "paridade_venda": reg["paridade_venda"],
                "tipo_boletim": reg["tipo_boletim"],
                "data_atualizacao": reg["data_atualizacao"],
                "importado_dw": True
            }
        lista_atualizar_mongo.append(doc)

        temp_data_ref = datetime.strptime(reg["data_referencia"][0:19], '%Y-%m-%d %H:%M:%S')
        temp_data_ref = datetime.strftime(temp_data_ref,"%Y-%m-%d %H:%M:%S")
        item = {
            "id_cotacao": reg["_id"],
            "id_moeda": reg["simbolo_moeda"],
            "tipo_boletim": reg["tipo_boletim"],
            "data_referencia": temp_data_ref,
            "cotacao_compra": reg["cotacao_compra"],
            "cotacao_venda": reg["cotacao_venda"],
            "paridade_compra": reg["paridade_compra"],
            "paridade_venda": reg["paridade_venda"]
        }
        lista_moedas_cotacoes.append(item)

    qtde_registros = len(lista_moedas_cotacoes)
    
    if qtde_registros > 0:
        print(f"Serão inseridos na 'stg.tb_moedas_cotacoes' {qtde_registros} registros.")

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conexao = pg_hook.get_conn()
        cursor = conexao.cursor()

        pg_hook.run(sql="TRUNCATE TABLE stg.tb_moedas_cotacoes;", autocommit=True)
        
        tamanho_lote = 100000

        sql = """
            INSERT INTO stg.tb_moedas_cotacoes (id_cotacao, id_moeda, tipo_boletim, data_referencia, cotacao_compra, cotacao_venda, paridade_compra, paridade_venda)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for i in range(0, qtde_registros, tamanho_lote):
            lote = lista_moedas_cotacoes[i:i+tamanho_lote]

            lote_formatado = [[
                record['id_cotacao'], record['id_moeda'], record['tipo_boletim'],
                record['data_referencia'],record['cotacao_compra'], record['cotacao_venda'],
                record['paridade_compra'], record['paridade_venda']
            ] for record in lote]

            cursor.executemany(sql, lote_formatado)

            print(f"Registros inseridos: {i+len(lote_formatado)} de {qtde_registros}.")

        conexao.commit()
        cursor.close()
        conexao.close()

        op.salvar_documentos(string_conexao=str_con_mongo, banco="raw_data",
                             colecao="moedas_cotacoes",lista_documentos=lista_atualizar_mongo)
    else:
        print("Não há novos documentos 'moedas_cotacoes' a serem integrados.")

def inserir_dados_selic_stg():
    query_selic = { "importado_dw": False }
    
    lista_selic_temp = op.buscar_documentos(string_conexao=str_con_mongo,banco="raw_data",
                                        colecao="selic",query=query_selic)
    
    lista_selic = []
    lista_atualizar_mongo = []

    for reg in lista_selic_temp:
        doc = {
                "_id": reg["_id"],
                "data_referencia": reg["data_referencia"],
                "valor": reg["valor"],
                "data_atualizacao": reg["data_atualizacao"],
                "importado_dw": True
            }
        lista_atualizar_mongo.append(doc)

        temp_data_ref = datetime.strftime(reg["data_referencia"],"%Y-%m-%d %H:%M:%S")
        item = {
            "id_selic": reg["_id"],
            "data_referencia": temp_data_ref,
            "valor_selic": reg["valor"]
        }
        lista_selic.append(item)

    qtde_registros = len(lista_selic)
    
    if qtde_registros > 0:
        print(f"Serão inseridos na 'stg.tb_selic' {qtde_registros} registros.")

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conexao = pg_hook.get_conn()
        cursor = conexao.cursor()

        pg_hook.run(sql="TRUNCATE TABLE stg.tb_selic;", autocommit=True)
        
        tamanho_lote = 100000

        sql = """
            INSERT INTO stg.tb_selic (id_selic,data_referencia,valor_selic)
            VALUES (%s, %s, %s)
        """

        for i in range(0, qtde_registros, tamanho_lote):
            lote = lista_selic[i:i+tamanho_lote]

            lote_formatado = [[
                record['id_selic'], record['data_referencia'], record['valor_selic']
            ] for record in lote]

            cursor.executemany(sql, lote_formatado)

            print(f"Registros inseridos: {i+len(lote_formatado)} de {qtde_registros}.")

        conexao.commit()
        cursor.close()
        conexao.close()

        op.salvar_documentos(string_conexao=str_con_mongo, banco="raw_data",
                             colecao="selic",lista_documentos=lista_atualizar_mongo)
    else:
        print("Não há novos documentos 'selic' a serem integrados.")

def inserir_ibov_cotacoes_stg():
    query_ibov_cotacoes = { "importado_dw": False }
    
    lista_ibov_cotacoes_temp = op.buscar_documentos(string_conexao=str_con_mongo,banco="raw_data",
                                        colecao="ibov_cotacoes",query=query_ibov_cotacoes)
    
    lista_ibov_cotacoes = []
    lista_atualizar_mongo = []

    for reg in lista_ibov_cotacoes_temp:
        doc = {
                "_id": reg["_id"],
                "abertura": reg["abertura"],
                "data_atualizacao": reg["data_atualizacao"],
                "data_referencia": reg["data_referencia"],
                "fechamento": reg["fechamento"],
                "fechamento_ajustado": reg["fechamento_ajustado"],
                "maxima": reg["maxima"],
                "minima": reg["minima"],
                "volume": reg["volume"],
                "importado_dw": True
            }
        lista_atualizar_mongo.append(doc)

        temp_data_ref = datetime.strftime(reg["data_referencia"],"%Y-%m-%d %H:%M:%S")
        item = {
            "id_cotacao": reg["_id"],
            "data_referencia": temp_data_ref,
            "valor_cotacao_abertura": reg["abertura"],
            "valor_cotacao_fechamento": reg["fechamento"],
            "valor_cotacao_fechamento_ajustado": reg["fechamento_ajustado"],
            "valor_cotacao_maxima": reg["maxima"],
            "valor_cotacao_minima": reg["minima"],
            "volume_total_transacionado": reg["volume"]
        }
        lista_ibov_cotacoes.append(item)

    qtde_registros = len(lista_ibov_cotacoes)
    
    if qtde_registros > 0:
        print(f"Serão inseridos na 'stg.tb_ibov_cotacoes' {qtde_registros} registros.")

        pg_hook = PostgresHook(postgres_conn_id='postgres')
        conexao = pg_hook.get_conn()
        cursor = conexao.cursor()

        pg_hook.run(sql="TRUNCATE TABLE stg.tb_ibov_cotacoes;", autocommit=True)
        
        tamanho_lote = 100000

        sql = """
            INSERT INTO stg.tb_ibov_cotacoes (id_cotacao,data_referencia,valor_cotacao_abertura,valor_cotacao_fechamento,valor_cotacao_fechamento_ajustado,valor_cotacao_maxima,valor_cotacao_minima,volume_total_transacionado)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for i in range(0, qtde_registros, tamanho_lote):
            lote = lista_ibov_cotacoes[i:i+tamanho_lote]

            lote_formatado = [[
                record['id_cotacao'], record['data_referencia'], record['valor_cotacao_abertura'],
                record['valor_cotacao_fechamento'],record['valor_cotacao_fechamento_ajustado'],
                record['valor_cotacao_maxima'],record['valor_cotacao_minima'],
                record['volume_total_transacionado']
            ] for record in lote]

            cursor.executemany(sql, lote_formatado)

            print(f"Registros inseridos: {i+len(lote_formatado)} de {qtde_registros}.")

        conexao.commit()
        cursor.close()
        conexao.close()

        op.salvar_documentos(string_conexao=str_con_mongo, banco="raw_data",
                             colecao="ibov_cotacoes",lista_documentos=lista_atualizar_mongo)
    else:
        print("Não há novos documentos 'ibov_cotacoes' a serem integrados.")

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024,4,10,22,0,0,0, tz="America/Sao_Paulo"),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    "email": ["alceu.pantoni@gmail.com"],
    "email_on_failure": True
}

with DAG(
    dag_id = "dag_financas_etl",
    description = "Carrega os dados de finanças para stg, tratá-os e carrega-os no dw.",
    schedule_interval = None,
    default_args = default_args,
    catchup = False
) as dag:
    
    task_enviar_email_inicio = PythonOperator(
        task_id = "tsk_enviar_email_inicio",
        python_callable = enviar_email_inicio
    )
    
    inicio = EmptyOperator(task_id="financas_etl_inicio")
    fim = EmptyOperator(task_id="financas_etl_fim")

    dummy_ok = EmptyOperator(task_id="financas_dummy_ok")

    task_criar_tabelas_financas_stg = PythonOperator(
        task_id = "tsk_criar_tabelas_financas_stg",
        python_callable = criar_tabelas_financas_stg
    )

    with TaskGroup(
        group_id="grp_financas_stg",
        tooltip="Grupo Finanças STG"
    ) as grp_financas_stg:

        task_inserir_moedas_stg = PythonOperator(
            task_id = "tsk_inserir_moedas_stg",
            python_callable = inserir_moedas_stg
        )

        task_inserir_moedas_cotacoes_stg = PythonOperator(
            task_id = "tsk_inserir_moedas_cotacoes_stg",
            python_callable = inserir_moedas_cotacoes_stg
        )

        task_inserir_dados_selic_stg = PythonOperator(
            task_id = "tsk_inserir_dados_selic_stg",
            python_callable = inserir_dados_selic_stg
        )

        task_inserir_ibov_cotacoes_stg = PythonOperator(
            task_id = "tsk_inserir_ibov_cotacoes_stg",
            python_callable = inserir_ibov_cotacoes_stg
        )

    with TaskGroup(
        group_id="grp_financas_dw",
        tooltip="Grupo Finanças DW (procedures)"
    ) as grp_financas_dw:

        task_proc_carregar_dim_moedas = PythonOperator(
            task_id="tsk_proc_carregar_dim_moedas",
            python_callable = executar_proc_dw_dim_moedas
        )

        task_proc_carregar_dim_moedas_cotacoes = PythonOperator(
            task_id="tsk_proc_carregar_dim_moedas_cotacoes",
            python_callable = executar_proc_dw_dim_moedas_cotacoes
        )

        task_proc_carregar_dim_ibov_cotacoes = PythonOperator(
            task_id="tsk_proc_carregar_dim_ibov_cotacoes",
            python_callable = executar_proc_dw_dim_ibov_cotacoes
        )

        task_proc_carregar_fato_financeiro = PythonOperator(
            task_id="tsk_proc_carregar_fato_financeiro",
            python_callable = executar_proc_dw_fato_financeiro
        )

    task_enviar_email_conclusao = PythonOperator(
        task_id = "tsk_enviar_email_conclusao",
        python_callable = enviar_email_conclusao,
        trigger_rule = "all_success"
    )

inicio >> task_enviar_email_inicio >> task_criar_tabelas_financas_stg >> grp_financas_stg >> dummy_ok >> grp_financas_dw >> task_enviar_email_conclusao >> fim