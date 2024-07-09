import modulos.moedas as m
import modulos.moedas_cotacoes as mc
import modulos.selic as s
import modulos.ibov as i
import modulos.operacoes_mongodb as op
import pendulum

from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email

str_con_mongo = Variable.get(key="string_conn_mongodb")
carga_completa = Variable.get(key="carga_completa")
destinatario_email = Variable.get(key="destinatario_email")
carga_completa = True if carga_completa == "True" else False

banco_raw = "raw_data"

def enviar_email_inicio():
    assunto = "Airflow: DAG Finanças iniciada"
    corpo_email = f"DAG Finanças foi iniciada às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def enviar_email_conclusao():
    assunto = "Airflow: DAG Finanças concluída"
    corpo_email = f"DAG Finanças foi concluída às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def receber_moedas():
    lista_moedas = m.get_moedas()
    
    op.salvar_documentos(string_conexao=str_con_mongo, banco=banco_raw,
                         colecao="moedas", lista_documentos=lista_moedas)

def receber_cotacoes_moedas():
    pipeline_moedas = [ {"$group": { "_id": "$_id" } } ]

    retorno_moedas = op.buscar_com_aggregate(string_conexao=str_con_mongo,
                                             banco=banco_raw, colecao="moedas",
                                             pipeline=pipeline_moedas)

    lista_moedas = []
    for item in retorno_moedas:
        lista_moedas.append(item["_id"])

    for moeda in lista_moedas:
        pipeline_data = [
            { "$match": { "simbolo_moeda": moeda } },
            { "$group": { "_id": 0, "max_data": { "$max": "$data_referencia"} } }
        ]

        retorno_data = op.buscar_com_aggregate(string_conexao=str_con_mongo,
                                               banco=banco_raw, colecao="moedas_cotacoes",
                                               pipeline=pipeline_data)
        
        retorno_data = list(retorno_data)

        data_inicial = "07-01-1994"

        if len(retorno_data) > 0 and not carga_completa: 
            lista_datas = []
            for item in retorno_data:
                lista_datas.append(item["max_data"])

            data_inicial = lista_datas[0][5:8]+lista_datas[0][8:10]+"-"+lista_datas[0][:4]    
        
        data_final = datetime.now().strftime('%m-%d-%Y')
        cotacoes = mc.get_cotacoes_moeda(simbolo_moeda=moeda, data_inicial=data_inicial,
                                        data_final=data_final)
        
        op.salvar_documentos(string_conexao=str_con_mongo, banco=banco_raw,
                             colecao="moedas_cotacoes", lista_documentos=cotacoes)
        
def receber_dados_selic():
    pipeline = [
        { "$group": { "_id": 0, "max_data": { "$max": "$data_referencia"} } }
    ]

    retorno_data = op.buscar_com_aggregate(string_conexao=str_con_mongo,
                                            banco=banco_raw, colecao="selic",
                                            pipeline=pipeline)
    
    retorno_data = list(retorno_data)

    data_inicial = "01/07/1994"

    if len(retorno_data) > 0 and not carga_completa: 
        lista_datas = []
        for item in retorno_data:
            lista_datas.append(datetime.date(item["max_data"]))

        data_inicial = lista_datas[0].strftime('%d/%m/%Y')
    
    data_final = datetime.now().strftime('%d/%m/%Y')
    selic = s.get_valores_selic(data_inicial=data_inicial,data_final=data_final)
    
    op.salvar_documentos(string_conexao=str_con_mongo, banco=banco_raw,
                            colecao="selic", lista_documentos=selic)
    
def receber_dados_ibov():
    pipeline = [
        { "$group": { "_id": 0, "max_data": { "$max": "$data_referencia"} } }
    ]

    retorno_data = op.buscar_com_aggregate(string_conexao=str_con_mongo,
                                            banco=banco_raw, colecao="ibov_cotacoes",
                                            pipeline=pipeline)
    
    retorno_data = list(retorno_data)

    data_inicial = "1994-07-01"

    if len(retorno_data) > 0 and not carga_completa: 
        lista_datas = []
        for item in retorno_data:
            lista_datas.append(datetime.date(item["max_data"]))

        data_inicial = lista_datas[0].strftime('%Y-%m-%d')
    
    data_final = datetime.now().strftime('%Y-%m-%d')
    ibov = i.get_cotacoes_ibov(data_inicial=data_inicial,data_final=data_final)
    
    op.salvar_documentos(string_conexao=str_con_mongo, banco=banco_raw,
                            colecao="ibov_cotacoes", lista_documentos=ibov)

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024,4,10,22,0,0,0, tz="America/Sao_Paulo"),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    "email": ["alceu.pantoni@gmail.com"],
    "email_on_failure": True
}

with DAG(
    dag_id = "dag_financas",
    description = "Receber dados de moedas e suas cotações, taxa Selic e índice Ibov",
    schedule_interval = "30 22 * * *",
    default_args = default_args,
    catchup = False
) as dag:
    
    task_enviar_email_inicio = PythonOperator(
        task_id = "tsk_enviar_email_inicio",
        python_callable = enviar_email_inicio
    )
    
    inicio = EmptyOperator(task_id="financas_inicio")
    fim = EmptyOperator(task_id="financas_fim")

    with TaskGroup(
        group_id="grp_moedas",
        tooltip="Grupo moedas e cotações"
    ) as grp_moedas:

        task_receber_moedas = PythonOperator(
            task_id = "tsk_receber_moedas",
            python_callable = receber_moedas
        )

        task_receber_cotacoes_moedas = PythonOperator(
            task_id = "tsk_receber_cotacoes_moedas",
            python_callable = receber_cotacoes_moedas
        )

    task_receber_dados_selic = PythonOperator(
        task_id = "tsk_receber_dados_selic",
        python_callable = receber_dados_selic
    )

    task_receber_cotacoes_ibov = PythonOperator(
        task_id = "tsk_receber_cotacoes_ibov",
        python_callable = receber_dados_ibov
    )

    task_trigger_dag_financas_etl = TriggerDagRunOperator(
        task_id = "tsk_trigger_dag_financas_etl",
        trigger_dag_id = "dag_financas_etl"
    )

    task_enviar_email_conclusao = PythonOperator(
        task_id = "tsk_enviar_email_conclusao",
        python_callable = enviar_email_conclusao,
        trigger_rule = "all_success"
    )

task_receber_moedas >> task_receber_cotacoes_moedas
inicio >> task_enviar_email_inicio >> [grp_moedas,task_receber_dados_selic,task_receber_cotacoes_ibov] >> task_trigger_dag_financas_etl >> task_enviar_email_conclusao >> fim