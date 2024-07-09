import modulos.vacinas as v
import modulos.operacoes_mongodb as op
import pendulum
import pandas as pd

from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.email import send_email

str_con_mongo = Variable.get(key="string_conn_mongodb")
carga_completa = Variable.get(key="carga_completa")
destinatario_email = Variable.get(key="destinatario_email")
carga_completa = True if carga_completa == "True" else False

banco_raw = "raw_data"

def enviar_email_inicio():
    assunto = "Airflow: DAG Vacinas iniciada"
    corpo_email = f"DAG Vacinas foi iniciada às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def enviar_email_conclusao():
    assunto = "Airflow: DAG Vacinas concluída"
    corpo_email = f"DAG Vacinas foi concluída às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def receber_vacinas():
    vacinas = []

    if carga_completa:
        lista_datas = pd.date_range(start ='2020-01-01',
                                    end =datetime.now().strftime('%Y-%m-%d'))
        lista_datas = lista_datas.strftime('%Y-%m-%d')

        for data in lista_datas:
            vacinas_diarias = v.get_dados_vacina(data_inicial=data, data_final=data,
                                    uf="SP", cidade="RIBEIRAO PRETO")
            if len(vacinas_diarias) > 0:
                op.salvar_documentos(string_conexao=str_con_mongo,banco=banco_raw,
                                     colecao="vacinacao_covid",lista_documentos=vacinas_diarias)
    else:
        pipeline = [
            { "$group": { "_id": 0, "max_data": { "$max": "$data_referencia"} } }
        ]

        retorno_data = op.buscar_com_aggregate(string_conexao=str_con_mongo,
                                                banco=banco_raw, colecao="vacinacao_covid",
                                                pipeline=pipeline)
        
        retorno_data = list(retorno_data)

        if len(retorno_data) > 0: 
            lista_datas = []
            
            for item in retorno_data:
                lista_datas.append(item["max_data"])
            
            data_inicial = lista_datas[0][:10]
        
            data_final = datetime.now().strftime('%Y-%m-%d')
        
            vacinas = v.get_dados_vacina(data_inicial=data_inicial, data_final=data_final,
                                        uf="SP", cidade="RIBEIRAO PRETO")
        else:
            print("Nenhuma data retornada para a carga incremental. Ative a carga completa!")
        
    op.salvar_documentos(string_conexao=str_con_mongo, banco=banco_raw,
                         colecao="vacinacao_covid", lista_documentos=vacinas)

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024,4,10,22,0,0,0, tz="America/Sao_Paulo"),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    "email": ["alceu.pantoni@gmail.com"],
    "email_on_failure": True
}

with DAG(
    dag_id = "dag_vacinas",
    description = "Receber dados de vacinas do COVID aplicadas na cidade de Ribeirão Preto/SP.",
    schedule_interval = "30 22 * * *",
    default_args = default_args,
    catchup = False
) as dag:
    
    task_enviar_email_inicio = PythonOperator(
        task_id = "tsk_enviar_email_inicio",
        python_callable = enviar_email_inicio
    )
    
    inicio = EmptyOperator(task_id="vacinas_inicio")
    fim = EmptyOperator(task_id="vacinas_fim")

    task_receber_dados_vacinas = PythonOperator(
        task_id = "tsk_receber_vacinas",
        python_callable = receber_vacinas
    )

    task_trigger_dag_vacinas_etl = TriggerDagRunOperator(
        task_id = "tsk_trigger_dag_vacinas_etl",
        trigger_dag_id = "dag_vacinas_etl"
    )

    task_enviar_email_conclusao = PythonOperator(
        task_id = "tsk_enviar_email_conclusao",
        python_callable = enviar_email_conclusao,
        trigger_rule = "all_success"
    )

inicio >> task_enviar_email_inicio >> task_receber_dados_vacinas >> task_trigger_dag_vacinas_etl >> task_enviar_email_conclusao >> fim