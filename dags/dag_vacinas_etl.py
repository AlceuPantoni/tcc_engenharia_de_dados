import modulos.vacinas_stg as v_stg
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
    assunto = "Airflow: DAG Vacinas ETL iniciada"
    corpo_email = f"DAG Vacinas ETL foi iniciada às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def enviar_email_conclusao():
    assunto = "Airflow: DAG Vacinas ETL concluída"
    corpo_email = f"DAG Vacinas ETL foi concluída às {datetime.now()}.\n\n"

    send_email(to=destinatario_email, subject=assunto, html_content=corpo_email)

def executar_proc_dw_create_temp_vacinas():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_temp_vacinas();")

def executar_proc_dw_dim_vacinas():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_dim_vacinas();")

def executar_proc_dw_dim_vacinas_estabelecimentos():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_dim_vacinas_estabelecimentos();")

def executar_proc_dw_dim_vacinas_pacientes():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_dim_vacinas_pacientes();")

def executar_proc_dw_fato_vacinas():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_carregar_fato_vacinas();")

def executar_proc_dw_drop_temp_vacinas():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    pg_hook.run("CALL stg.proc_drop_temp_vacinas();")

def criar_tabelas_vacinas_stg():
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conexao = pg_hook.get_conn()
    cursor = conexao.cursor()

    lista_comandos_criar_tabelas_stg = v_stg.comandos_criar_tabelas_stg()

    for create_table in lista_comandos_criar_tabelas_stg:
        cursor.execute(create_table)

    conexao.commit()
    cursor.close()
    conexao.close()

def inserir_dados_vacinas_stg():
    query = {"importado_dw": False}

    qtde_total_documentos = op.contagem_total_documentos(string_conexao=str_con_mongo,
                                                         banco="raw_data",
                                                         colecao="vacinacao_covid",
                                                         query=query)
    
    if qtde_total_documentos == 0:
        print("Não há novos documentos 'vacinacao_covid' a serem integrados.")
        return
    
    pg_hook = PostgresHook(postgres_conn_id='postgres')
    conexao = pg_hook.get_conn()
    cursor = conexao.cursor()

    pg_hook.run(sql="TRUNCATE TABLE stg.tb_vacinacao_covid;", autocommit=True)

    indice_inicial = 0
    qtde_para_buscar = 100000

    while indice_inicial < qtde_total_documentos:
        lista_vacinas_temp = op.buscar_documentos_paginados(string_conexao=str_con_mongo,
                                                            banco="raw_data",
                                                            colecao="vacinacao_covid",
                                                            query=query,
                                                            skip=indice_inicial,
                                                            limit=qtde_para_buscar)

        lista_vacinas = []

        for reg in lista_vacinas_temp:
            temp_data_ref = datetime.strptime(reg["data_referencia"][0:10], '%Y-%m-%d')
            temp_data_ref = datetime.strftime(temp_data_ref,"%Y-%m-%d")

            temp_idade = str(reg["paciente_idade"]).strip()
            temp_idade = temp_idade if temp_idade != "NaN" and temp_idade is not None else ""

            item = {
                "id_vacinacao": reg["_id"],
                "data_referencia": temp_data_ref,
                "sistema_origem": reg["sistema_origem"],
                "vacina_nome": reg["vacina_nome"],
                "vacina_descricao_dose": reg["vacina_descricao_dose"],
                "vacina_nome_fabricante": reg["vacina_fabricante"],
                "vacina_lote": reg["vacina_lote"],
                "estabelecimento_codigo": int(reg["estabelecimento_codigo"]),
                "estabelecimento_nome": reg["estabelecimento_nome"],
                "estabelecimento_razao_social": reg["estabelecimento_razao_social"],
                "estabelecimento_uf": reg["estabelecimento_uf"],
                "estabelecimento_municipio": reg["estabelecimento_municipio_nome"],
                "paciente_id": reg["paciente_id"],
                "paciente_pais": reg["paciente_endereco_pais"],
                "paciente_uf": reg["paciente_endereco_uf"],
                "paciente_municipio": reg["paciente_endereco_municipio"],
                "paciente_data_nascimento": reg["paciente_data_nascimento"],
                "paciente_idade": temp_idade,
                "paciente_raca": reg["paciente_raca_descricao"],
                "paciente_sexo": reg["paciente_sexo"]
            }
            lista_vacinas.append(item)

        qtde_registros = len(lista_vacinas)
    
        print(f"Serão inseridos na 'stg.tb_vacinacao_covid' {qtde_registros} registros.")

        sql = """
            INSERT INTO stg.tb_vacinacao_covid (id_vacinacao, data_referencia, sistema_origem, vacina_nome, vacina_descricao_dose, vacina_nome_fabricante, vacina_lote, estabelecimento_codigo, estabelecimento_nome, estabelecimento_razao_social, estabelecimento_uf, estabelecimento_municipio, paciente_id, paciente_pais, paciente_uf, paciente_municipio, paciente_data_nascimento, paciente_idade, paciente_raca, paciente_sexo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        lote_formatado = [[
            record["id_vacinacao"],record["data_referencia"],record["sistema_origem"],
            record["vacina_nome"],record["vacina_descricao_dose"],record["vacina_nome_fabricante"],
            record["vacina_lote"],record["estabelecimento_codigo"],record["estabelecimento_nome"],
            record["estabelecimento_razao_social"],record["estabelecimento_uf"],
            record["estabelecimento_municipio"],record["paciente_id"],record["paciente_pais"],
            record["paciente_uf"],record["paciente_municipio"],record["paciente_data_nascimento"],
            record["paciente_idade"],record["paciente_raca"],record["paciente_sexo"]
        ] for record in lista_vacinas]

        cursor.executemany(sql, lote_formatado)

        conexao.commit()

        print(f"Registros inseridos: {len(lote_formatado)}.")
        
        indice_inicial += qtde_para_buscar

    cursor.close()
    conexao.close()

    importado_query = { "importado_dw" : False}
    importado_update = { "$set": {"importado_dw": True}}

    op.atualizar_documentos(string_conexao=str_con_mongo, banco="raw_data",
                            colecao="vacinacao_covid", query=importado_query,
                            update=importado_update)

default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2024,4,10,22,0,0,0, tz="America/Sao_Paulo"),
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    "email": ["alceu.pantoni@gmail.com"],
    "email_on_failure": True
}

with DAG(
    dag_id = "dag_vacinas_etl",
    description = "Carrega os dados de vacinas covif para stg, tratá-os e carrega-os no dw.",
    schedule_interval = None,
    default_args = default_args,
    catchup = False
) as dag:
    
    task_enviar_email_inicio = PythonOperator(
        task_id = "tsk_enviar_email_inicio",
        python_callable = enviar_email_inicio
    )
    
    inicio = EmptyOperator(task_id="vacinas_etl_inicio")
    fim = EmptyOperator(task_id="vacinas_etl_fim")

    task_criar_tabelas_vacinas_stg = PythonOperator(
        task_id = "tsk_criar_tabelas_vacinas_stg",
        python_callable = criar_tabelas_vacinas_stg
    )

    task_inserir_dados_vacinas_stg = PythonOperator(
        task_id = "tsk_inserir_dados_vacinas_stg",
        python_callable = inserir_dados_vacinas_stg
    )

    with TaskGroup(
        group_id="grp_vacinas_dw",
        tooltip="Grupo Vacinas DW (procedures)"
    ) as grp_vacinas_dw:

        task_proc_carregar_temp_vacinas = PythonOperator(
            task_id="tsk_proc_carregar_temp_vacinas",
            python_callable = executar_proc_dw_create_temp_vacinas
        )

        task_proc_carregar_dim_vacinas = PythonOperator(
            task_id="tsk_proc_carregar_dim_vacinas",
            python_callable = executar_proc_dw_dim_vacinas
        )

        task_proc_carregar_dim_vacinas_estabelecimentos = PythonOperator(
            task_id="tsk_proc_carregar_dim_vacinas_estabelecimentos",
            python_callable = executar_proc_dw_dim_vacinas_estabelecimentos
        )

        task_proc_carregar_dim_vacinas_pacientes = PythonOperator(
            task_id="tsk_proc_carregar_dim_vacinas_pacientes",
            python_callable = executar_proc_dw_dim_vacinas_pacientes
        )

        task_proc_carregar_fato_vacinas = PythonOperator(
            task_id="tsk_proc_carregar_fato_vacinas",
            python_callable = executar_proc_dw_fato_vacinas
        )

        task_proc_drop_temp_vacinas = PythonOperator(
            task_id="tsk_proc_drop_temp_vacinas",
            python_callable = executar_proc_dw_drop_temp_vacinas
        )

    task_enviar_email_conclusao = PythonOperator(
        task_id = "tsk_enviar_email_conclusao",
        python_callable = enviar_email_conclusao,
        trigger_rule = "all_success"
    )
task_proc_carregar_dim_vacinas_pacientes >> task_proc_carregar_dim_vacinas_estabelecimentos >> task_proc_carregar_temp_vacinas >> task_proc_carregar_dim_vacinas >> task_proc_carregar_fato_vacinas >> task_proc_drop_temp_vacinas
inicio >> task_enviar_email_inicio >> task_criar_tabelas_vacinas_stg >> task_inserir_dados_vacinas_stg >> grp_vacinas_dw >> task_enviar_email_conclusao >> fim