def comandos_criar_tabelas_stg():
    lista_comandos_criar_tabelas_stg = []

    lista_comandos_criar_tabelas_stg.append(
        """
        CREATE TABLE IF NOT EXISTS stg.tb_vacinacao_covid
        (
            id_vacinacao VARCHAR(250) NULL,
            data_referencia VARCHAR(10) NULL,
            sistema_origem VARCHAR(100) NULL,
            vacina_nome VARCHAR(250) NULL,
            vacina_descricao_dose VARCHAR(250) NULL,
            vacina_nome_fabricante VARCHAR(250) NULL,
            vacina_lote VARCHAR(250) NULL,
            estabelecimento_codigo VARCHAR(250) NULL,
            estabelecimento_nome VARCHAR(250) NULL,
            estabelecimento_razao_social VARCHAR(250) NULL,
            estabelecimento_uf VARCHAR(250) NULL,
            estabelecimento_municipio VARCHAR(250) NULL,
            paciente_id VARCHAR(250) NULL,
            paciente_pais VARCHAR(250) NULL,
            paciente_uf VARCHAR(250) NULL,
            paciente_municipio VARCHAR(250) NULL,
            paciente_data_nascimento VARCHAR(10) NULL,
            paciente_idade VARCHAR(250) NULL,
            paciente_raca VARCHAR(250) NULL,
            paciente_sexo VARCHAR(250) NULL
        );
        """
    )

    return lista_comandos_criar_tabelas_stg