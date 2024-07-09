def comandos_criar_tabelas_stg():
    lista_comandos_criar_tabelas_stg = []

    lista_comandos_criar_tabelas_stg.append(
        """
        CREATE TABLE IF NOT EXISTS stg.tb_moedas
        (
            id_moeda VARCHAR(10) NULL,
            nome_moeda VARCHAR(100) NULL
        );
        """
    )

    lista_comandos_criar_tabelas_stg.append(
        """
            CREATE TABLE IF NOT EXISTS stg.tb_moedas_cotacoes
            (
                id_cotacao VARCHAR(100) NULL,
                id_moeda VARCHAR(10) NULL,
                tipo_boletim VARCHAR(50) NULL,
                data_referencia VARCHAR(19) NULL,
                cotacao_compra DECIMAL NULL,
                cotacao_venda DECIMAL NULL,
                paridade_compra DECIMAL NULL,
                paridade_venda DECIMAL NULL
            );
        """
    )

    lista_comandos_criar_tabelas_stg.append(
        """
        CREATE TABLE IF NOT EXISTS stg.tb_ibov_cotacoes
        (
            id_cotacao INT NULL,
            data_referencia VARCHAR(19) NULL,
            valor_cotacao_abertura DECIMAL NULL,
            valor_cotacao_fechamento DECIMAL NULL,
            valor_cotacao_fechamento_ajustado DECIMAL NULL,
            valor_cotacao_maxima DECIMAL NULL,
            valor_cotacao_minima DECIMAL NULL,
            volume_total_transacionado DECIMAL NULL
        );
        """
    )

    lista_comandos_criar_tabelas_stg.append(
        """
        CREATE TABLE IF NOT EXISTS stg.tb_selic
        (
            id_selic INT NULL,
            data_referencia VARCHAR(19) NULL,
            valor_selic DECIMAL NULL
        );
        """
    )

    return lista_comandos_criar_tabelas_stg