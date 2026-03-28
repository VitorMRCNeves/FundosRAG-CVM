def apply_schema(df, schema, rename_cols=True, select_cols=True):
    """
    Aplica schema em um DataFrame com base em um dicionário de schema customizado.
    Para cada chave do schema, busca o primeiro nome encontrado de nomes_origem nas colunas do DataFrame,
    e renomeia para a chave do schema.

    Args:
        df (pd.DataFrame): DataFrame alvo.
        schema (dict): Dicionário com informações das colunas.
                       Exemplo:
                       {
                           "Nome Fundo": {"nomes_origem": ['nomes_fundos','fundos'], "tipo": "string"},
                           "CNPJ": {"nomes_origem": ['cnpj'], "tipo": "string"}
                       }
        rename_cols (bool): Se True, renomeia as colunas conforme o schema.
        select_cols (bool): Se True, seleciona somente as colunas do schema.
                            Se False, verifica se há compatibilidade estrita de colunas.

    Returns:
        pd.DataFrame com schema aplicado.

    Raises:
        ValueError se houver incompatibilidade entre colunas conforme o modo de seleção.
    """

    col_map = {}
    for target_col, info in schema.items():
        found = None
        for nome_origem in info["nomes_origem"]:
            if nome_origem in df.columns:
                found = nome_origem
                break
        if found is not None:
            col_map[target_col] = found
        else:
            raise ValueError(
                f"A coluna de origem para '{target_col}' não foi encontrada no DataFrame."
            )

    if not select_cols:
        schema_flat_cols = set()
        for info in schema.values():
            schema_flat_cols.update(info["nomes_origem"])
        df_extra_cols = set(df.columns) - schema_flat_cols
        if df_extra_cols:
            raise ValueError(
                f"As colunas {df_extra_cols} do DataFrame não estão presentes no schema."
            )

    if select_cols:
        df = df[[col_map[c] for c in schema.keys()]]

    if rename_cols:
        rename_dict = {col_map[c]: c for c in schema.keys()}
        df = df.rename(columns=rename_dict)

    for target_col, info in schema.items():
        tipo = info.get("tipo")
        if target_col in df.columns:
            try:
                df[target_col] = df[target_col].astype(tipo)
            except Exception as e:
                raise ValueError(
                    f"Erro ao converter coluna '{target_col}' para o tipo '{tipo}': {str(e)}"
                ) from e

    return df
