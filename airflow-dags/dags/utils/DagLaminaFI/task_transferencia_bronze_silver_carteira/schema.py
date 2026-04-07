SCHEMA_SILVER_CARTEIRA = {
    "cnpj_fundo": {"nomes_origem": ["CNPJ_FUNDO_CLASSE"], "tipo": "string"},
    "denom_social": {"nomes_origem": ["DENOM_SOCIAL"], "tipo": "string"},
    "tp_fundo_classe": {"nomes_origem": ["TP_FUNDO_CLASSE"], "tipo": "string"},
    "id_subclasse": {"nomes_origem": ["ID_SUBCLASSE"], "tipo": "string"},
    "dt_comptc": {"nomes_origem": ["DT_COMPTC"], "tipo": "datetime64[us]"},
    "tp_ativo": {"nomes_origem": ["TP_ATIVO"], "tipo": "string"},
    "pr_pl_ativo": {"nomes_origem": ["PR_PL_ATIVO"], "tipo": "float32"},
    "ano_particao": {"nomes_origem": ["ano_particao"], "tipo": "int16"},
    "mes_particao": {"nomes_origem": ["mes_particao"], "tipo": "int8"},
    "timestamp_dagrun": {"nomes_origem": ["timestamp_dagrun"], "tipo": "datetime64[us]"},
    "timestamp_escrita": {"nomes_origem": ["timestamp_escrita"], "tipo": "datetime64[us]"},
}
