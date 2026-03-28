SCHEMA_INFORME_FUNDOS = {
    "CNPJ": {"nomes_origem": ["CNPJ"], "tipo": "string"},
    "classe": {"nomes_origem": ["Classe"], "tipo": "string"},
    "sub_classe": {"nomes_origem": ["SUBCLASSE"], "tipo": "string"},
    "data_cota": {"nomes_origem": ["DT_COMPTC"], "tipo": "datetime64[us]"},
    "vl_total": {"nomes_origem": ["VL_TOTAL"], "tipo": "float32"},
    "vl_quota": {"nomes_origem": ["VL_QUOTA"], "tipo": "float32"},
    "patrimonio_liquido": {"nomes_origem": ["VL_PATRIM_LIQ"], "tipo": "float64"},
    "captacao": {"nomes_origem": ["CAPTC_DIA"], "tipo": "float64"},
    "resgate": {"nomes_origem": ["RESG_DIA"], "tipo": "float64"},
    "numero_cotistas": {"nomes_origem": ["NR_COTST"], "tipo": "float64"},
    "ano_particao": {"nomes_origem": ["ano_particao"], "tipo": "int16"},
    "mes_particao": {"nomes_origem": ["mes_particao"], "tipo": "int8"},
    "timestamp_dagrun": {
        "nomes_origem": ["timestamp_dagrun"],
        "tipo": "datetime64[us]",
    },
    "timestamp_escrita": {
        "nomes_origem": ["timestamp_escrita"],
        "tipo": "datetime64[us]",
    },
}
