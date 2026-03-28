SCHEMA_INFORME_FUNDOS = {
    "Classe": {"nomes_origem": ["TP_FUNDO_CLASSE"], "tipo": "string"},
    "CNPJ": {"nomes_origem": ["CNPJ_FUNDO_CLASSE"], "tipo": "string"},
    "SUBCLASSE": {"nomes_origem": ["ID_SUBCLASSE"], "tipo": "string"},
    "DT_COMPTC": {"nomes_origem": ["DT_COMPTC"], "tipo": "string"},
    "VL_TOTAL": {"nomes_origem": ["VL_TOTAL"], "tipo": "string"},
    "VL_QUOTA": {"nomes_origem": ["VL_QUOTA"], "tipo": "string"},
    "VL_PATRIM_LIQ": {"nomes_origem": ["VL_PATRIM_LIQ"], "tipo": "string"},
    "CAPTC_DIA": {"nomes_origem": ["CAPTC_DIA"], "tipo": "string"},
    "RESG_DIA": {"nomes_origem": ["RESG_DIA"], "tipo": "string"},
    "NR_COTST": {"nomes_origem": ["NR_COTST"], "tipo": "string"},
    "ano_particao": {"nomes_origem": ["ano_particao"], "tipo": "int16"},
    "mes_particao": {"nomes_origem": ["mes_particao"], "tipo": "int"},
    "timestamp_dagrun": {
        "nomes_origem": ["timestamp_dagrun"],
        "tipo": "datetime64[us]",
    },
    "timestamp_escrita": {
        "nomes_origem": ["timestamp_escrita"],
        "tipo": "datetime64[us]",
    },
}
