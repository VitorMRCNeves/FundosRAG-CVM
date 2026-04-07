from unittest.mock import MagicMock, patch
import pandas as pd


_SAMPLE_BLC1 = pd.DataFrame({
    "CNPJ_FUNDO_CLASSE": ["00.000.000/0001-00"],
    "DENOM_SOCIAL": ["Fundo Teste"],
    "DT_COMPTC": ["2024-01-31"],
    "DT_CONFID_APLIC": [""],
    "EMISSOR_LIGADO": ["N"],
    "QT_AQUIS_NEGOC": ["100.000000"],
    "QT_POS_FINAL": ["100.000000"],
    "QT_VENDA_NEGOC": ["0.000000"],
    "TP_APLIC": ["Títulos Públicos"],
    "TP_ATIVO": ["Títulos Públicos"],
    "TP_FUNDO_CLASSE": ["FI"],
    "TP_NEGOC": ["Para negociação"],
    "VL_AQUIS_NEGOC": ["1000.00"],
    "VL_CUSTO_POS_FINAL": ["1000.00"],
    "VL_MERC_POS_FINAL": ["1050.00"],
    "VL_VENDA_NEGOC": ["0.00"],
    "CD_ISIN": ["BRTNLPCTF007"],
    "CD_SELIC": ["760199"],
    "DT_EMISSAO": ["2020-01-01"],
    "DT_VENC": ["2030-01-01"],
    "TP_TITPUB": ["LFT"],
})


def test_landing_bronze_chama_write_deltalake(monkeypatch, tmp_path):
    """Deve chamar write_deltalake com mode=append e partition_by correto."""
    monkeypatch.setenv("BUCKET_LANDING", str(tmp_path))
    monkeypatch.setenv("BUCKET_BRONZE", str(tmp_path / "bronze"))

    landing_path = tmp_path / "fundos" / "cda" / "blc_1" / "ano=2024" / "mes=01"
    landing_path.mkdir(parents=True)
    _SAMPLE_BLC1.to_csv(landing_path / "blc_1_2024-01-31.csv", index=False)

    _mod = "utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc"
    mock_dt = MagicMock()
    with patch(f"{_mod}.write_deltalake") as mock_write, patch(
        f"{_mod}.DeltaTable",
        return_value=mock_dt,
    ):
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )
        fn_processa_landing_bronze_blc("2024-01-31T10:00:00", "blc_1")

    mock_write.assert_called_once()
    call_kwargs = mock_write.call_args
    assert call_kwargs.kwargs.get("mode") == "append"
    assert "ano_particao" in call_kwargs.kwargs.get("partition_by", [])
    assert "mes_particao" in call_kwargs.kwargs.get("partition_by", [])


def test_landing_bronze_adiciona_metadata(monkeypatch, tmp_path):
    """O DataFrame enviado para write_deltalake deve conter timestamp_dagrun e timestamp_escrita."""
    monkeypatch.setenv("BUCKET_LANDING", str(tmp_path))
    monkeypatch.setenv("BUCKET_BRONZE", str(tmp_path / "bronze"))

    landing_path = tmp_path / "fundos" / "cda" / "blc_1" / "ano=2024" / "mes=01"
    landing_path.mkdir(parents=True)
    _SAMPLE_BLC1.to_csv(landing_path / "blc_1_2024-01-31.csv", index=False)

    _mod = "utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc"
    captured = {}
    mock_dt = MagicMock()

    def _capture_write(path, df, **kwargs):
        captured["df"] = df

    with patch(f"{_mod}.write_deltalake", side_effect=_capture_write), patch(
        f"{_mod}.DeltaTable",
        return_value=mock_dt,
    ):
        from utils.DagCarteiraCDA.task_transferencia_landing_bronze_blc.task_transferencia_landing_bronze_blc import (
            fn_processa_landing_bronze_blc,
        )
        fn_processa_landing_bronze_blc("2024-01-31T10:00:00", "blc_1")

    df = captured["df"]
    assert "timestamp_dagrun" in df.columns
    assert "timestamp_escrita" in df.columns
    assert "ano_particao" in df.columns
    assert "mes_particao" in df.columns
