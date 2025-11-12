from langflow.custom.custom_component.component import Component
from langflow.io import MessageTextInput, Output
from langflow.schema.data import Data
import pandas as pd
import re
import json

# ============================================================
# FUNÇÃO PRINCIPAL: EXECUTAR PESQUISA
# ============================================================
def executar_pesquisa(entrada, arquivo_excel="LA_data_consolidation.xlsx", aba=None, header_linha=13):
    # --- Mapeamento completo de colunas ---
    COLUNAS_MAPEAMENTO = {
        "aprovacao": "Approval Date",
        "approval": "Approval Date",
        "mes": "Month",
        "month": "Month",
        "bid": "Bid Number",
        "cases": "Bid Number",
        "casos": "Bid Number",
        "caso": "Bid Number",
        "case": "Bid Number",
        "approver": "Delegation Approver",
        "pricer": "Assigned Pricer",
        "cliente": "Customer Name",
        "customer": "Customer Name",
        "client": "Customer Name",
        "contract": "Contract Type",
        "market": "Market",
        "produto": "Product",
        "product": "Product",
        "serial": "Serial",
        "serie": "Serial",
        "transacao": "Transaction Type",
        "transaction": "Transaction Type",
        "description": "Case Description",
        "exchange": "Exchange/MO",
        "mo": "Exchange/MO",
        "path": "Path",
        "mips": "Incremental MIPS",
        "$/mips": "$/MIPS",
        "preco": "New Content Customer Price",
        "price": "New Content Customer Price",
        "tcv": "New Contnt Customer Price + WR",
        "invoice price": "New Contnt Customer Price + WR",
        "rbd": "RBD - Total",
        "tip": "TIP",
        "credito": "OIO & Other Credit",
        "credit": "OIO & Other Credit",
        "oio": "OIO & Other Credit",
        "receita": "New Content Net Revenue",
        "revenue": "New Content Net Revenue",
        "delegation rev": "New Content Delegation Rev",
        "bmc": "New Content BMC",
        "bmc gp %": "New Content BMC GP (%)",
        "tmc": "New Content TMC",
        "tmc gp %": "New Content TMC GP (%)",
        "delegation gp %": "Delegation GP %",
        "delta": "Delta % to Target Price",
        "machine": "To Model",
        "maquina": "To Model",
        "total price": "Total Customer Price",
        "config": "Upgrade Config",
        "configuration": "Upgrade Config",
        "partner bp": "Partner Margin %",
        "target": "Target Price",
        "target price": "Target Price",
        "delegation level": "Delegation Level",
        "upgrade ref price": "Upgrade Ref Price",
        "partner margin": "Partner Margin %",
        "contract type": "Contract Type"
    }

    COLUNAS_MAPEAMENTO_LOWER = {k.lower(): v.lower() for k, v in COLUNAS_MAPEAMENTO.items()}

    # --- Normalização de colunas ---
    def normalizar_colunas(df):
        return {str(c).strip().lower(): c for c in df.columns}

    # --- Mapeamento de coluna ---
    def mapear_coluna(coluna, df_map):
        if not coluna:
            return None
        key = coluna.strip().lower()
        if key in COLUNAS_MAPEAMENTO_LOWER:
            mapped = COLUNAS_MAPEAMENTO_LOWER[key]
            if mapped in df_map:
                return df_map[mapped]
        if key in df_map:
            return df_map[key]
        for k, v in df_map.items():
            if key in k:
                return v
        return None

    # --- Leitura do Excel ---
    df = pd.read_excel(arquivo_excel, sheet_name=aba or 0, header=header_linha)
    df.columns = [str(c).strip() for c in df.columns]
    df_map = normalizar_colunas(df)

    # ============================================================
    # FUNÇÃO DE CONVERSÃO SEGURA PARA PORCENTAGEM
    # ============================================================
    def converter_para_numero(coluna):
        """Converte strings de porcentagem em números corretamente, sem duplicar conversão."""
        if df[coluna].dtype == object:
            serie = df[coluna].astype(str).str.strip()
            if serie.str.contains("%").any():
                serie = serie.str.replace("%", "", regex=False)
                serie = serie.str.replace(",", ".", regex=False)
                serie = pd.to_numeric(serie, errors="coerce") / 100
                return serie
        return pd.to_numeric(df[coluna], errors="coerce")

    # ============================================================
    # FILTROS
    # ============================================================
    filtros_entrada = []
    for chave in ["data", "filter", "filtros", "filters"]:
        valor = entrada.get(chave)
        if isinstance(valor, list):
            filtros_entrada.extend([x for x in valor if isinstance(x, dict) and "column_name" in x and "value" in x])

    if filtros_entrada:
        filtro = pd.Series([True] * len(df), index=df.index)
        termos_por_coluna = {}
        for item in filtros_entrada:
            coluna_real = mapear_coluna(item.get("column_name"), df_map)
            if coluna_real:
                termos_por_coluna.setdefault(coluna_real, []).append(item.get("value"))

        for col, valores in termos_por_coluna.items():
            col_text = df[col].astype(str).str.lower()
            filtro_col = pd.Series([False] * len(df), index=df.index)
            for v in valores:
                filtro_col |= col_text.str.contains(str(v).lower(), na=False)
            filtro &= filtro_col

        df_filtrado = df.loc[filtro].copy()
    else:
        print("ℹ️ Nenhum filtro fornecido — usando planilha completa.")
        df_filtrado = df.copy()

    if df_filtrado.empty:
        print("⚠️ Nenhum resultado encontrado após aplicar filtros.")
        return pd.DataFrame()

    # ============================================================
    # COLUNAS A EXIBIR
    # ============================================================
    colunas_reais = []
    for c in entrada.get("columns_to_show", []):
        mapped = mapear_coluna(c, df_map)
        if mapped:
            colunas_reais.append(mapped)

    resultado = df_filtrado[colunas_reais] if colunas_reais else pd.DataFrame()

    # ============================================================
    # OPERAÇÕES NUMÉRICAS (com tratamento de %)
    # ============================================================
    coluna_operacao = entrada.get("column_operation")
    operacao = entrada.get("operation")
    if coluna_operacao and operacao:
        col_op = mapear_coluna(coluna_operacao, df_map)
        if col_op:
            serie = converter_para_numero(col_op)
            valor = None
            if operacao == "media":
                valor = serie.mean()
            elif operacao == "soma":
                valor = serie.sum()
            elif operacao == "contagem":
                valor = serie.count()
            elif operacao == "maximo":
                valor = serie.max()
            elif operacao == "minimo":
                valor = serie.min()
            elif operacao == "diferenca":
                valor = serie.max() - serie.min()
            elif operacao == "moda" and not serie.dropna().empty:
                valor = serie.mode()[0]
            elif operacao == "mediana":
                valor = serie.median()
            elif operacao == "desvio_padrao":
                valor = serie.std()
            elif operacao == "variancia":
                valor = serie.var()

            if valor is not None:
                if resultado.empty:
                    resultado = pd.DataFrame([{f"{operacao}_{col_op}": valor}])
                else:
                    resultado[f"{operacao}_{col_op}"] = valor

    # ============================================================
    # RANKING
    # ============================================================
    if entrada.get("ranking"):
        r = entrada["ranking"]
        col_r = mapear_coluna(r.get("column"), df_map)
        n = r.get("n", 5)
        ordem = r.get("order", "desc")
        if col_r:
            serie = converter_para_numero(col_r)
            ranking = serie.sort_values(ascending=(ordem == "asc")).head(n)
            print(f"\nRanking ({ordem}) dos {n} valores em {col_r}:\n{ranking}")

    # ============================================================
    # AGRUPAMENTO
    # ============================================================
    if entrada.get("group_by"):
        group_cols = [mapear_coluna(c, df_map) for c in entrada["group_by"] if mapear_coluna(c, df_map)]
        col_op = mapear_coluna(entrada.get("column_operation"), df_map)
        if group_cols and col_op:
            agrupado = df_filtrado.groupby(group_cols)[col_op].mean().reset_index()
            print(f"\nAgregação (média de {col_op}) por {group_cols}:\n{agrupado}")

    # ============================================================
    # CORRELAÇÃO
    # ============================================================
    if entrada.get("correlation"):
        cols_corr = [mapear_coluna(c, df_map) for c in entrada["correlation"] if mapear_coluna(c, df_map)]
        if len(cols_corr) == 2:
            serie1 = converter_para_numero(cols_corr[0])
            serie2 = converter_para_numero(cols_corr[1])
            corr = serie1.corr(serie2)
            print(f"\nCorrelação entre {cols_corr[0]} e {cols_corr[1]}: {corr}")

    return resultado


# ============================================================
# COMPONENTE LANGFLOW COMPLETO
# ============================================================
class XLSComponent(Component):
    display_name = "LA_data_consolidation Component"
    description = "Componente para processar planilha e gerar relatórios"
    documentation: str = "https://docs.langflow.org/components-custom-components"
    icon = "code"
    name = "LA_data_consolidation Component"

    inputs = [
        MessageTextInput(
            name="entrada",
            display_name="Entrada JSON do LLM",
            info="Entrada JSON já processada pelo LLM",
            value="{}",
            tool_mode=True,
        )
    ]

    outputs = [
        Output(display_name="Output", name="output", method="build_output"),
    ]

    def build_output(self) -> Data:
        entrada = self.entrada

        if isinstance(entrada, str):
            try:
                entrada = json.loads(entrada)
            except json.JSONDecodeError:
                print("⚠️ Erro ao decodificar entrada JSON.")
                entrada = {}

        resultado = executar_pesquisa(
            entrada,
            arquivo_excel="/Users/gabrieladozono/Downloads/LA_data_consolidation.xlsx",
            aba=None,
            header_linha=13
        )

        data = Data(value={"resultado": resultado.to_dict(orient="records")})
        self.status = data
        return data
