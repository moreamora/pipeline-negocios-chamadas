from dotenv import load_dotenv
import pandas as pd
import gspread
import numpy as np
from google.oauth2.service_account import Credentials
import os
import json
import tempfile
import time

# Configurações
NEGOCIOS_CSV_PATH = "data/atualizado/negocios.csv"
NEGOCIOS_CHAMADAS_CSV_PATH = "data/atualizado/negocios-chamadas.csv"
SHEET_NAME = "Acompanhamento métricas-chave aMORA - 2025 - Lead time"
WORKSHEET_NAME = "Negócios"

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive',
]

def autenticar():
    # Lê o JSON da conta de serviço a partir da variável de ambiente
    load_dotenv()
    service_account_info_str = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    
    if not service_account_info_str:
        raise ValueError("Variável de ambiente GOOGLE_SERVICE_ACCOUNT_JSON não definida.")
    
    # Converte de string para dicionário
    service_account_info = json.loads(service_account_info_str)

    # Cria as credenciais e o cliente gspread
    creds = Credentials.from_service_account_info(service_account_info, scopes=SCOPES)
    return gspread.authorize(creds)

def clean_row(row):
    clean = []
    for val in row:
        if pd.isna(val):
            clean.append('')
        elif isinstance(val, float) and (val == float('inf') or val == float('-inf')):
            clean.append('')
        else:
            clean.append(str(val))
    return clean

def insere_novos_negocios(worksheet, df_sheets):
    df_csv = pd.read_csv(NEGOCIOS_CSV_PATH)

    # Ordena o CSV por data de criação: mais antigo primeiro (crescente)
    df_csv["Data de criação"] = pd.to_datetime(df_csv["Data de criação"], errors="coerce")
    df_csv = df_csv.sort_values(by="Data de criação", ascending=True)  # crescente

    # Limpa e converte os IDs
    df_sheets["ID do registro."] = df_sheets["ID do registro."].astype(str).str.split('.').str[0]
    df_csv["ID do registro."] = df_csv["ID do registro."].astype(str).str.split('.').str[0]

    ids_sheets = set(df_sheets["ID do registro."])
    novos_ids = [id_ for id_ in df_csv["ID do registro."] if id_ not in ids_sheets]

    novas_linhas = []

    # Montagem das novas linhas
    for id_ in novos_ids:
        linha_csv = df_csv[df_csv["ID do registro."] == id_].iloc[0]
        nova_linha = [
            linha_csv.get("ID do registro.", ""),
            linha_csv.get("Nome do negócio", ""),
            linha_csv.get("Etapa do negócio", ""),
            linha_csv.get("Data de criação", "").strftime("%Y-%m-%d %H:%M") if pd.notna(linha_csv.get("Data de criação", "")) else "",
            "",  # Semana de criação
            "",  # Mês de criação
            linha_csv.get("status_cadastro", ""),
            linha_csv.get("Momento de Compra", ""),
            linha_csv.get("Proprietário do negócio", ""),
            "",  # Horário Comercial
            "",  # Data da primeira chamada
            "",  # Lead Time (min)
        ]

        # Checa se 2025
        if "2025" in str(linha_csv["Data de criação"]):
            novas_linhas.append(clean_row(nova_linha))

    if novas_linhas:
        novas_linhas.reverse()  # inverter para que o mais recente fique em cima (linha 3)
        worksheet.insert_rows(novas_linhas, 2)  # insere a partir da linha 2
        print(f"✅ Inseridos {len(novas_linhas)} registros de uma vez na ordem correta.")
    else:
        print("Nenhum registro novo para inserir.")



def atualiza_leadtime(worksheet, df_sheets):
    df_csv = pd.read_csv(NEGOCIOS_CHAMADAS_CSV_PATH)

    df_sheets["ID do registro."] = df_sheets["ID do registro."].astype(str).str.split('.').str[0]
    df_csv["Associated Deal IDs"] = df_csv["Associated Deal IDs"].astype(str).str.split('.').str[0]

    lookup = {
        str(row["Associated Deal IDs"]): {
            "Lead time (min)": row.get("Lead Time (min)", ""),
            
            # "Horário da atividade": row.get("Horário da atividade", ""),
            "Data da atividade": row.get("Data da atividade", "")
        }
        for _, row in df_csv.iterrows()
    }

    for idx, row in df_sheets.iterrows():
        id_ = str(row["ID do registro."]).split('.')[0]
        if id_ in lookup:
            info = lookup[id_]
            df_sheets.at[idx, "Lead time (min)"] = info.get("Lead time (min)", "")
            # df_sheets.at[idx, "Horário Comercial"] = info.get("Horário da atividade", "")
            df_sheets.at[idx, "Data da primeira chamada"] = info.get("Data da atividade", "")

    df_sheets = df_sheets.replace([np.inf, -np.inf], np.nan).fillna('')
    valores = [df_sheets.columns.tolist()] + df_sheets.values.tolist()
    worksheet.update('A1', valores)

    print("Lead time atualizado com sucesso!")

    os.makedirs(os.path.dirname(NEGOCIOS_CHAMADAS_CSV_PATH), exist_ok=True)
    df_sheets.to_csv(NEGOCIOS_CHAMADAS_CSV_PATH, index=False, encoding='utf-8')
    print(f"Arquivo salvo: {NEGOCIOS_CHAMADAS_CSV_PATH}")

def insere_formulas(worksheet):
    horario_comercial = '''=ARRAYFORMULA(
            SE(D2:D=""; "";
                SE(
                DIA.DA.SEMANA(D2:D; 2) = 7;
                "Fora do Horário Comercial";
                SE(
                    (
                    (DIA.DA.SEMANA(D2:D; 2) <= 5) * (TEXTO(D2:D;"HH:MM") >= "08:00") * (TEXTO(D2:D;"HH:MM") < "20:00")
                    ) +
                    (
                    (DIA.DA.SEMANA(D2:D; 2) = 6) * (TEXTO(D2:D;"HH:MM") >= "08:00") * (TEXTO(D2:D;"HH:MM") < "18:00")
                    );
                    "Dentro do Horário Comercial";
                    "Fora do Horário Comercial"
                )
                )
            )
            )
            '''

    mes_criacao = '''=ARRAYFORMULA(SE(D2:D<>""; MÊS(D2:D); ""))'''

    semana_criacao = '''=ARRAYFORMULA(SE(D2:D<>""; ISOWEEKNUM(D2:D); ""))'''

    worksheet.update_acell('J2', horario_comercial)
    worksheet.update_acell('F2', mes_criacao)
    worksheet.update_acell('E2', semana_criacao)

def limpa_colunas(worksheet):
    total_linhas = len(worksheet.get_all_values())

    if total_linhas <= 2:
        return

    intervalo_E = f"E3:E{total_linhas}"
    intervalo_F = f"F3:F{total_linhas}"
    intervalo_J = f"J3:J{total_linhas}"

    # Limpa (deleta conteúdo) dos intervalos
    worksheet.batch_clear([intervalo_E, intervalo_F, intervalo_J])

def main():
    gc = autenticar()
    sh = gc.open(SHEET_NAME)
    worksheet = sh.worksheet(WORKSHEET_NAME)

    # Lê o conteúdo atual da planilha
    records = worksheet.get_all_records()
    df_sheets = pd.DataFrame(records)

    # Atualiza negócios
    insere_novos_negocios(worksheet, df_sheets)

    # 🔁 Recarrega a planilha após inserção
    time.sleep(2)  # Pequeno delay para garantir que o Google Sheets salve os dados
    records = worksheet.get_all_records()
    df_sheets = pd.DataFrame(records)

    # # Atualiza lead time com base na versão mais atual da planilha
    atualiza_leadtime(worksheet, df_sheets)

    # # Formulas
    time.sleep(1)
    limpa_colunas(worksheet)
    insere_formulas(worksheet)

    print("✅ Planilha final atualizada com dados de negócios e lead time.")

if __name__ == "__main__":
    main()
