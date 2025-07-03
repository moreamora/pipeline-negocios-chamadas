import pandas as pd
from datetime import datetime, timedelta, time, timezone

UTC = timezone.utc

# Caminhos
CAMINHO_CHAMADAS = "data/atualizado/chamadas.csv"
CAMINHO_NEGOCIOS = "data/atualizado/negocios.csv"
CAMINHO_LEADTIME = "data/atualizado/leadtime.csv"

# Fase 1 – Criar CSV base com colunas desejadas
def criar_base_leadtime():
    colunas_desejadas = [
        "Associated Deal IDs",
        "Associated Deal",
        "Data da atividade",
        "Atividade atribuída a"
    ]
    df_chamadas = pd.read_csv(CAMINHO_CHAMADAS)

    # Seleciona apenas as colunas desejadas
    df_filtrado = df_chamadas[[col for col in colunas_desejadas if col in df_chamadas.columns]].copy()

    # Inverte para que as chamadas mais antigas fiquem primeiro
    df_filtrado = df_filtrado.iloc[::-1].reset_index(drop=True)

    # Remove duplicados, mantendo apenas o primeiro (mais antigo) por ID
    df_filtrado = df_filtrado.drop_duplicates(subset="Associated Deal IDs", keep="first")

    # Converte o ID para string
    if "Associated Deal IDs" in df_filtrado.columns:
        df_filtrado["Associated Deal IDs"] = df_filtrado["Associated Deal IDs"].astype(str)

    df_filtrado.to_csv(CAMINHO_LEADTIME, index=False)
    print(f"✅ leadtime.csv criado com {len(df_filtrado)} registros únicos por ID.")


# Fase 2 – Adicionar 'Data de criação' e 'Momento de Compra' com merge baseado no ID
def completar_data_criacao_em_leadtime():
    df_leadtime = pd.read_csv(CAMINHO_LEADTIME)
    df_negocios = pd.read_csv(CAMINHO_NEGOCIOS)

    df_leadtime["Associated Deal IDs"] = pd.to_numeric(df_leadtime["Associated Deal IDs"], errors="coerce")
    df_negocios["ID do registro."] = pd.to_numeric(df_negocios["ID do registro."], errors="coerce")

    # Merge incluindo 'Data de criação' e 'Momento de Compra'
    df_merged = df_leadtime.merge(
        df_negocios[["ID do registro.", "Data de criação", "Momento de Compra"]],
        how="left",
        left_on="Associated Deal IDs",
        right_on="ID do registro."
    )

    df_merged.drop(columns=["ID do registro."], inplace=True)

    # Reorganizar: id, associado a, momento de Compra, atividade atribuída a, data criação, data atividade
    colunas_ordenadas = [
        "Associated Deal IDs",
        "Associated Deal",
        "Momento de Compra",
        "Atividade atribuída a",
        "Data de criação",
        "Data da atividade"
    ]
    colunas_restantes = [col for col in df_merged.columns if col not in colunas_ordenadas]
    df_merged = df_merged[colunas_ordenadas + colunas_restantes]

    df_merged.to_csv(CAMINHO_LEADTIME, index=False)

# Fase 3 – Calcular e salvar Lead Time
def arredondar_para_periodo_util(dt):
    dia_semana = dt.weekday()
    hora = dt.time()
    
    # Domingo (6): Avança para segunda-feira às 08:00
    if dia_semana == 6:
        return datetime.combine((dt + timedelta(days=1)).date(), time(8, 0), tzinfo=UTC)
    # Sábado (5): Horário comercial é 08:00-18:00
    elif dia_semana == 5:
        if hora < time(8, 0):
            return dt.replace(hour=8, minute=0, second=0, microsecond=0)
        elif hora >= time(18, 0):
            return datetime.combine((dt + timedelta(days=2)).date(), time(8, 0), tzinfo=UTC)
    # Segunda a sexta: Horário comercial é 08:00-20:00
    else:
        if hora < time(8, 0):
            return dt.replace(hour=8, minute=0, second=0, microsecond=0)
        elif hora >= time(20, 0):
            return datetime.combine((dt + timedelta(days=1)).date(), time(8, 0), tzinfo=UTC)
    return dt

def calcular_lead_time_util(data_inicio, data_fim):
    if data_inicio.tzinfo is None:
        data_inicio = data_inicio.replace(tzinfo=UTC)
    if data_fim.tzinfo is None:
        data_fim = data_fim.replace(tzinfo=UTC)
    
    data_inicio = arredondar_para_periodo_util(data_inicio)
    data_fim = arredondar_para_periodo_util(data_fim)
    
    if data_fim <= data_inicio:
        return timedelta(0)
    
    total = timedelta()
    atual = data_inicio
    
    # Loop que vai somando tempo útil dentro do intervalo
    while atual.date() < data_fim.date():
        dia = atual.weekday()
        if dia < 5:  # Segunda a sexta
            ini = max(atual, atual.replace(hour=8, minute=0, second=0, microsecond=0))
            fim = atual.replace(hour=20, minute=0, second=0, microsecond=0)
        elif dia == 5:  # Sábado
            ini = max(atual, atual.replace(hour=8, minute=0, second=0, microsecond=0))
            fim = atual.replace(hour=18, minute=0, second=0, microsecond=0)
        else:  # Domingo
            atual = datetime.combine((atual + timedelta(days=1)).date(), time(0, 0), tzinfo=UTC)
            continue   

        if ini < fim:
            total += fim - ini

        atual = datetime.combine((atual + timedelta(days=1)).date(), time(0, 0), tzinfo=UTC)
    
    # Trata o último dia separadamente
    dia_fim = data_fim.weekday()
    if dia_fim < 5:
        ini = max(data_fim.replace(hour=8, minute=0, second=0, microsecond=0), atual)
        fim = min(data_fim, data_fim.replace(hour=20, minute=0, second=0, microsecond=0))
    elif dia_fim == 5:
        ini = max(data_fim.replace(hour=8, minute=0, second=0, microsecond=0), atual)
        fim = min(data_fim, data_fim.replace(hour=18, minute=0, second=0, microsecond=0))
    else:
        
        return total

    if fim > ini:
        total += fim - ini
    
    return total

def formatar_timedelta(td):
    total_segundos = int(td.total_seconds())
    horas, resto = divmod(total_segundos, 3600)
    minutos = resto // 60
    return f"{horas:02}:{minutos:02}"

def processar_leadtime_csv():
    df = pd.read_csv(CAMINHO_LEADTIME)
    dados = df.to_dict(orient="records")

    for row in dados:
        data_criacao = row.get('Data de criação')
        data_atividade = row.get('Data da atividade')

        if data_criacao and data_atividade:
            try:
                dt_criacao = datetime.strptime(data_criacao, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
                dt_atividade = datetime.strptime(data_atividade, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
                lead_time = calcular_lead_time_util(dt_criacao, dt_atividade)

                # Verifica se a DATA DE CRIAÇÃO está dentro do horário comercial
                hora = dt_criacao.time()
                dia_semana = dt_criacao.weekday()
                if (
                    (0 <= dia_semana <= 4 and time(8, 0) <= hora < time(20, 0)) or  # Seg-Sex
                    (dia_semana == 5 and time(8, 0) <= hora < time(18, 0))           # Sábado
                ):
                    row["Horário da atividade"] = "Dentro do horário comercial"
                else:
                    row["Horário da atividade"] = "Fora do horário comercial"

                # Formatos: HH:MM e minutos inteiros
                row["Lead Time"] = formatar_timedelta(lead_time)
                row["Lead Time (min)"] = int(lead_time.total_seconds() // 60)

            except Exception as e:
                row["Lead Time"] = f"Erro: {str(e)}"
                row["Lead Time (min)"] = ""
                row["Horário da atividade"] = "Erro"
        else:
            row["Lead Time"] = ""
            row["Lead Time (min)"] = ""
            row["Horário da atividade"] = ""

    # Cria o novo DataFrame e ordena por "Data de criação"
    df_resultado = pd.DataFrame(dados)
    df_resultado["Data de criação"] = pd.to_datetime(df_resultado["Data de criação"], errors="coerce")
    df_resultado = df_resultado.sort_values(by="Data de criação", ascending=False)

    # Salva ordenado
    df_resultado.to_csv(CAMINHO_LEADTIME, index=False)
    print("✅ Lead Time calculados e salvos com sucesso.")


# --- Executar tudo em uma função principal ---
def main():
    criar_base_leadtime()
    completar_data_criacao_em_leadtime()
    processar_leadtime_csv()

if __name__ == "__main__":
    main()
