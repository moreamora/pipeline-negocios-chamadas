import csv
import os
from datetime import datetime, timezone, timedelta
import requests
import json
from bs4 import BeautifulSoup
import re
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("HUBSPOT_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

BR_TZ = timezone(timedelta(hours=-3))

# Negócios
NEGOCIOS_URL = "https://api.hubapi.com/crm/v3/objects/deals"
NEGOCIOS_CSV = "data/atualizado/negocios.csv"
PROPERTIES_NEGOCIOS = [
    "hs_object_id", "dealname", "dealstage", "hubspot_owner_id", "createdate",
    "status_cadastro", "hs_tag_ids", "purchase_moment", "foi_conectado", "valor_original",
    "renda_cadastrada", "hs_v2_date_entered_94896182", "hs_v2_date_entered_94896183",
    "hs_v2_date_entered_94896184", "hs_v2_date_entered_94896185", "hs_v2_date_entered_94896186",
    "hs_v2_date_entered_94944032", "hs_v2_date_entered_94944033", "hs_v2_date_entered_94944035",
    "email", "motivo_de_perda_do_negocio", "analise_de_credito", "quentura_do_lead",
    "score_report", "score_report_2o_prop", "hs_lastmodifieddate"
]
API_TO_CSV_NEGOCIOS = {
    "hs_object_id": "ID do registro.",
    "dealname": "Nome do negócio",
    "dealstage": "Etapa do negócio",
    "hubspot_owner_id": "Proprietário do negócio",
    "createdate": "Data de criação",
    "status_cadastro": "status_cadastro",
    "hs_tag_ids": "Tags do negócio",
    "purchase_moment": "Momento de Compra",
    "foi_conectado": "Foi conectado",
    "valor_original": "Valor",
    "renda_cadastrada": "Renda Cadastrada",
    "hs_v2_date_entered_94896182": 'Date entered "Fila de atendimento (Funil.Vendas)"',
    "hs_v2_date_entered_94896183": 'Date entered "Tentativa de contato 1 (Funil.Vendas)"',
    "hs_v2_date_entered_94896184": 'Date entered "Tentativa de contato 2 (Funil.Vendas)"',
    "hs_v2_date_entered_94896185": 'Date entered "Tentativa de contato 3 (Funil.Vendas)"',
    "hs_v2_date_entered_94896186": 'Date entered "FUP atendimento (Funil.Vendas)"',
    "hs_v2_date_entered_94944032": 'Date entered "FUP docs (Funil.Vendas)"',
    "hs_v2_date_entered_94944033": 'Date entered "Pré-análise crédito (Funil.Vendas)"',
    "hs_v2_date_entered_94944035": 'Date entered "Análise crédito (Funil.Vendas)"',
    "email": "email",
    "motivo_de_perda_do_negocio": "Sub-motivo de perda do negócio",
    "analise_de_credito": "Motivo de Perda",
    "quentura_do_lead": "análi",
    "score_report": "Análise de crédito*",
    "score_report_2o_prop": "Probabilidade de Fechamento",
    "hs_lastmodifieddate": "Última modificação"
}
DEALSTAGE_MAP = {
    # Funil Vendas
    "94896180": "Cadastro recebido",
    "94896181": "Pré-atendimento",
    "94896182": "Fila de atendimento",
    "94896183": "Tentativa de contato 1",
    "94896184": "Tentativa de contato 2",
    "94896185": "Tentativa de contato 3",
    "94896186": "FUP atendimento",
    "94944037": "FUP busca imóvel",
    "94944038": "Pré-avaliação imóvel",
    "94944032": "FUP docs",
    "121912887": "Conferência de docs",
    "94944035": "Análise crédito",
    "94944034": "Cobrança docs faltantes",
    "944269102": "Revisão de docs",
    "94944036": "Comunicação crédito",
    "116619140": "Crédito ok - FUP busca imóvel",
    "94944039": "Crédito ok - Avaliação imóvel",
    "94944040": "FUP decisão do cliente",
    "129524824": "Transferência vendas <>ops",
    "157717619": "Vistoria",
    "157720469": "Pré-negociação",
    "94944042": "Negociar com vendedor",
    "94944044": "Fechamento negociação",
    "94944046": "Elaboração contrato",
    "94944047": "Leitura contrato",
    "94944045": "Update cliente",
    "94944048": "FUP assinaturas",
    "94944049": "Sucesso",
    "112199786": "Limbo",
    "94944050": "Negócio perdido",
    "94944033": "Pré-análise crédito",
    "234977811": "Pré-vendas - Tentativa de contato 1",
    "234977812": "Pré-vendas - Tentativa de contato 2",
    "234977813": "Pré-vendas - Tentativa de contato 3",
    "234977814": "Pré-vendas - Tentativa de contato 4",
    "234977815": "Pré-vendas - Tentativa de contato 5",
    "1095006102": "Autoatendimento",

    # Funil Indireto
    "107082496": "CADASTRO RECEBIDO",
    "107082498": "FILA DE ATENDIMENTO",
    "107082499": "TENTATIVA DE CONTATO 1",
    "107082500": "TENTATIVA DE CONTATO 2",
    "107082501": "TENTATIVA DE CONTATO 3",
    "107082502": "FUP ATENDIMENTO",
    "107082503": "FUP DOCS",
    "107082504": "PRÉ-ANÁLISE CRÉDITO",
    "107082505": "COBRANÇA DOCS FALTANTES",
    "107082506": "ANÁLISE CRÉDITO",
    "107082507": "COMUNICAÇÃO CRÉDITO",
    "107082508": "FUP BUSCA IMÓVEL",
    "107082509": "PRÉ-AVALIAÇÃO IMÓVEL",
    "107082510": "COMUNICAÇÃO PRÉ-PROPOSTA",
    "107082511": "FUP DECISÃO",
    "107082512": "VISTORIA AGENDADA",
    "107082513": "NEGOCIAR COM VENDEDOR",
    "107082515": "FECHAMENTO NEGOCIAÇÃO",
    "107082516": "ELABORAÇÃO CONTRATO",
    "107082517": "LEITURA CONTRATO",
    "107082514": "ENVIO PARA INVESTIDORES",
    "125208472": "ANÁLISE JURÍDICA",
    "107082518": "SUCESSO",
    "106400265": "NEGÓCIO PERDIDO",

    # Funil.MORAdor.Junior
    "146197058": "Pré-Diligência & Revisão de contrato",
    "146197059": "Assinatura com vendedor",
    "146197060": "Emissão das CNDs",
    "146197061": "Envio para Kevork",
    "146197062": "Solicitar docs adicionais",
    "146248779": "Agendamento da escritura",
    "146248780": "Pagamento imóvel/escritura",
    "146251670": "Entrega de chaves",
    "146197063": "MORAdor.Pleno",
    "146197064": "Negócio perdido",

    # Funil.MORAdor.Pleno
    "30003627": "Morador alocado",
    "35810737": "Análise CRI",
    "30003628": "Enviar CCV ao vendedor",
    "30003629": "Pagamento de sinal",
    "30003630": "Levantar documentos para análise jurídica",
    "30003631": "Solicitar emissão de parecer",
    "29994885": "Agendar data de escritura",
    "29994886": "Enviar documentos para cartório",
    "29994887": "Enviar minuta para vendedor",
    "29994888": "Pagar ITBI + Cartório",
    "29994889": "Comprar imóvel",
    "29994890": "Entrega de chaves",
    "29994891": "Solicitar documentos de transferência do imóvel",
    "29994892": "Avisar condomínio",
    "30003632": "Sucesso",
    "30003633": "Negócio perdido",

    # Imobiliárias <> aMORA
    "16361912": "Cadastrado",
    "16361913": "Tentativa de Contato 1",
    "16362602": "Tentativa de Contato 2",
    "16361916": "Conectado",
    "16362605": "Parceria em andamento",
    "16361918": "Negócio perdido",

    # Funil Direto
    "27460865": "Cadastro recebido",
    "27460866": "Cadastro alocado",
    "27460867": "Tentando contato",
    "27460868": "Contato com sucesso",
    "27460869": "Conectado",
    "27460870": "Buscando imóvel - Genérico",
    "27301698": "Buscando imóvel - Características definidas",
    "27301699": "Solicitação de documentos",
    "27301700": "Análise de crédito",
    "69483157": "Crédto ok - Sem imóvel",
    "27301701": "Análise do imóvel",
    "27301704": "Aprovação cliente",
    "27301702": "Comitê de crédito e imóvel",
    "27301703": "Aprovação investidores",
    "27301705": "Elaboração de contrato",
    "27301706": "Leitura de contrato",
    "27301707": "Aguardando assinaturas",
    "27301708": "Assinatura realizada",
    "27301709": "Sucesso",
    "27460871": "Negócio perdido",

    # Funil.Dev
    "33432345": "Cadastro recebido",
    "33432346": "Cadastro alocado",
    "33432347": "Tentando contato",
    "33432348": "Contato com sucesso",
    "33432349": "Conectado",
    "33432350": "Buscando imóvel - Genérico",
    "33432351": "Buscando imóvel - Características definidas",
    "33463042": "Solicitação de documentos",
    "33463043": "Análise de crédito",
    "33463044": "Análise do imóvel",
    "33463045": "Comitê de crédito e imóvel",
    "33463046": "Aprovação investidores",
    "33463047": "Aprovação cliente",
    "33463048": "Elaboração de contrato",
    "33463049": "Leitura de contrato",
    "33463050": "Aguardando assinaturas",
    "33463051": "Assinatura realizada",
    "33463052": "Sucesso",
    "33463053": "Negócio perdido"
}
OWNER_MAP = {
    "63328405": "Conta Geral aMORA",
    "63329317": "Aram Apovian",
    "63330382": "rafael cerqueira",
    "76317589": "Gabriel Okamoto",
    "79247225": "Maria Carolina Ferrari",
    "80147940": "Samuel Yuen",
    "80225754": "Beatriz Toffanin",
    "80225755": "Felipe Facio",
    "80628288": "Iasmin Martins",
    "80628289": "Lucas Casa Mausa",
    "80628290": "João Victor Ragazzi",
    "80925449": "Lucas Vaz",
    "80953049": "App amora",
    "80980248": "Arthur Cardoso",
    "83132452": "Romeu Piccolotto",
    "83430246": "Larissa Marinho",
    "83436113": "Kleber Brilhante",
    "96862115": "Gustavo Zago Canal",
    "105591154": "Juliana Narcizo",
    "123952167": "MANUELLA MAGALHÃES",
    "146490545": "Maria Clara",
    "253998216": "Nathalia Sincerre de Lemos",
    "286645898": "Ana Jacqueline",
    "309859619": "Ivan Pereira",
    "383132643": "Ricardo Lemos",
    "508280870": "Fathi-Alexandre de Souza Abid",
    "544053593": "Fernando Fochi",
    "586406042": "Gabriela Macedo",
    "659122948": "Operação Amora",
    "719022483": "Carlos Costa Neto",
    "1070149734": "Isabella Simões",
    "1324958526": "João Chade",
    "1430945138": "Jerônimo Rosado",
    "1587870116": "Vitor Araujo",
    "1617505258": "William Stanley",
    "2054951700": "Breno Feijo",
}
PURCHASE_MOMENT_MAP = {
    "has_property": "Quero comprar e já tenho imóvel definido",
    "visiting_property": "Estou visitando algumas opções",
    "beginning_journey": "Estou no começo da jornada",
    "curious": "Só quero saber como funciona"
}
FOI_CONECTADO_MAP = {
    "true": "Sim",
    "false": "Não"
}

# Chamadas
CHAMADAS_URL = "https://api.hubapi.com/crm/v3/objects/calls"
CHAMADAS_CSV = "data/atualizado/chamadas.csv"
PROPERTIES_CHAMADAS = [
    "hs_object_id", "hs_call_title", "hs_timestamp", "hs_call_direction",
    "hs_call_disposition", "hubspot_owner_id", "hs_call_duration",
    "hs_call_body", "hs_call_title_nome", "hs_call_primary_deal", "hs_call_deal_stage_during_call",
    "hs_lastmodifieddate"
]
API_TO_CSV_CHAMADAS = {
    "hs_object_id": "ID do objeto",
    "hs_call_title": "Título da chamada",
    "hs_timestamp": "Data da atividade",
    "hs_call_direction": "Direção da chamada",
    "hs_call_disposition": "Resultado da chamada",
    "hubspot_owner_id": "Atividade atribuída a",
    "hs_call_duration": "Duração da chamada (HH:mm:ss)",
    "hs_call_body": "Observações de chamada",
    "hs_call_primary_deal": "Associated Deal IDs",
    "hs_lastmodifieddate": "Última modificação"
}
CALL_DISPOSITION_MAP = {
    "9d9162e7-6cf3-4944-bf63-4dff82258764": "Ocupado",
    "f240bbac-87c9-4f6e-bf70-924b57d47db7": "Conectado",
    "a4c4c377-d246-4b32-a13b-75a56a4cd0ff": "Deixou mensagem ativa",
    "b2cf5968-551e-4856-9783-52b3da59a7d0": "Deixou mensagem de voz",
    "73a0d17f-1163-4015-bdd5-ec830791da20": "Sem resposta",
    "17b47fee-58de-441e-a44c-c6300d46f273": "Número errado",
    "032df4ca-3a9a-4c34-aed9-292d09616033": "Fora de área / Desligado",
    "8d94ac64-195e-43ba-8dc5-cbf810b56281": "Caixa postal"
}

def mapeia_valores(mapa: dict, propriedade_id: str):
    """
    Traduz o valor original com base em um dicionário de mapeamento.
    Se não encontrar, retorna o valor padrão (ou o próprio valor original).
    """

    if propriedade_id:
        return mapa.get(propriedade_id, propriedade_id)


def formata_data(data_iso):
    if not data_iso:
        return ""
    try:
        dt_utc = datetime.fromisoformat(data_iso.replace("Z", "+00:00"))
        dt_br = dt_utc.astimezone(BR_TZ)
        return dt_br.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return data_iso


def limpa_html(html: str) -> str:
    """
    Remove tags HTML e retorna texto limpo, com quebras de linha entre blocos e frases grudadas.
    """
    if not html:
        return ""

    soup = BeautifulSoup(html, "html.parser")

    # Substitui <br> por quebras de linha
    for br in soup.find_all("br"):
        br.replace_with("\n")

    # Adiciona \n após parágrafos <p>
    for p in soup.find_all("p"):
        if p.text and not p.text.endswith("\n"):
            p.append("\n")

    # Extrai texto limpo
    texto = soup.get_text(separator='', strip=True)

    # Adiciona quebra de linha entre letras minúsculas seguidas de maiúsculas
    texto_formatado = re.sub(r'(?<=[a-z])(?=[A-Z])', '\n', texto)

    return texto_formatado.strip()


def converte_ms_para_hms(ms):
    try:
        segundos = int(ms) // 1000
        horas = segundos // 3600
        minutos = (segundos % 3600) // 60
        segundos_restantes = segundos % 60
        return f"{horas:02}:{minutos:02}:{segundos_restantes:02}"
    except:
        return ""

def limpa_associated_deal_id(valor):
    """
    Remove o prefixo '0-3-' (ou similar) do ID do negócio e retorna apenas o número final.
    """
    if isinstance(valor, str) and '-' in valor:
        return valor.split('-')[-1]
    return valor


def coleta_dados_da_api(url: str, props: list, after_date: str) -> list:
    print(f"📥 Buscando dados da API após {after_date}...")

    data_corte = datetime.strptime(after_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    iso_date = data_corte.isoformat()

    resultados_brutos = []
    after = None

    while True:
        payload = {
            "filterGroups": [{
                "filters": [{
                    "propertyName": "hs_lastmodifieddate",
                    "operator": "GT",
                    "value": iso_date
                }]
            }],
            "properties": props,
            "limit": 100
        }
        if after:
            payload["after"] = after

        response = requests.post(f"{url}/search", headers={**HEADERS, "Content-Type": "application/json"}, data=json.dumps(payload))
        if not response.ok:
            raise Exception(f"Erro na API: {response.text}")

        data = response.json()
        registros = data.get("results", [])
        print(f"↪ Página com {len(registros)} registros")
        resultados_brutos.extend(registros)

        paging = data.get("paging", {}).get("next", {}).get("after")
        if paging:
            after = paging
        else:
            print("[3] Fim da paginação")
            break

    return resultados_brutos


def processa_dados(tipo: str, dados_brutos: list, mapa_api_to_csv: dict) -> list:
    resultados = []

    for item in dados_brutos:
        props_api = item.get("properties", {})
        props_csv = {mapa_api_to_csv.get(k, k): v for k, v in props_api.items()}

        if tipo == "negocios":
            # Traduzir dealstage
            dealstage_id = props_api.get("dealstage")
            if dealstage_id in DEALSTAGE_MAP:
                props_csv["Etapa do negócio"] = DEALSTAGE_MAP[dealstage_id]

            # Traduzir ids para nomes
            props_csv["Proprietário do negócio"] = mapeia_valores(OWNER_MAP, props_api.get("hubspot_owner_id"))
            props_csv["Momento de Compra"] = mapeia_valores(PURCHASE_MOMENT_MAP, props_api.get("purchase_moment"))

            # Traduzir foi_conectado
            foi_conectado_valor = props_api.get("foi_conectado")
            if foi_conectado_valor is not None:
                props_csv["Foi conectado"] = FOI_CONECTADO_MAP.get(str(foi_conectado_valor).lower(), "Não informado")

        if tipo == "chamadas":
            # Traduzir ids para nomes
            props_csv["Atividade atribuída a"] = mapeia_valores(OWNER_MAP, props_api.get("hubspot_owner_id"))
            props_csv["Resultado da chamada"] = mapeia_valores(CALL_DISPOSITION_MAP, props_api.get("hs_call_disposition"))

            # Formatação
            props_csv["Observações de chamada"] = limpa_html(props_api.get("hs_call_body"))
            props_csv["Duração da chamada (HH:mm:ss)"] = converte_ms_para_hms(props_api.get("hs_call_duration"))
            props_csv["Associated Deal IDs"] = limpa_associated_deal_id(props_api.get("hs_call_primary_deal"))

            # Remove "Chamada com" do título da chamada
            titulo_chamada = props_csv.get("Título da chamada", "")
            props_csv["Associated Deal"] = titulo_chamada.replace("Chamada com ", "").strip()

        # Formatar datas, limpar valores inválidos
        for campo_api in props_api:
            if "date" in campo_api.lower() or "timestamp" in campo_api.lower():
                valor_bruto = props_api[campo_api]
                nome_csv = mapa_api_to_csv.get(campo_api, campo_api)

                if isinstance(valor_bruto, str) and ("T" in valor_bruto or "-" in valor_bruto):
                    props_csv[nome_csv] = formata_data(valor_bruto)
                else:
                    props_csv[nome_csv] = ""

        # Garantir que todas as colunas existem, mesmo vazias
        for col in mapa_api_to_csv.values():
            props_csv.setdefault(col, "")

        resultados.append(props_csv)

    print(f"✅ Total processado ({tipo}): {len(resultados)}")
    return resultados


def ler_csv_existente(caminho_csv: str, mapa_api_to_csv: dict):
    print(f"📂 Carregando dados do CSV existente: {caminho_csv}...")
    if not os.path.exists(caminho_csv):
        print("⚠️  CSV ainda não existe. Criando novo.")
        return [], list(mapa_api_to_csv.values())  # usa nomes legíveis

    with open(caminho_csv, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        print(f"✅ {len(rows)} registros carregados do CSV.")
        return rows, reader.fieldnames


def atualiza_csv(tipo: str, caminho_csv: str, after_date: str, props: list, mapa_api_to_csv: dict, id_coluna: str):
    print(f"🚀 Iniciando atualização do CSV {tipo}...\n")

    # Coletar dados da API diretamente
    url = NEGOCIOS_URL if tipo == "negocios" else CHAMADAS_URL
    dados_brutos = coleta_dados_da_api(url, props, after_date)
    novos_dados = processa_dados(tipo, dados_brutos, mapa_api_to_csv)

    # Carregar CSV existente
    existentes, colunas = ler_csv_existente(caminho_csv, mapa_api_to_csv)
    colunas = list(colunas or [])

    if not colunas: 
        colunas = list(mapa_api_to_csv.values())

    id_map = {linha[id_coluna]: linha for linha in existentes}
    novos_count = 0
    atualizados_count = 0

    for novo in novos_dados:
        hs_id = novo.get(id_coluna)
        if hs_id in id_map:
            id_map[hs_id].update(novo)
            atualizados_count += 1
        else:
            existentes.insert(0, novo)
            novos_count += 1

    print(f"\n🧾 Atualizações concluídas:")
    print(f"🔁 {tipo} atualizados: {atualizados_count}")
    print(f"➕ Novos {tipo} adicionados: {novos_count} → {caminho_csv}")

    for row in existentes:
        for col in row:
            if col not in colunas:
                if col == "Associated Deal":
                    colunas.append(col)

    print(f"\n💾 Salvando dados no arquivo: {caminho_csv}...")
    with open(caminho_csv, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=colunas, quoting=csv.QUOTE_ALL)
        writer.writeheader()
        for row in existentes:
            linha_completa = {col: str(row.get(col, "") or "") for col in colunas}
            writer.writerow(linha_completa)

    print("✅ CSV salvo com sucesso!\n")


def main():
    hoje = datetime.now()

    # Subtrai 4 dias
    quatro_dias_atras = hoje - timedelta(days=4)

    # Formata para "YYYY-MM-DD"
    data_formatada = quatro_dias_atras.strftime("%Y-%m-%d")

    print("Data atual menos 4 dias:", data_formatada)
    DATA_CORTE = "2025-01-01"
    atualiza_csv("negocios", NEGOCIOS_CSV, data_formatada, PROPERTIES_NEGOCIOS, API_TO_CSV_NEGOCIOS, "ID do registro.")
    atualiza_csv("chamadas", CHAMADAS_CSV, data_formatada, PROPERTIES_CHAMADAS, API_TO_CSV_CHAMADAS, "ID do objeto")

if __name__ == "__main__":
    main()
