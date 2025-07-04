import requests
import json
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("HUBSPOT_API_KEY")
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

def descobrir_nome_etapa_por_id(stage_id):
    """
    Procura em todos os pipelines de deals o nome da etapa com ID fornecido.
    """
    url = "https://api.hubapi.com/crm/v3/pipelines/deals"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    for pipeline in data["results"]:
        pipeline_id = pipeline["id"]
        pipeline_label = pipeline["label"]
        for stage in pipeline["stages"]:
            if stage["id"] == stage_id:
                print(f"✅ ID encontrado no pipeline '{pipeline_label}' (ID: {pipeline_id}):")
                print(f'🔹 "{stage["id"]}": "{stage["label"]}"')
                return
    print("❌ ID não encontrado em nenhum pipeline.")


def listar_estagios_pipeline(pipeline_id):
    """
    Lista todas as etapas de um pipeline de negócios com base no pipeline_id.
    """
    url = f"https://api.hubapi.com/crm/v3/pipelines/deals/{pipeline_id}"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    print(f"\n📋 Etapas do pipeline {pipeline_id}:\n")
    for stage in data["stages"]:
        print(f'"{stage["id"]}": "{stage["label"]}",')


def listar_propriedades_calls():
    """
    Lista todas as propriedades disponíveis no objeto de chamadas (calls).
    """
    url = "https://api.hubapi.com/crm/v3/properties/calls"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    print("📋 Propriedades disponíveis para chamadas:\n")
    for prop in data.get("results", []):
        nome = prop.get("name")
        rotulo = prop.get("label")
        print(f"- {nome}: {rotulo}")


def mostrar_opcoes_propriedade_calls(property_name):
    """
    Exibe os valores possíveis de uma propriedade de chamada do tipo enum (ex: hs_call_disposition).
    """
    url = f"https://api.hubapi.com/crm/v3/properties/calls/{property_name}"
    response = requests.get(url, headers=HEADERS)

    if response.status_code != 200:
        print(f"Erro {response.status_code} ao buscar a propriedade '{property_name}'")
        try:
            print("Resposta:", response.json())
        except Exception:
            print("Resposta não é um JSON válido:", response.text)
        return

    data = response.json()

    options = data.get("options", [])
    if not options:
        print(f"⚠️ Nenhuma opção encontrada para '{property_name}' (talvez não seja do tipo enum?)")
        return

    print(f"\n📋 Opções disponíveis para '{property_name}':\n")
    for option in options:
        print(f'"{option["value"]}": "{option["label"]}",')

def gerar_owner_map():
    url = "https://api.hubapi.com/crm/v3/owners/"
    response = requests.get(url, headers=HEADERS)

    if not response.ok:
        print(f"❌ Erro {response.status_code}: {response.text}")
        return

    data = response.json()

    owner_map = {}
    for owner in data.get("results", []):
        owner_id = str(owner.get("id"))
        nome = owner.get("firstName", "") + " " + owner.get("lastName", "")
        nome = nome.strip() or owner.get("email", "")  # fallback para email se nome estiver vazio
        owner_map[owner_id] = nome

    print("\n📋 OWNER_MAP gerado:\n")
    print("OWNER_MAP = {")
    for k, v in owner_map.items():
        print(f'    "{k}": "{v}",')
    print("}")



url = "https://api.hubapi.com/calling/v1/dispositions"
headers = {"Authorization": f"Bearer {API_KEY}"}

response = requests.get(url, headers=headers)

if response.ok:
    data = response.json()
    disposition_map = {item["id"]: item["label"] for item in data}
    print("Mapa de disposições:\n")
    for k, v in disposition_map.items():
        print(f"{k}: {v}")
else:
    print(f"Erro {response.status_code}: {response.text}")


# descobrir_nome_etapa_por_id("33463045")
# listar_estagios_pipeline("253998216")
# listar_propriedades_calls()
# mostrar_opcoes_propriedade_calls("hubspot_owner_id")
# gerar_owner_map()
