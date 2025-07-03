from fastapi import FastAPI, Query
from datetime import datetime, timezone
import requests
import os
from dotenv import load_dotenv

app = FastAPI() 

load_dotenv()
API_KEY = os.getenv("HUBSPOT_API_KEY")
HUBSPOT_URL = "https://api.hubapi.com/crm/v3/objects/deals"

@app.get("/hubspot/deals")
def get_deals(
    after: str = Query(..., description="Formato YYYY-MM-DD"),
    max_results: int = Query(None, description="Limite máximo de negócios retornados")
):
    try:
        data_corte = datetime.strptime(after, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return {"error": "Formato de data inválido. Use YYYY-MM-DD"}

    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {
        "limit": 100,
        "properties": [
            "hs_object_id",
            "dealname",
            "dealstage",
            "hubspot_owner_id",
            "createdate",
            "status_cadastro",
            "hs_tag_ids",
            "purchase_moment",
            "conectado",
            "amount",
            "renda_cadastrada",
            "hs_v2_date_entered_94896182",
            "hs_v2_date_entered_94896183",
            "hs_v2_date_entered_94896184",
            "hs_v2_date_entered_94896185",
            "hs_v2_date_entered_94896186",
            "hs_v2_date_entered_94944032",
            "hs_v2_date_entered_94944033",
            "hs_v2_date_entered_94944035",
            "email",
            "motivo_de_perda_do_negocio",
            "motivo_de_perda_do_negocio",
            "analise_de_credito",
            "analise_de_credito",
            "quentura_do_lead",
            "score_report",
            "score_report_2o_prop"
        ]
    }

    resultados_filtrados = []
    next_after = None

    while True:
        if next_after:
            params["after"] = next_after

        response = requests.get(HUBSPOT_URL, headers=headers, params=params)
        if not response.ok:
            return {"error": "Erro na API HubSpot", "detalhes": response.text}

        data = response.json()
        negocios = data.get("results", [])

        for negocio in negocios:
            modificado_str = negocio["properties"].get("hs_lastmodifieddate")
            if modificado_str:
                dt_modificado = datetime.fromisoformat(modificado_str.replace("Z", "+00:00"))
                if dt_modificado > data_corte:
                    resultados_filtrados.append(negocio)

            # Parar se atingiu o limite desejado
            if max_results and len(resultados_filtrados) >= max_results:
                return {
                    "deals_modificados": resultados_filtrados[:max_results],
                    "total": len(resultados_filtrados[:max_results]),
                    "limite_atingido": True
                }

        # Próxima página
        paging = data.get("paging")
        if paging and "next" in paging and "after" in paging["next"]:
            next_after = paging["next"]["after"]
        else: 
            break

    return {
        "deals_modificados": resultados_filtrados,
        "total": len(resultados_filtrados),
        "limite_atingido": False
    }
