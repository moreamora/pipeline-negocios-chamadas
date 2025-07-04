from app.api.atualizar_negocios_chamadas import main as atualiza_dados
from app.services.merge_negocios_chamadas import main as merge_dados
from app.api.exportar_para_sheets import main as atualiza_google_sheets

def executar_pipeline_completo():
    print("🚀 Iniciando pipeline completo...")
    
    print("\n🔁 1. Atualizando dados...")
    atualiza_dados()

    print("\n🧱 2. Juntando csvs e calculando leadtime...")
    merge_dados()

    print("\n📊 3. Atualizando Sheets...")
    atualiza_google_sheets()

    print("\n✅ Pipeline finalizado com sucesso!")

if __name__ == "__main__":
    executar_pipeline_completo()
