from app.api.atualiza_2 import main as atualizar_dados
from app.services.leadtime import main as gerar_leadtime
from app.api.google_sheets import main as atualizar_google_sheets

def executar_pipeline_completo():
    print("🚀 Iniciando pipeline completo...")
    
    print("\n🔁 1. Atualizando dados...")
    atualizar_dados()

    print("\n🧱 2. Gerando leadtime...")
    gerar_leadtime()

    print("\n📊 3. Atualizando Sheets...")
    atualizar_google_sheets()

    print("\n✅ Pipeline finalizado com sucesso!")

if __name__ == "__main__":
    executar_pipeline_completo()
