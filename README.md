# 📌 Projeto de Coleta e Processamento HubSpot

Este projeto coleta dados da API da HubSpot, processa e calcula o *lead time* das chamadas e atualiza automaticamente uma planilha do Google Sheets com os resultados.

## ▶️ Como executar localmente

1. **Instale as dependências:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Crie um arquivo `.env` veja o `.env.exemplo`:**
   ```bash
    HUBSPOT_API_KEY

    GOOGLE_CLIENT_SECRET_JSON
    GOOGLE_TOKEN_JSON
    ```

3. **Set up do ambiente virtual (recomendado):**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows use: venv\\Scripts\\activate
    pip install -r requirements.txt
    ```

4. **Execute o pipeline completo:**

    ```bash
    python3 -m app.api.main
    ```

## ⏰ Cronjob

Esse projeto contém um cronjob configurado para rodar automaticamente de hora em hora. O agendamento segue a seguinte linha:

```bash
0 * * * * moreamora/pipeline-negocios-chamadas && /usr/bin/cronitor exec main -- python3 -m app.api.main >> /tmp/main.log 2>&1
```
## Para acessar a documentação do projeto, veja o arquivo `main.md`