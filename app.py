import requests
import pandas as pd
from datetime import datetime, timedelta
import os # Importe a biblioteca OS

# --- PEGAR CREDENCIAIS DE FORMA SEGURA ---
# Os tokens serão lidos das "Secrets" do GitHub
headers_betel = {
    "access-token": os.getenv("BETEL_ACCESS_TOKEN"),
    "secret-access-token": os.getenv("BETEL_SECRET_ACCESS_TOKEN"),
    "Content-Type": "application/json"
}

# Configurações do Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
SUPABASE_TABLE = "vendas"

headers_supabase = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

# --- O RESTANTE DO SEU CÓDIGO PERMANECE O MESMO ---

# Intervalo de datas
ontem = datetime.now() - timedelta(days=1)
data_inicio = data_fim = ontem.strftime("%Y-%m-%d")

# Obter lista de lojas
lojas_url = "https://api.beteltecnologia.com/api/lojas"
try:
    response_lojas = requests.get(lojas_url, headers=headers_betel)
    response_lojas.raise_for_status() # Lança um erro para respostas ruins (4xx ou 5xx)
    lojas = response_lojas.json().get("data", [])
except requests.exceptions.RequestException as e:
    print(f"Erro ao buscar lojas: {e}")
    lojas = []


# Lista para armazenar os dados
dados_vendas = []

# Loop pelas lojas
for loja in lojas:
    loja_id = loja["id"]
    nome_loja = loja["nome"]
    pagina = 1

    while True:
        url_vendas = f"https://api.beteltecnologia.com/vendas?tipo=vendas_balcao&data_inicio={data_inicio}&data_fim={data_fim}&loja_id={loja_id}&pagina={pagina}"
        
        try:
            response_vendas = requests.get(url_vendas, headers=headers_betel)
            response_vendas.raise_for_status()
            vendas_data = response_vendas.json()

            if vendas_data.get("code") != 200 or not vendas_data.get("data"):
                break

            for venda in vendas_data["data"]:
                for pagamento in venda.get("pagamentos", []):
                    valor_pagamento = pagamento.get("pagamento", {}).get("valor", 0.0)
                    forma_pagamento = pagamento.get("pagamento", {}).get("nome_forma_pagamento", "")

                    dados_vendas.append({
                        "Loja": nome_loja,
                        "cód": venda.get("codigo"),
                        "cliente": venda.get("nome_cliente"),
                        "Data": venda.get("data"),
                        "Prazo de entrega": venda.get("previsao_entrega"),
                        "situação": venda.get("nome_situacao"),
                        "valor_custo": venda.get("valor_custo"),
                        "valor_total": float(valor_pagamento) if valor_pagamento else None,
                        "canal": venda.get("nome_canal_venda"),
                        "Devolução": "Venda",
                        "Vendedor": venda.get("nome_vendedor"),
                        "pagamento": forma_pagamento,
                        "nf": venda.get("nota_fiscal_id"),
                        "valor_total_venda": venda.get("valor_total")
                    })

            proxima_pagina = vendas_data.get("meta", {}).get("proxima_pagina")
            if not proxima_pagina:
                break
            pagina = proxima_pagina
        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar vendas da loja {nome_loja}: {e}")
            break # Pula para a próxima loja em caso de erro

# Criar DataFrame e processar
if dados_vendas:
    df = pd.DataFrame(dados_vendas)

    # Função de sanitização removida pois a conversão já é feita na criação
    
    # Enviar para o Supabase
    data_json = df.to_dict(orient="records")

    print(f"Enviando {len(data_json)} registros para o Supabase...")
    for i in range(0, len(data_json), 50):
        batch = data_json[i:i+50]
        try:
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                headers=headers_supabase,
                json=batch
            )
            response.raise_for_status()
            print(f"✅ Lote {i//50 + 1} inserido com sucesso!")
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro ao inserir lote {i//50 + 1}: {e} - {response.text}")

    print("📦 Upload concluído!")
else:
    print("Nenhum dado de venda encontrado para processar.")