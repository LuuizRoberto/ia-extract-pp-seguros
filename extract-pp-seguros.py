import base64, requests, json, logging
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# Configuração de Logging no padrão solicitado
logging.basicConfig(
    level=logging.INFO,
    format='INFO - %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# CONFIGURAÇÃO
URL = "https://ia-extract-pp-seguros-415207976946.europe-west1.run.app"
KEY_FILE = "chave.json"
PDF_FILE = "Proposta de Seguro.pdf"

# 1. GERAR TOKEN E LOGAR
logger.info(f"Generating Google ID Token using {KEY_FILE}...")
creds = service_account.IDTokenCredentials.from_service_account_file(KEY_FILE, target_audience=URL)
creds.refresh(Request())
token = creds.token

# 2. CONVERTER PDF E LOGAR
logger.info(f"Reading and encoding local file: {PDF_FILE}")
with open(PDF_FILE, "rb") as f:
    pdf_b64 = base64.b64encode(f.read()).decode()

# 3. ENVIAR REQUISIÇÃO E LOGAR
logger.info(f"Sending request to Cloud Function at {URL}")
payload = {"pdf": pdf_b64, "file_name": PDF_FILE}
headers = {"Authorization": f"Bearer {token}"}

response = requests.post(URL, json=payload, headers=headers)

logger.info(f"Response received from server. Status Code: {response.status_code}")

# 4. EXIBIR RESULTADO FINAL
print("\n--- RESULTADO DA EXTRAÇÃO ---")
if response.status_code == 200:
    print(json.dumps(response.json(), indent=4, ensure_ascii=False))
else:
    print(f"Error: {response.text}")