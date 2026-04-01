import logging
import os
import time
import json 
import base64
from datetime import datetime
from typing import Optional, Dict, Any

from google import genai
from google.genai import types
from dotenv import load_dotenv

# Configure logging to match the requested pattern
# INFO - YYYY-MM-DD HH:MM:SS - Message
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class GeminiDataExtractor:
    def __init__(self):
        load_dotenv()
        self.api_key = os.environ.get('GOOGLE_CLOUD_API_KEY')
        if not self.api_key:
            raise ValueError("Environment variable GOOGLE_CLOUD_API_KEY is not set.")
        
        self.client = genai.Client(api_key=self.api_key)

    def read_text_file(self, path: str) -> str:
        """Reads local prompt files with error handling."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logger.error(f"Error reading file {path}: {str(e)}")
            raise

    def process_pdf(
        self,
        pdf_bytes: bytes,  # Agora recebendo bytes decodificados
        file_name: str,    # Nome do arquivo para log
        system_prompt: str,
        user_prompt: str,
        model_name: str = "gemini-2.0-flash",
        temperature: float = 0.1,
        top_k: Optional[int] = None,
        output_schema: Optional[Dict[str, Any]] = None
        ) -> Optional[str]:
        
        start_time = time.time()
        logger.info(f"[{file_name}] - Extraction started via bytes stream.")

        try:
            safety_settings = [
                types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF")
            ]

            config_kwargs = {
                "temperature": temperature,
                "system_instruction": [types.Part.from_text(text=system_prompt)],
                "response_mime_type": "application/json",
                "safety_settings": safety_settings,
            }

            if top_k is not None:
                config_kwargs["top_k"] = top_k
            
            if output_schema:
                config_kwargs["response_schema"] = output_schema

            logger.info(f"[{file_name}] - Payload ready. Calling model {model_name}...")

            # API Call usando os bytes
            response = self.client.models.generate_content(
                model=model_name,
                contents=[
                    types.Part.from_text(text=user_prompt), 
                    types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")
                ],
                config=types.GenerateContentConfig(**config_kwargs)
            )

            execution_duration = time.time() - start_time
            logger.info(f"[{file_name}] - Successfully processed by LLM in {execution_duration:.2f}s")
            
            return response.text

        except Exception as e:
            logger.error(f"[{file_name}] - Critical error during Gemini extraction: {str(e)}")
            return None

extractor = GeminiDataExtractor()

try:
    SYSTEM_PROMPT_CACHE = extractor.read_text_file('system_prompt.txt')
    USER_PROMPT_CACHE = extractor.read_text_file('prompt.txt')
    logger.info("Prompts loaded into memory successfully.")
except Exception as e:
    logger.error(f"Failed to cache prompts at startup: {e}")

def run_pipeline(request):
    """
    HTTP Cloud Function entry point.
    Request Example (JSON):
    {
        "pdf": "string_pdf_bytes",
        "file_name": "file_name",
        "model_name": "gemini-2.5-flash",
        "temperature": 0.2,
        "top_k": 20, 
        "output_schema" : "output_schema"
    }
    """

    request_json = request.get_json(silent=True) or {}

    # 1. Parâmetros Obrigatórios
    pdf_base64 = request_json.get('pdf') # Chave 'pdf' conforme seu teste
    file_name = request_json.get('file_name', 'unknown_document.pdf')

    if not pdf_base64:
        logger.warning(f"[{file_name}] - Validation failed: Missing 'pdf' field in JSON request")
        return json.dumps({"error": "Mandatory parameter 'pdf' is missing."}), 400

    # 2. Parâmetros Opcionais
    model_name = request_json.get('model_name', 'gemini-2.5-flash')
    temperature = float(request_json.get('temperature', 0.1))
    top_k = int(request_json.get('top_k', 40))
    output_schema = request_json.get('output_schema', None)

    try:
        # 3. Fail-safe: Verificar se os prompts foram carregados
        if not SYSTEM_PROMPT_CACHE or not USER_PROMPT_CACHE:
            logger.error(f"[{file_name}] - Execution failed: Prompts not found in cache.")
            return json.dumps({"error": "System configuration error: Prompts missing."}), 500

        # 4. Decode
        pdf_bin_bytes = base64.b64decode(pdf_base64)

        # 5. Execute extraction
        result_text = extractor.process_pdf(
            pdf_bytes=pdf_bin_bytes,
            file_name=file_name, 
            system_prompt=SYSTEM_PROMPT_CACHE,
            user_prompt=USER_PROMPT_CACHE,
            model_name=model_name,
            temperature=temperature,
            top_k=top_k,
            output_schema=output_schema
        )

        if result_text is None:
            return json.dumps({"error": "Processing failed at LLM level"}), 500

        # 6. Parse final e resposta
        try:
            final_data = json.loads(result_text)
            logger.info(f"[{file_name}] - Finished copying local file data to JSON response.")
            return json.dumps(final_data), 200, {'Content-Type': 'application/json'}
        except json.JSONDecodeError:
            logger.error(f"[{file_name}] - LLM returned invalid JSON.")
            return json.dumps({"error": "Invalid JSON format from LLM"}), 500

    except Exception as e:
        logger.error(f"[{file_name}] - Fatal pipeline error: {str(e)}")
        return json.dumps({"error": "Internal Server Error", "details": str(e)}), 500