import vertexai

from vertexai.preview.generative_models import GenerativeModel
from app.config import settings
from sql_model.prompt_template import build_prompt

vertexai.init(project=settings.PROJECT_ID, location=settings.VERTEX_LOCATION)

MODEL_NAME = settings.EXTRACTOR_MODEL
model = GenerativeModel(MODEL_NAME)



def generate_sql(question: str, keywords: list):
    prompt = build_prompt(question, keywords)

    try:
        response = model.generate_content(
            prompt,
            generation_config={
                "temperature": 0.0,
                "top_p": 1.0,
                "max_output_tokens": 512,
            }
        )

        sql_text = response.text.strip()
        sql_text = sql_text.replace("```sql", "").replace("```", "").strip()
        return sql_text

    except Exception as e:
        print("Vertex SQL generation error:", e)
        return "SELECT 'SQL generation failed' AS error_message;"
