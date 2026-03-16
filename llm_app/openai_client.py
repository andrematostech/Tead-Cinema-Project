import os

from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field

from prompts import build_sql_system_prompt


load_dotenv()


class SqlGenerationResult(BaseModel):
    sql: str = Field(description="A single safe SELECT query.")
    can_answer: bool = Field(description="Whether the available Gold table can answer the question.")
    explanation: str = Field(description="A short plain-English note about the generated query.")


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set.")

    return OpenAI(api_key=api_key)


def get_model_name() -> str:
    return os.getenv("OPENAI_MODEL", "gpt-5-mini")


def generate_sql_from_question(question: str) -> SqlGenerationResult:
    client = get_openai_client()

    response = client.responses.parse(
        model=get_model_name(),
        input=[
            {
                "role": "system",
                "content": build_sql_system_prompt(),
            },
            {
                "role": "user",
                "content": question,
            },
        ],
        text_format=SqlGenerationResult,
    )

    return response.output_parsed
