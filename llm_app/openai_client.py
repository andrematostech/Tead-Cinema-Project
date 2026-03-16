import os

from dotenv import load_dotenv
from openai import APIConnectionError
from openai import APIStatusError
from openai import APITimeoutError
from openai import OpenAI
from pydantic import BaseModel, Field

try:
    from .prompts import build_sql_system_prompt
except ImportError:
    from prompts import build_sql_system_prompt


load_dotenv()


class SqlGenerationResult(BaseModel):
    sql: str = Field(description="A single safe SELECT query.")
    can_answer: bool = Field(description="Whether the available Gold table can answer the question.")
    explanation: str = Field(description="A short plain-English note about the generated query.")


def should_run_generated_sql(result: SqlGenerationResult) -> bool:
    return result.can_answer


def get_openai_client() -> OpenAI:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY is not set.")

    return OpenAI(api_key=api_key)


def get_model_name() -> str:
    return os.getenv("OPENAI_MODEL", "gpt-5-mini")


def get_openai_timeout() -> float:
    return float(os.getenv("OPENAI_TIMEOUT_SECONDS", "60"))


def normalize_generation_result(result: SqlGenerationResult) -> SqlGenerationResult:
    sql = result.sql.strip()
    explanation = result.explanation.strip()

    if not sql:
        raise ValueError("The OpenAI response did not include SQL.")

    if not explanation:
        raise ValueError("The OpenAI response did not include an explanation.")

    return SqlGenerationResult(
        sql=sql,
        can_answer=result.can_answer,
        explanation=explanation,
    )


def generate_sql_from_question(question: str) -> SqlGenerationResult:
    question = question.strip()

    if not question:
        raise ValueError("Question cannot be empty.")

    client = get_openai_client()

    try:
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
            timeout=get_openai_timeout(),
        )
    except APITimeoutError as exc:
        raise ValueError("The OpenAI request timed out. Try again in a moment.") from exc
    except APIConnectionError as exc:
        raise ValueError("Could not reach the OpenAI API. Check your network connection.") from exc
    except APIStatusError as exc:
        raise ValueError(f"OpenAI API returned an error: {exc.status_code}") from exc

    if response.output_parsed is None:
        raise ValueError("The OpenAI response could not be parsed into the expected SQL format.")

    return normalize_generation_result(response.output_parsed)
