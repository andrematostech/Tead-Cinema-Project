try:
    from .openai_client import generate_sql_from_question
    from .openai_client import should_run_generated_sql
    from .sql_guard import validate_sql
    from .trino_client import run_select_query
except ImportError:
    from openai_client import generate_sql_from_question
    from openai_client import should_run_generated_sql
    from sql_guard import validate_sql
    from trino_client import run_select_query


def main() -> None:
    question = "Which movies have the highest revenue-to-budget ratio?"

    result = generate_sql_from_question(question)
    safe_sql = validate_sql(result.sql)

    print("QUESTION:")
    print(question)
    print("")
    print("GENERATED SQL:")
    print(result.sql)
    print("")
    print("SAFE SQL:")
    print(safe_sql)
    print("")
    print("CAN ANSWER:")
    print(result.can_answer)
    print("")
    print("EXPLANATION:")
    print(result.explanation)

    if should_run_generated_sql(result):
        dataframe = run_select_query(safe_sql)
        print("")
        print("RESULT PREVIEW:")
        print(dataframe.head().to_string(index=False))
    else:
        print("")
        print("RESULT PREVIEW:")
        print("Skipped because the question is not supported by the current Gold tables.")


if __name__ == "__main__":
    main()
