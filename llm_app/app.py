import streamlit as st

try:
    from .openai_client import generate_sql_from_question
    from .openai_client import should_run_generated_sql
    from .schema_context import get_allowed_table_names, get_supported_question_examples
    from .sql_guard import validate_sql
    from .trino_client import run_select_query
except ImportError:
    from openai_client import generate_sql_from_question
    from openai_client import should_run_generated_sql
    from schema_context import get_allowed_table_names, get_supported_question_examples
    from sql_guard import validate_sql
    from trino_client import run_select_query


st.set_page_config(page_title="TEAD LLM Analytics", layout="wide")


def format_result_df(result_df):
    display_df = result_df.copy()

    if "roi" in display_df.columns:
        display_df["roi"] = display_df["roi"].round(3)

    return display_df

st.title("TEAD Movie Analytics Assistant")
st.write("Ask a question about the Gold movie analytics table.")

st.caption("Currently supported Gold tables: " + ", ".join(get_allowed_table_names()))

with st.expander("Example questions"):
    for example in get_supported_question_examples():
        st.write(f"- {example}")

selected_example = st.selectbox(
    "Quick example",
    ["Choose an example..."] + get_supported_question_examples(),
    index=0,
)

if selected_example != "Choose an example...":
    st.session_state["question_input"] = selected_example

question = st.text_input(
    "Question",
    placeholder="Which movies have the highest revenue-to-budget ratio?",
    key="question_input",
)

run_button = st.button("Run")

if run_button and question.strip():
    try:
        with st.spinner("Generating SQL ..."):
            generation = generate_sql_from_question(question)

        st.subheader("Generated SQL")
        st.code(generation.sql, language="sql")

        safe_sql = validate_sql(generation.sql)

        st.subheader("Validated SQL")
        st.code(safe_sql, language="sql")

        st.subheader("Explanation")
        st.write(generation.explanation)

        if should_run_generated_sql(generation):
            with st.spinner("Running query in Trino ..."):
                result_df = run_select_query(safe_sql)

            st.subheader("Results")
            st.caption("Source: iceberg.gold.movie_performance")
            st.caption(f"Returned {len(result_df)} row(s).")

            if result_df.empty:
                st.info("The query ran successfully, but it returned no rows.")
            else:
                st.dataframe(format_result_df(result_df), use_container_width=True, hide_index=True)
        else:
            st.warning("This question is not fully answerable with the current Gold tables.")
            st.info("Try a movie-level question about ROI, revenue, release year, votes, or performance buckets.")

    except Exception as exc:
        st.error(str(exc))
elif run_button:
    st.warning("Please enter a question.")
