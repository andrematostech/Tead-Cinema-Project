ALLOWED_TABLES = {
    "movie_performance": {
        "full_name": "iceberg.gold.movie_performance",
        "description": "Gold table with movie-level performance metrics for the prototype.",
        "columns": [
            {"name": "movie_id", "type": "integer", "description": "Movie identifier."},
            {"name": "title", "type": "varchar", "description": "Movie title."},
            {"name": "release_year", "type": "integer", "description": "Movie release year."},
            {"name": "budget", "type": "bigint", "description": "Movie budget."},
            {"name": "revenue", "type": "bigint", "description": "Movie revenue."},
            {"name": "vote_average", "type": "double", "description": "Average vote score."},
            {"name": "vote_count", "type": "integer", "description": "Number of votes."},
            {"name": "roi", "type": "double", "description": "Revenue-to-budget return metric."},
            {"name": "performance_bucket", "type": "varchar", "description": "Simple performance label."},
        ],
    }
}


def get_allowed_table_names() -> list[str]:
    return [table["full_name"] for table in ALLOWED_TABLES.values()]


def get_supported_question_examples() -> list[str]:
    return [
        "Which movies have the highest revenue-to-budget ratio?",
        "Which movies have the highest revenue?",
        "What are the most profitable movies by ROI?",
        "Which release years have the highest average revenue?",
        "Which performance buckets contain the most movies?",
        "Which languages are associated with higher revenue?",
    ]


def build_schema_prompt_text() -> str:
    lines = [
        "You can query only the following Gold tables.",
        "",
    ]

    for table_name, table_info in ALLOWED_TABLES.items():
        lines.append(f"Table: {table_info['full_name']}")
        lines.append(f"Description: {table_info['description']}")
        lines.append("Columns:")

        for column in table_info["columns"]:
            lines.append(
                f"- {column['name']} ({column['type']}): {column['description']}"
            )

        lines.append("")

    return "\n".join(lines).strip()
