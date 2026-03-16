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
