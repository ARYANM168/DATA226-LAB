FROM apache/superset:4.1.2

USER root
# Add the Snowflake driver so Superset can connect to Snowflake
RUN if [ -x /app/.venv/bin/python ] && command -v uv >/dev/null 2>&1; then \
      uv pip install --python /app/.venv/bin/python "snowflake-sqlalchemy>=1.5,<2"; \
    else \
      pip install --no-cache-dir "snowflake-sqlalchemy>=1.5,<2"; \
    fi
USER superset
