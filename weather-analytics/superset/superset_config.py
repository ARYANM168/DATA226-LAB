import os

# Required by Superset; change it if you ever expose this beyond your laptop
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "weather-local-superset-secret-change-me")

# Keep Superset's own metadata (users, charts, dashboards) in a persistent volume
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Local dev: no CSRF hassles with the SQL Lab / chart builder
WTF_CSRF_ENABLED = False
