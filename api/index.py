from app.main import app
# Vercel looks for 'app' variable by default, or we specify handler in vercel.json.
# Usually 'handler = app' in vercel.json for Python.
handler = app
