from flask import Flask
from flask_cors import CORS

cors = CORS()


def create_app():
    app = Flask(__name__)
    cors.init_app(
        app,
        supports_credentials=True,
        origins=["http://127.0.0.1:3001"],
    )

    from backend.endpoints import endpoints

    app.register_blueprint(endpoints)
    return app
