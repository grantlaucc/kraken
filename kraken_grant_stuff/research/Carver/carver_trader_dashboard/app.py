import os
from flask import Flask
from .config import SETTINGS
from .services.ws_manager import WSManager
from .routes import init_routes


def create_app(strategy_name: str):
    app = Flask(__name__)
    app.secret_key = SETTINGS.secret_key
    strategy_dir = os.path.join("strategies", strategy_name)
    os.makedirs(strategy_dir, exist_ok=True)
    app.config["STRATEGY_DIR"] = strategy_dir

    ws_mgr = WSManager()
    ws_mgr.start()  # start WS thread immediately
    init_routes(app, ws_mgr=ws_mgr, strategy_name=strategy_name)
    return app
