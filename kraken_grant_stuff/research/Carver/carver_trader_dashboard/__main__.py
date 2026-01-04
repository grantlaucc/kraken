import argparse, webbrowser
from .app import create_app

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--strategy-name", required=True, help="Strategy folder under ./strategies/")
    parser.add_argument("--port", type=int, default=5055)
    parser.add_argument("--no-open", action="store_true")
    args = parser.parse_args()
    app = create_app(args.strategy_name)
    url = f"http://127.0.0.1:{args.port}/"
    if not args.no_open:
        webbrowser.open(url, new=2)
    app.run(host="127.0.0.1", port=args.port, debug=False, use_reloader=False)

if __name__ == "__main__":
    main()