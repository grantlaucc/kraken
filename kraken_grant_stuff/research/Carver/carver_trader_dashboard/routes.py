from flask import Blueprint, render_template, request, redirect, url_for, flash
from .config import SETTINGS
from .data.orders_store import (record_order, load_recent_orders, update_status_by_reqid, update_last_order_status,
                                set_order_id_by_reqid, update_status_by_order_id,)
from .data.positions import load_bases_from_position_file
from .services.ordmin import load_ordermin_map
from .services.order_exec import place_order, cancel_order

bp = Blueprint("dashboard", __name__)

def init_routes(app, *, ws_mgr, strategy_name: str):
    strategy_dir = app.config["STRATEGY_DIR"]
    allowed_quotes = SETTINGS.allowed_quotes
    ordmin_map = load_ordermin_map(SETTINGS.ordermin_csv_path)
    bases = load_bases_from_position_file(strategy_dir, allowed_quotes)+list(allowed_quotes)

    # Wire WS ack -> update CSV
    def on_add_order_ack(msg: dict):
        ok  = msg.get("success")
        res = msg.get("result") or {}
        err = msg.get("error") or res.get("error")
        req = msg.get("req_id")
        sym = msg.get("symbol") or (res.get("descr") or {}).get("pair") or ""
        oid = res.get("order_id")

        # 1) Persist order_id onto the row we created at submit-time
        if req is not None and oid:
            try:
                set_order_id_by_reqid(strategy_dir, int(req), str(oid))   # <-- NEW
            except Exception as e:
                print(f"[on_add_order_ack] set_order_id failed: {e}")

        # 2) Status update (prefer req_id, fallback to last-by-symbol)
        try:
            if req is not None:
                update_status_by_reqid(strategy_dir, int(req), "accepted" if ok else f"error: {err}")
            elif sym:
                update_last_order_status(strategy_dir, sym, "accepted" if ok else f"error: {err}")
        except Exception as e:
            print(f"[on_add_order_ack] update status failed: {e}")

    ws_mgr.on_add_order_ack = on_add_order_ack

    def on_cancel_order_ack(msg: dict):
        """
        Handle Kraken cancel_order acknowledgements and mark rows as 'canceled'.
        Be defensive about the shape of `result`.
        """
        ok  = bool(msg.get("success"))
        res = msg.get("result") or {}
        oid = res.get("order_id")
        err = msg.get("error") or res.get("error")

        if ok and oid:
            try:
                update_status_by_order_id(strategy_dir, str(oid), "canceled")
            except Exception as e:
                print(f"[on_cancel_order_ack] failed to persist canceled for {oid}: {e}")
        else:
            # Optional: if we got an id but it failed, reflect that in the row
            if oid:
                try:
                    update_status_by_order_id(strategy_dir, str(oid), f"cancel_error: {err or 'unknown'}")
                except Exception as e:
                    print(f"[on_cancel_order_ack] failed to persist cancel_error for {oid}: {e}")
            # Always log for diagnostics
            print(f"[on_cancel_order_ack] not ok. err={err} msg={msg}")
    ws_mgr.on_cancel_order_ack = on_cancel_order_ack

    @bp.route("/", methods=["GET"])
    def index():
        orders_df = load_recent_orders(strategy_dir)
        return render_template(
            "index.html",
            strategy_name=strategy_name,
            strategy_dir=strategy_dir,
            bases=bases,
            quotes=allowed_quotes,
            has_orders=not orders_df.empty,
            orders=orders_df.to_dict("records"),
            ws_ready=ws_mgr.connected.is_set(),
            ordmin_count=len(ordmin_map),
        )

    @bp.route("/submit", methods=["POST"])
    def submit_order():
        base  = (request.form.get("base") or "").strip().upper()
        quote = (request.form.get("quote") or "").strip().upper()
        side  = (request.form.get("side") or "BUY").strip().upper()
        otype = (request.form.get("order_type") or "market").strip().lower()
        qty_s = (request.form.get("qty") or "").strip()
        limit_s = (request.form.get("limit_price") or "").strip()
        validate = (request.form.get("validate") == "true")

        # parse/validate
        try: qty = float(qty_s)
        except: qty = None
        limit_price = None
        if otype == "limit" and limit_s:
            try: limit_price = float(limit_s)
            except:
                flash("Invalid limit price."); return redirect(url_for("dashboard.index"))

        if not base or quote not in allowed_quotes:
            flash("Invalid base/quote."); return redirect(url_for("dashboard.index"))
        if side not in {"BUY","SELL"} or qty is None or qty <= 0:
            flash("Invalid side/quantity."); return redirect(url_for("dashboard.index"))
        if otype not in {"market","limit"}:
            flash("Invalid order type."); return redirect(url_for("dashboard.index"))
        if otype == "limit" and limit_price is None:
            flash("Limit price required."); return redirect(url_for("dashboard.index"))

        min_qty = ordmin_map.get(base)
        if min_qty is not None and qty < float(min_qty):
            flash(f"Qty {qty} is below minimum {min_qty} for base {base}.")
            return redirect(url_for("dashboard.index"))

        symbol = f"{base}/{quote}"

        try:
            req_id = place_order(ws_mgr, symbol=symbol, side=side, order_type=otype,
                                 qty=qty, limit_price=limit_price, validate=validate)
            record_order(strategy_dir, asset=symbol, side=side, qty=qty,
                         order_type=otype, limit_price=limit_price, status="pending", req_id=req_id)
            flash(f"PENDING: {side} {qty:.8g} {symbol}" + (f" @ {limit_price}" if limit_price else ""))
        except Exception as e:
            record_order(strategy_dir, asset=symbol, side=side, qty=qty,
                         order_type=otype, limit_price=limit_price, status=f"error: {e}", req_id=None)
            flash(f"Order error: {e}")

        return redirect(url_for("dashboard.index"))

    @bp.route("/cancel", methods=["POST"])
    def cancel():
        order_id = (request.form.get("order_id") or "").strip()
        if not order_id:
            flash("Missing order_id.")
            return redirect(url_for("dashboard.index"))

        # Optimistic UI: show that we're canceling
        update_status_by_order_id(strategy_dir, order_id, "cancel_requested")

        try:
            cancel_order(ws_mgr, order_id=order_id)   # sends WS cancel with your token
            flash(f"Cancel requested for {order_id}.")
        except Exception as e:
            update_status_by_order_id(strategy_dir, order_id, f"cancel_error: {e}")
            flash(f"Cancel error: {e}")

        return redirect(url_for("dashboard.index"))
    
    app.register_blueprint(bp)
