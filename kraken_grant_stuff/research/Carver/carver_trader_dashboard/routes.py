from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from .config import get_settings
from .data.orders_store import (record_order, load_recent_orders, update_status_by_reqid, update_last_order_status,
                                set_order_id_by_reqid, update_status_by_order_id, get_asset_by_order_id,)
from .data.positions import load_bases_from_position_file
from .data import hl_trade_history
from .services.ordmin import load_ordermin_map, load_hyperliquid_ordermin_map
from .services.order_exec import place_order, cancel_order

bp = Blueprint("dashboard", __name__)

def init_routes(app, *, ws_mgr, strategy_name: str, venue: str = "kraken", get_trade_plan=None,
                on_fill=None, on_funding=None):
    """
    venue: "kraken" or "hyperliquid" -- fixed for this process's whole lifetime (one dashboard =
    one venue, see carver_trader.py's --venue flag). Drives symbol format (Kraken BASE/QUOTE vs.
    Hyperliquid base-only), order-min source, and which WS manager's ack/update hooks get wired.

    on_fill/on_funding: optional *extra* callables invoked alongside this module's own
    hl_trade_history recording (which always runs for venue="hyperliquid" -- persisting fills/
    funding isn't optional behavior a caller has to remember to wire up).
    """
    strategy_dir = app.config["STRATEGY_DIR"]
    settings = get_settings(venue)
    allowed_quotes = settings.allowed_quotes
    if venue == "kraken":
        ordmin_map = load_ordermin_map(settings.ordermin_csv_path)
    else:
        ordmin_map = load_hyperliquid_ordermin_map()
    bases = load_bases_from_position_file(strategy_dir, allowed_quotes)+list(allowed_quotes)

    if venue == "kraken":
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

    else:  # hyperliquid
        # Hyperliquid has one unified order-status channel (orderUpdates) rather than separate
        # add/cancel acks -- it's the authoritative follow-up to whatever order_exec.place_order/
        # cancel_order already set synchronously from the REST response. Shape is defensive
        # (logs and skips) since it hasn't been observed against a real fill yet -- see the plan.
        def on_hl_order_update(upd: dict):
            try:
                order_info = upd.get("order", {})
                oid = order_info.get("oid")
                status = upd.get("status")
                if oid is None or status is None:
                    print(f"[on_hl_order_update] unrecognized shape: {upd}")
                    return
                update_status_by_order_id(strategy_dir, str(oid), str(status))
            except Exception as e:
                print(f"[on_hl_order_update] failed: {e} ({upd})")
        ws_mgr.on_order_update = on_hl_order_update

        def on_hl_fill(fill: dict):
            try:
                hl_trade_history.record_fill(strategy_dir, fill)
            except Exception as e:
                print(f"[on_hl_fill] failed to record fill: {e} ({fill})")
            if on_fill is not None:
                on_fill(fill)
        ws_mgr.on_fill = on_hl_fill

        def on_hl_funding(funding: dict):
            try:
                hl_trade_history.record_funding(strategy_dir, funding)
            except Exception as e:
                print(f"[on_hl_funding] failed to record funding: {e} ({funding})")
            if on_funding is not None:
                on_funding(funding)
        ws_mgr.on_funding = on_hl_funding

    @bp.route("/", methods=["GET"])
    def index():
        orders_df = load_recent_orders(strategy_dir)
        trade_plan = get_trade_plan() if get_trade_plan else {}
        account = getattr(ws_mgr, "account", None)  # HyperliquidAccount, only set for venue="hyperliquid"

        fills, funding, pnl = [], [], None
        if venue == "hyperliquid":
            fills = hl_trade_history.load_recent_fills(strategy_dir).to_dict("records")
            funding = hl_trade_history.load_recent_funding(strategy_dir).to_dict("records")
            pnl = hl_trade_history.summarize_pnl(strategy_dir)

        return render_template(
            "index.html",
            strategy_name=strategy_name,
            strategy_dir=strategy_dir,
            venue=venue,
            bases=bases,
            quotes=allowed_quotes,
            has_orders=not orders_df.empty,
            orders=orders_df.to_dict("records"),
            ws_ready=ws_mgr.connected.is_set(),
            ordmin_count=len(ordmin_map),
            trade_plan=trade_plan,
            positions=(account.positions if account else {}),
            account_value=(account.account_value if account else None),
            withdrawable=(account.withdrawable if account else None),
            fills=fills,
            funding=funding,
            pnl=pnl,
        )

    @bp.route("/trade_plan.json", methods=["GET"])
    def trade_plan_json():
        return jsonify(get_trade_plan() if get_trade_plan else {})

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

        # Hyperliquid perps are base-only (USDC-margined, no quote leg); Kraken needs BASE/QUOTE.
        symbol = base if venue == "hyperliquid" else f"{base}/{quote}"

        try:
            req_id, status, order_id = place_order(ws_mgr, venue=venue, symbol=symbol, side=side, order_type=otype,
                                 qty=qty, limit_price=limit_price, validate=validate)
            record_order(strategy_dir, asset=symbol, side=side, qty=qty,
                         order_type=otype, limit_price=limit_price, status=status, req_id=req_id)
            if order_id:
                set_order_id_by_reqid(strategy_dir, req_id, order_id)
            status_label = status.upper() if not status.startswith("error") else status
            flash(f"{status_label}: {side} {qty:.8g} {symbol}" + (f" @ {limit_price}" if limit_price else ""))
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
            if venue == "hyperliquid":
                coin = get_asset_by_order_id(strategy_dir, order_id)
                if not coin:
                    raise RuntimeError(f"No recorded asset for order_id {order_id}; can't derive Hyperliquid asset index.")
                cancel_order(ws_mgr, venue=venue, order_id=order_id, coin=coin)
                update_status_by_order_id(strategy_dir, order_id, "canceled")
            else:
                cancel_order(ws_mgr, venue=venue, order_id=order_id)   # sends WS cancel with your token
            flash(f"Cancel requested for {order_id}.")
        except Exception as e:
            update_status_by_order_id(strategy_dir, order_id, f"cancel_error: {e}")
            flash(f"Cancel error: {e}")

        return redirect(url_for("dashboard.index"))

    app.register_blueprint(bp)
