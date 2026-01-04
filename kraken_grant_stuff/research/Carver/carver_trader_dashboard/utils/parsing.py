def extract_base(col: str, allowed_quotes: tuple[str, ...]) -> str | None:
    s = col.strip().upper()
    for sep in ("/", "_", "-"):
        parts = [p for p in s.split(sep) if p]
        if len(parts) >= 2:
            for p in parts[1:]:
                p_alpha = "".join(ch for ch in p if ch.isalpha())
                if p_alpha in allowed_quotes:
                    return parts[0]
    for q in sorted(allowed_quotes, key=len, reverse=True):
        if s.endswith(q) and len(s) > len(q):
            return s[:-len(q)]
    if s.isalpha() and s not in {"CASH", "PORTFOLIO", "VALUE", "NOTIONAL", "PORTFOLIO_VALUE"}:
        return s
    return None
