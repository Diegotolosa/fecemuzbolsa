import os
import time
import datetime as dt
import requests
import xml.etree.ElementTree as ET

from supabase import create_client

BASE_URL = "https://gdcdyn.interactivebrokers.com/Universal/servlet/"


def _env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v


def flex_send_request(token: str, query_id: str, version: str = "3") -> str:
    url = f"{BASE_URL}FlexStatementService.SendRequest"
    r = requests.get(url, params={"t": token, "q": query_id, "v": version}, timeout=60)
    r.raise_for_status()
    root = ET.fromstring(r.text)

    status = (root.findtext(".//Status") or "").strip().lower()
    if status != "success":
        err = root.findtext(".//ErrorMessage") or r.text
        raise RuntimeError(f"Flex SendRequest failed: {err}")

    ref = (root.findtext(".//ReferenceCode") or "").strip()
    if not ref:
        raise RuntimeError("No ReferenceCode returned.")
    return ref


def flex_get_statement(token: str, reference_code: str, version: str = "3", max_wait_s: int = 120) -> str:
    url = f"{BASE_URL}FlexStatementService.GetStatement"
    start = time.time()

    while True:
        r = requests.get(url, params={"t": token, "q": reference_code, "v": version}, timeout=60)
        r.raise_for_status()
        txt = r.text.strip()

        if "<FlexStatement" in txt:
            return txt

        if time.time() - start > max_wait_s:
            raise RuntimeError("Flex statement not ready after waiting.")
        time.sleep(3)


def _num(x):
    try:
        return float(str(x).replace(",", "").strip())
    except Exception:
        return None


def main():
    flex_token = _env("FLEX_TOKEN")
    flex_query_id = _env("FLEX_QUERY_ID")
    supabase_url = _env("SUPABASE_URL")
    supabase_key = _env("SUPABASE_SERVICE_ROLE_KEY")

    sb = create_client(supabase_url, supabase_key)

    ref = flex_send_request(flex_token, flex_query_id)
    statement_xml = flex_get_statement(flex_token, ref)
    root = ET.fromstring(statement_xml)

    snapshot_date = dt.date.today()
    positions = []
    nav_eur = 0.0

    # Nota: usamos marketValueInBase (ideal si la base es EUR)
    for op in root.findall(".//OpenPosition"):
        mv_eur = _num(op.attrib.get("marketValueInBase"))
        if mv_eur is None:
            continue
        nav_eur += mv_eur

        positions.append({
            "snapshot_date": str(snapshot_date),
            "symbol": op.attrib.get("symbol") or None,
            "isin": op.attrib.get("isin") or None,
            "name": op.attrib.get("description") or None,
            "currency": op.attrib.get("currency") or None,
            "quantity": _num(op.attrib.get("position")),
            "avg_price": _num(op.attrib.get("averagePrice")),
            "last_price": _num(op.attrib.get("markPrice")),
            "market_value_eur": mv_eur,
            "unrealized_pnl_eur": _num(op.attrib.get("unrealizedPnLInBase")),
            "realized_pnl_eur": _num(op.attrib.get("realizedPnLInBase")),
            "weight": None,
        })

    for p in positions:
        p["weight"] = (p["market_value_eur"] / nav_eur) if nav_eur else None

    # Upsert posiciones
    if positions:
        sb.table("positions_daily").upsert(positions).execute()

    # Upsert snapshot agregado (público)
    sb.table("portfolio_snapshots").upsert([{
        "snapshot_date": str(snapshot_date),
        "base_currency": "EUR",
        "nav_eur": nav_eur,
    }]).execute()

    print(f"OK | date={snapshot_date} nav_eur={nav_eur:.2f} positions={len(positions)}")


if __name__ == "__main__":
    main()
