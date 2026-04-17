"""Parsea la nómina PDF del CRUCH 2026 → parquet con rut, monto_utm, universidad."""
import pdfplumber
import pandas as pd
from pathlib import Path
import time
import re

PDF = Path("/Users/antonio/Desktop/deudores_morosos.pdf")
OUT = Path("/Users/antonio/deudores-fscu/data/deudores_pdf.parquet")

DV_RE = re.compile(r"^[0-9kK]$")
NUM_RE = re.compile(r"^\d+$")

def parse_row(row):
    """Recibe lista de strings. Devuelve dict o None si no parsea."""
    # Esperado: [None|row_num, rut_num, dv, ap_p, ap_m, nombres, monto, '1', universidad]
    if not row or len(row) < 9:
        return None
    _, rut_num, dv, ap_p, ap_m, nombres, monto, periodos, univ = row[:9]
    if not rut_num or not NUM_RE.match(str(rut_num).strip()):
        return None
    if not dv or not DV_RE.match(str(dv).strip()):
        return None
    try:
        monto_f = float(str(monto).replace(",", "."))
    except (ValueError, TypeError):
        return None
    return {
        "rut": str(rut_num).strip(),
        "dv": str(dv).strip().upper(),
        "apellido_paterno": (ap_p or "").strip(),
        "apellido_materno": (ap_m or "").strip(),
        "nombres": (nombres or "").strip(),
        "monto_utm": monto_f,
        "periodos": int(periodos) if periodos and str(periodos).strip().isdigit() else None,
        "universidad": (univ or "").strip(),
    }

def main():
    pdf = pdfplumber.open(PDF)
    n = len(pdf.pages)
    print(f"PDF: {n} páginas", flush=True)
    rows = []
    t0 = time.time()
    for i, page in enumerate(pdf.pages):
        tables = page.extract_tables()
        for tbl in tables:
            for r in tbl:
                parsed = parse_row(r)
                if parsed:
                    rows.append(parsed)
        if (i + 1) % 50 == 0 or i == n - 1:
            dt = time.time() - t0
            rate = (i + 1) / dt
            eta = (n - i - 1) / rate
            print(f"[{i+1:>4}/{n}] rows={len(rows):,} rate={rate:.1f} p/s eta={eta/60:.1f}min", flush=True)
    pdf.close()

    df = pd.DataFrame(rows)
    df["rut_dv"] = df["rut"] + "-" + df["dv"]
    # Dedup por RUT (mismo deudor puede aparecer bajo varias universidades → conservar todas)
    df.to_parquet(OUT, index=False)
    print(f"\nEscrito {OUT}: {len(df):,} filas")
    print(f"RUTs únicos: {df['rut'].nunique():,}")
    print(f"RUTs con múltiples universidades: {(df.groupby('rut').size() > 1).sum():,}")
    print(f"Monto total (UTM): {df['monto_utm'].sum():,.0f}")
    print(f"Universidades distintas: {df['universidad'].nunique()}")
    print("\nTop 10 universidades por nº deudores:")
    print(df['universidad'].value_counts().head(10).to_string())

if __name__ == "__main__":
    main()
