"""Extrae RUTs y datos personales del Excel FSCU a parquet."""
import openpyxl
import pandas as pd
from pathlib import Path

SRC = Path("/Users/antonio/Desktop/Deudores_2024-04-17.xlsx")
OUT = Path("/Users/antonio/deudores-fscu/data")

def load_sheet(sheet_name: str, out_name: str):
    wb = openpyxl.load_workbook(SRC, read_only=True)
    ws = wb[sheet_name]
    rows = ws.iter_rows(values_only=True)
    header = next(rows)
    df = pd.DataFrame(rows, columns=header)
    out = OUT / f"{out_name}.parquet"
    df.to_parquet(out, index=False)
    print(f"{sheet_name}: {len(df):,} rows -> {out}")
    return df

if __name__ == "__main__":
    OUT.mkdir(parents=True, exist_ok=True)
    load_sheet("Información Personal", "deudores_personal")
    load_sheet("Información Vehicular", "deudores_vehiculos")
    load_sheet("Información Propiedades", "deudores_propiedades")
