# Deudores FSCU · DecideChile

Dashboard público agregado y anonimizado de la nómina de deudores morosos del **Fondo Solidario de Crédito Universitario** (FSCU) publicada por el Consejo de Rectoras y Rectores de las Universidades Chilenas para el año tributario 2026.

**Live:** https://deudores-fscu.vercel.app/

## Arquitectura

- **Pipeline offline (Python + DuckDB):** toma la nómina oficial + enriquecimiento con patrimonio (SII/Servel) + cruce con LinkedIn Chile (rutificación Unholster, `high`-confidence únicamente) → produce `data/deudores_full.parquet` (no se commitea).
- **Agregación con k-anonimato (k ≥ 10):** `app/aggregates_v2.py` produce `static/aggregates.json` con todas las distribuciones, suprimiendo celdas con menos de 10 deudores. Ningún dato individual (RUT, nombre, dirección, patente) sale en el JSON.
- **Frontend estático:** HTML + Chart.js puro. FastAPI opcional para dev local.
- **Deploy:** Vercel, sólo `public/` (archivos estáticos), cero PII en el servidor.

## Pipeline

```
load_deudores.py         # Excel → parquet (3 hojas: personal, vehículos, propiedades)
parse_pdf.py             # PDF CRUCH → monto_utm + universidad por RUT
join_linkedin.py         # Enriquece con LinkedIn Chile (DuckDB icare-linkedin-research)
app/aggregates_v2.py     # Genera agregados JSON con k≥10
```

## Fuentes

- Nómina oficial CRUCH 2026 (`deudores_morosos.pdf`, publicado febrero 2026 conforme Decreto 297/2009 MINEDUC, Art. 15 Ley 19.287, Art. 13 bis Ley 19.848).
- Enriquecimiento de patrimonio: servicios internos Unholster.
- Rutificación LinkedIn Chile: pipeline propietario Unholster (5.2M perfiles).

## Privacidad

Todas las cifras publicadas están agregadas con supresión k-anónima (k ≥ 10). Ningún dato individual es accesible desde el dashboard ni queda expuesto en el deploy. El Excel original, el PDF y todos los parquets intermedios están en `.gitignore` y jamás se suben al repositorio ni a Vercel.

## Desarrollo local

```bash
# Regenerar agregados (requiere los parquets en data/)
python3 app/aggregates_v2.py

# Servir dashboard
uvicorn app.main:app --reload
# o
python3 -m http.server 8000 --directory public
```

## Deploy

```bash
npx vercel@latest --prod --yes
```

---
© Unholster · DecideChile
