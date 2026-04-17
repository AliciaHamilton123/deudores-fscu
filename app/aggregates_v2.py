"""Agregados v2: incluye monto_utm + universidad_administradora.

Pipeline:
1. Carga deudores_enriched.parquet (370k con patrimonio + LinkedIn)
2. Carga deudores_pdf.parquet (nómina parseada del PDF CRUCH con monto + universidad)
3. LEFT JOIN por rut-dv → deudores_full.parquet (un solo archivo con TODO)
4. Recomputa todos los agregados + nuevos cortes: por universidad, por monto bucket,
   monto × perfil, monto × región, académicos-deben-a-su-universidad, etc.
"""
import duckdb
import json
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
OUT = Path("/Users/antonio/deudores-fscu/app/static/aggregates.json")
K = 10

# UTM Abril 2026 (valor nominal aprox. para presentar en CLP)
UTM_CLP = 72800  # ~CLP por UTM — actualizar con valor oficial del mes de publicación

con = duckdb.connect(":memory:")
con.execute(f"""
CREATE VIEW enriched AS
SELECT * FROM read_parquet('{DATA}/deudores_enriched.parquet')
""")
con.execute(f"""
CREATE VIEW pdf_nom AS
SELECT rut_dv, monto_utm, universidad, periodos
FROM read_parquet('{DATA}/deudores_pdf.parquet')
""")

# JOIN: un deudor puede aparecer en varias universidades → agregamos al rut_dv:
#   monto_total = suma de montos
#   universidades = lista
#   universidad_principal = la de mayor monto
con.execute("""
CREATE VIEW pdf_agg AS
SELECT
    rut_dv,
    SUM(monto_utm) AS monto_utm,
    COUNT(*) AS n_universidades,
    arg_max(universidad, monto_utm) AS universidad_principal,
    STRING_AGG(DISTINCT universidad, '; ') AS universidades
FROM pdf_nom GROUP BY rut_dv
""")

con.execute("""
CREATE TABLE full_t AS
SELECT
    e.*,
    p.monto_utm,
    p.n_universidades,
    p.universidad_principal,
    p.universidades
FROM enriched e
LEFT JOIN pdf_agg p ON e.rut_dv = p.rut_dv
""")

# Guardar join completo para futuros análisis
con.execute(f"COPY full_t TO '{DATA}/deudores_full.parquet' (FORMAT parquet, COMPRESSION zstd)")

# Métricas del join
m = con.execute("""
SELECT
    COUNT(*) total,
    SUM(CASE WHEN monto_utm IS NOT NULL THEN 1 ELSE 0 END) con_monto,
    ROUND(SUM(monto_utm)) total_utm,
    ROUND(AVG(monto_utm),2) avg_utm,
    ROUND(MEDIAN(monto_utm),2) median_utm,
    ROUND(MAX(monto_utm),2) max_utm
FROM full_t
""").fetchone()
print(f"Join: {m[0]:,} total | {m[1]:,} con monto | total {m[2]:,.0f} UTM | avg {m[3]} | median {m[4]} | max {m[5]}")

def table(sql, k_col="n"):
    rows = con.execute(sql).fetchdf().to_dict(orient="records")
    return [{k: (None if v != v else v) for k, v in r.items()} for r in rows if r[k_col] >= K]

out = {}

# ═══ RESUMEN ═══
r = con.execute("""
SELECT
    COUNT(*) total,
    SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) en_linkedin,
    ROUND(100.0*SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END)/COUNT(*),1) pct_linkedin,
    SUM(CASE WHEN monto_utm IS NOT NULL THEN 1 ELSE 0 END) con_monto,
    ROUND(SUM(monto_utm)) total_utm,
    ROUND(AVG(monto_utm),1) avg_utm,
    ROUND(MEDIAN(monto_utm),1) median_utm,
    SUM(CASE WHEN decil_avaluo >= 8 THEN 1 ELSE 0 END) patrimonio_alto,
    SUM(CASE WHEN total_vehiculos >= 1 THEN 1 ELSE 0 END) con_vehiculos,
    SUM(CASE WHEN total_propiedades >= 1 THEN 1 ELSE 0 END) con_propiedades
FROM full_t
""").fetchone()
out["resumen"] = {
    "total": r[0], "en_linkedin": r[1], "pct_linkedin": r[2],
    "con_monto": r[3], "total_utm": r[4], "avg_utm": r[5], "median_utm": r[6],
    "total_clp": int((r[4] or 0) * UTM_CLP),
    "utm_clp": UTM_CLP,
    "patrimonio_alto": r[7], "con_vehiculos": r[8], "con_propiedades": r[9],
}

# ═══ GEOGRAFÍA (heredado) ═══
out["por_region"] = table("""
SELECT COALESCE(region,'(sin región)') region, COUNT(*) n,
       ROUND(SUM(monto_utm)) utm_total,
       ROUND(AVG(monto_utm),1) utm_avg,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) pct
FROM full_t GROUP BY 1 ORDER BY 2 DESC
""")

MACROZONAS = {
    "Arica Y Parinacota": "Norte", "Tarapacá": "Norte", "Antofagasta": "Norte", "Atacama": "Norte",
    "Coquimbo": "Centro", "Valparaíso": "Centro",
    "Metropolitana": "Metropolitana",
    "O’Higgins": "Centro sur", "Maule": "Centro sur", "Ñuble": "Centro sur", "Biobío": "Centro sur",
    "La Araucanía": "Sur", "Los Ríos": "Sur", "Los Lagos": "Sur",
    "Aysén": "Austral", "Magallanes": "Austral",
}
for row in out["por_region"]:
    row["macrozona"] = MACROZONAS.get(row["region"], "(sin macrozona)")
out["por_macrozona"] = {}
for row in out["por_region"]:
    mz = row["macrozona"]
    if mz not in out["por_macrozona"]:
        out["por_macrozona"][mz] = {"macrozona": mz, "n": 0, "utm_total": 0}
    out["por_macrozona"][mz]["n"] += row["n"]
    out["por_macrozona"][mz]["utm_total"] += row["utm_total"] or 0
out["por_macrozona"] = sorted(out["por_macrozona"].values(), key=lambda x: -x["n"])

out["por_comuna"] = table("""
SELECT COALESCE(comuna,'(sin comuna)') comuna, cod_comuna,
       COALESCE(region,'(sin región)') region,
       COUNT(*) n, ROUND(SUM(monto_utm)) utm_total
FROM full_t GROUP BY 1,2,3 ORDER BY 4 DESC
""")

# ═══ DEMOGRAFÍA ═══
out["por_sexo"] = table("""
SELECT COALESCE(sexo,'(sin dato)') sexo, COUNT(*) n, ROUND(SUM(monto_utm)) utm_total, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t GROUP BY 1 ORDER BY 2 DESC
""")

out["por_edad"] = table("""
SELECT
    CASE
        WHEN edad < 26 THEN '18-25' WHEN edad < 36 THEN '26-35' WHEN edad < 46 THEN '36-45'
        WHEN edad < 56 THEN '46-55' WHEN edad < 66 THEN '56-65' WHEN edad >= 66 THEN '66+'
        ELSE '(sin dato)' END rango_edad,
    COUNT(*) n, ROUND(AVG(monto_utm),1) utm_avg, ROUND(SUM(monto_utm)) utm_total
FROM full_t GROUP BY 1
ORDER BY CASE rango_edad WHEN '18-25' THEN 1 WHEN '26-35' THEN 2 WHEN '36-45' THEN 3
    WHEN '46-55' THEN 4 WHEN '56-65' THEN 5 WHEN '66+' THEN 6 ELSE 7 END
""")

out["por_edad_sexo"] = table("""
SELECT CASE
    WHEN edad < 26 THEN '18-25' WHEN edad < 36 THEN '26-35' WHEN edad < 46 THEN '36-45'
    WHEN edad < 56 THEN '46-55' WHEN edad < 66 THEN '56-65' WHEN edad >= 66 THEN '66+'
    ELSE '(sin dato)' END rango_edad,
    COALESCE(sexo,'(sin dato)') sexo, COUNT(*) n
FROM full_t GROUP BY 1,2
""")

out["por_decil"] = table("""
SELECT COALESCE(CAST(decil_avaluo AS VARCHAR),'(sin decil)') decil, COUNT(*) n, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t GROUP BY 1 ORDER BY decil
""")

out["por_nse"] = table("""
SELECT COALESCE(nse,'(sin dato)') nse, COUNT(*) n, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t GROUP BY 1 ORDER BY 2 DESC
""")

# ═══ PATRIMONIO ═══
out["por_vehiculos"] = table("""
SELECT CASE
    WHEN total_vehiculos IS NULL OR total_vehiculos = 0 THEN '0'
    WHEN total_vehiculos = 1 THEN '1' WHEN total_vehiculos = 2 THEN '2'
    WHEN total_vehiculos <= 4 THEN '3-4' ELSE '5+' END bucket,
    COUNT(*) n
FROM full_t GROUP BY 1 ORDER BY bucket
""")

out["por_propiedades"] = table("""
SELECT CASE
    WHEN total_propiedades IS NULL OR total_propiedades = 0 THEN '0'
    WHEN total_propiedades = 1 THEN '1' WHEN total_propiedades = 2 THEN '2'
    WHEN total_propiedades <= 4 THEN '3-4' ELSE '5+' END bucket,
    COUNT(*) n
FROM full_t GROUP BY 1 ORDER BY bucket
""")

# ═══ MONTO (nuevo) ═══
out["por_monto_bucket"] = table("""
SELECT CASE
    WHEN monto_utm IS NULL THEN '(sin registro)'
    WHEN monto_utm < 50 THEN '0-50 UTM'
    WHEN monto_utm < 100 THEN '50-100 UTM'
    WHEN monto_utm < 200 THEN '100-200 UTM'
    WHEN monto_utm < 500 THEN '200-500 UTM'
    WHEN monto_utm < 1000 THEN '500-1.000 UTM'
    WHEN monto_utm < 2000 THEN '1.000-2.000 UTM'
    ELSE '2.000+ UTM' END bucket,
    COUNT(*) n, ROUND(SUM(monto_utm)) utm_total, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t GROUP BY 1
ORDER BY CASE bucket
    WHEN '0-50 UTM' THEN 1 WHEN '50-100 UTM' THEN 2 WHEN '100-200 UTM' THEN 3
    WHEN '200-500 UTM' THEN 4 WHEN '500-1.000 UTM' THEN 5 WHEN '1.000-2.000 UTM' THEN 6
    WHEN '2.000+ UTM' THEN 7 ELSE 8 END
""")

# ═══ UNIVERSIDAD ACREEDORA (nuevo) ═══
out["por_universidad"] = table("""
SELECT universidad_principal AS universidad,
       COUNT(*) n,
       ROUND(SUM(monto_utm)) utm_total,
       ROUND(AVG(monto_utm),1) utm_avg,
       ROUND(MEDIAN(monto_utm),1) utm_median
FROM full_t WHERE universidad_principal IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC
""")

# ═══ LINKEDIN ═══
out["por_seniority"] = table("""
SELECT COALESCE(seniority,'(sin dato)') seniority, COUNT(*) n,
       ROUND(AVG(monto_utm),1) utm_avg, ROUND(SUM(monto_utm)) utm_total
FROM full_t WHERE en_linkedin GROUP BY 1 ORDER BY 2 DESC
""")

out["por_tier"] = table("""
SELECT COALESCE(tier,'(sin dato)') tier, COUNT(*) n, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t WHERE en_linkedin GROUP BY 1 ORDER BY 2 DESC
""")

out["top_industrias"] = table("""
SELECT industry, COUNT(*) n, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t WHERE en_linkedin AND industry IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 30
""")

out["top_empresas"] = table("""
SELECT company, COUNT(*) n, ROUND(AVG(monto_utm),1) utm_avg
FROM full_t WHERE en_linkedin AND company IS NOT NULL
  AND company NOT IN ('autónomo','independiente','profesional independiente','colegio')
GROUP BY 1 ORDER BY 2 DESC LIMIT 50
""")

# ═══ PERFILES (con monto) ═══
out["perfiles"] = table("""
WITH p AS (
    SELECT monto_utm, CASE
        WHEN en_linkedin AND seniority IN ('c-level','director') THEN '1. Ejecutivo (C-Level/Director)'
        WHEN en_linkedin AND seniority = 'manager' THEN '2. Gerente'
        WHEN en_linkedin AND seniority = 'academic' THEN '3. Académico'
        WHEN en_linkedin AND seniority = 'senior' THEN '4. Profesional senior'
        WHEN en_linkedin AND seniority = 'professional' THEN '5. Profesional'
        WHEN en_linkedin AND seniority = 'operational' THEN '6. Operativo'
        WHEN en_linkedin THEN '7. En LinkedIn (sin clasificar)'
        WHEN decil_avaluo >= 8 OR total_propiedades >= 2 THEN '8. Alto patrimonio (sin LinkedIn)'
        WHEN total_vehiculos >= 1 OR total_propiedades >= 1 THEN '9. Con patrimonio (sin LinkedIn)'
        ELSE '10. Sin información adicional' END perfil
    FROM full_t
)
SELECT perfil, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct,
       ROUND(AVG(monto_utm),1) utm_avg, ROUND(SUM(monto_utm)) utm_total
FROM p GROUP BY 1 ORDER BY 1
""")

# ═══ ÁNGULO PERIODÍSTICO: académicos que deben a su propia universidad ═══
# Match fuzzy company LinkedIn ~ universidad_principal (normalización)
out["academicos_propia_universidad"] = con.execute("""
SELECT
    universidad_principal,
    company,
    COUNT(*) n,
    ROUND(AVG(monto_utm),1) utm_avg
FROM full_t
WHERE en_linkedin AND seniority = 'academic'
  AND universidad_principal IS NOT NULL AND company IS NOT NULL
  AND (
    -- fuzzy match común
    LOWER(company) LIKE '%' || LOWER(REPLACE(REPLACE(universidad_principal, 'U.', ''), 'DE ', '')) || '%'
    OR LOWER(universidad_principal) LIKE '%' || LOWER(company) || '%'
  )
GROUP BY 1,2
HAVING COUNT(*) >= 5
ORDER BY 3 DESC
""").fetchdf().to_dict(orient="records")

# ═══ CRUCE perfil × región (heredado) ═══
out["perfil_x_region"] = table("""
WITH p AS (
    SELECT COALESCE(region,'(sin región)') region,
        CASE
            WHEN en_linkedin AND seniority IN ('c-level','director') THEN '1. Ejecutivo'
            WHEN en_linkedin AND seniority = 'manager' THEN '2. Gerente'
            WHEN en_linkedin AND seniority = 'academic' THEN '3. Académico'
            WHEN en_linkedin AND seniority IN ('senior','professional') THEN '4. Profesional'
            WHEN en_linkedin AND seniority = 'operational' THEN '5. Operativo'
            WHEN en_linkedin THEN '6. LinkedIn s/clasif'
            WHEN decil_avaluo >= 8 OR total_propiedades >= 2 THEN '7. Alto patrimonio s/LK'
            ELSE '8. Resto' END perfil
    FROM full_t
)
SELECT region, perfil, COUNT(*) n FROM p GROUP BY 1,2
""")

# Heredado
out["condicion_propietario"] = table("""
SELECT CASE
    WHEN (total_propiedades IS NULL OR total_propiedades=0) THEN 'Arrendatario/Allegado'
    WHEN (total_vehiculos IS NULL OR total_vehiculos=0) THEN 'Solo propietario'
    ELSE 'Propietario con vehículos' END bucket, COUNT(*) n
FROM full_t GROUP BY 1 ORDER BY 2 DESC
""")

# ═══ ESCRIBIR ═══
with open(OUT, "w") as f:
    json.dump(out, f, ensure_ascii=False, indent=2, default=str)

print(f"Escrito {OUT} ({OUT.stat().st_size/1024:.1f} KB)")
print(f"Claves: {list(out.keys())}")
