"""Pre-computa agregados seguros (k-anonimato k>=10) desde el parquet enriquecido.

Output: app/static/aggregates.json — consumido por el dashboard.
Garantías: ningún campo individual (rut, nombre, url, direccion) sale en el JSON.
"""
import duckdb
import json
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data/deudores_enriched.parquet")
OUT = Path("/Users/antonio/deudores-fscu/app/static/aggregates.json")
K = 10  # supresión k-anónima

con = duckdb.connect(":memory:")
con.execute(f"CREATE VIEW d AS SELECT * FROM read_parquet('{DATA}')")

MACROZONAS = {
    "Arica Y Parinacota": "Norte", "Tarapacá": "Norte", "Antofagasta": "Norte", "Atacama": "Norte",
    "Coquimbo": "Centro", "Valparaíso": "Centro",
    "Metropolitana": "Metropolitana",
    "O’Higgins": "Centro sur", "Maule": "Centro sur", "Ñuble": "Centro sur", "Biobío": "Centro sur",
    "La Araucanía": "Sur", "Los Ríos": "Sur", "Los Lagos": "Sur",
    "Aysén": "Austral", "Magallanes": "Austral",
}

def table(sql, k_col="n"):
    """Ejecuta query, suprime filas con k_col < K, devuelve lista de dicts."""
    rows = con.execute(sql).fetchdf().to_dict(orient="records")
    return [
        {**{k: (None if v != v else v) for k, v in r.items()}}  # NaN -> None
        for r in rows if r[k_col] >= K
    ]

out = {}

# ═══ RESUMEN ═══
resumen = con.execute("""
SELECT
    COUNT(*) total,
    SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) en_linkedin,
    ROUND(100.0*SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END)/COUNT(*),1) pct_linkedin,
    SUM(CASE WHEN decil_avaluo >= 8 THEN 1 ELSE 0 END) patrimonio_alto,
    SUM(CASE WHEN total_vehiculos >= 1 THEN 1 ELSE 0 END) con_vehiculos,
    SUM(CASE WHEN total_propiedades >= 1 THEN 1 ELSE 0 END) con_propiedades
FROM d
""").fetchone()
out["resumen"] = {
    "total": resumen[0], "en_linkedin": resumen[1], "pct_linkedin": resumen[2],
    "patrimonio_alto": resumen[3], "con_vehiculos": resumen[4], "con_propiedades": resumen[5],
}

# ═══ GEOGRAFÍA ═══
out["por_region"] = table("""
SELECT COALESCE(region,'(sin región)') region, COUNT(*) n,
       ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) pct
FROM d GROUP BY 1 ORDER BY 2 DESC
""")

# Agregar macrozona desde Python
for r in out["por_region"]:
    r["macrozona"] = MACROZONAS.get(r["region"], "(sin macrozona)")

out["por_macrozona"] = {}
for r in out["por_region"]:
    mz = r["macrozona"]
    out["por_macrozona"][mz] = out["por_macrozona"].get(mz, 0) + r["n"]
out["por_macrozona"] = [{"macrozona": k, "n": v} for k, v in sorted(out["por_macrozona"].items(), key=lambda x: -x[1])]

# Comuna con supresión k
out["por_comuna"] = table("""
SELECT COALESCE(comuna,'(sin comuna)') comuna, cod_comuna,
       COALESCE(region,'(sin región)') region,
       COUNT(*) n
FROM d GROUP BY 1,2,3 ORDER BY 4 DESC
""")

# ═══ DEMOGRAFÍA ═══
out["por_sexo"] = table("""
SELECT COALESCE(sexo,'(sin dato)') sexo, COUNT(*) n
FROM d GROUP BY 1 ORDER BY 2 DESC
""")

out["por_edad"] = table("""
SELECT
    CASE
        WHEN edad < 26 THEN '18-25'
        WHEN edad < 36 THEN '26-35'
        WHEN edad < 46 THEN '36-45'
        WHEN edad < 56 THEN '46-55'
        WHEN edad < 66 THEN '56-65'
        WHEN edad >= 66 THEN '66+'
        ELSE '(sin dato)' END rango_edad,
    COUNT(*) n
FROM d GROUP BY 1
ORDER BY CASE rango_edad
    WHEN '18-25' THEN 1 WHEN '26-35' THEN 2 WHEN '36-45' THEN 3
    WHEN '46-55' THEN 4 WHEN '56-65' THEN 5 WHEN '66+' THEN 6 ELSE 7 END
""")

out["por_edad_sexo"] = table("""
SELECT
    CASE
        WHEN edad < 26 THEN '18-25'
        WHEN edad < 36 THEN '26-35'
        WHEN edad < 46 THEN '36-45'
        WHEN edad < 56 THEN '46-55'
        WHEN edad < 66 THEN '56-65'
        WHEN edad >= 66 THEN '66+'
        ELSE '(sin dato)' END rango_edad,
    COALESCE(sexo,'(sin dato)') sexo,
    COUNT(*) n
FROM d GROUP BY 1,2
""")

out["por_decil"] = table("""
SELECT COALESCE(CAST(decil_avaluo AS VARCHAR),'(sin decil)') decil, COUNT(*) n
FROM d GROUP BY 1 ORDER BY decil
""")

out["por_nse"] = table("""
SELECT COALESCE(nse,'(sin dato)') nse, COUNT(*) n
FROM d GROUP BY 1 ORDER BY 2 DESC
""")

# ═══ PATRIMONIO ═══
out["por_vehiculos"] = table("""
SELECT
    CASE
        WHEN total_vehiculos IS NULL OR total_vehiculos = 0 THEN '0'
        WHEN total_vehiculos = 1 THEN '1'
        WHEN total_vehiculos = 2 THEN '2'
        WHEN total_vehiculos <= 4 THEN '3-4'
        ELSE '5+' END bucket,
    COUNT(*) n
FROM d GROUP BY 1 ORDER BY bucket
""")

out["por_propiedades"] = table("""
SELECT
    CASE
        WHEN total_propiedades IS NULL OR total_propiedades = 0 THEN '0'
        WHEN total_propiedades = 1 THEN '1'
        WHEN total_propiedades = 2 THEN '2'
        WHEN total_propiedades <= 4 THEN '3-4'
        ELSE '5+' END bucket,
    COUNT(*) n
FROM d GROUP BY 1 ORDER BY bucket
""")

out["condicion_propietario"] = table("""
SELECT
    CASE
        WHEN (total_propiedades IS NULL OR total_propiedades=0) THEN 'Arrendatario/Allegado'
        WHEN (total_vehiculos IS NULL OR total_vehiculos=0) THEN 'Solo propietario'
        ELSE 'Propietario con vehículos' END bucket,
    COUNT(*) n
FROM d GROUP BY 1 ORDER BY 2 DESC
""")

# ═══ LINKEDIN (subset 66k) ═══
out["por_seniority"] = table("""
SELECT COALESCE(seniority,'(sin dato)') seniority, COUNT(*) n
FROM d WHERE en_linkedin GROUP BY 1 ORDER BY 2 DESC
""")

out["por_tier"] = table("""
SELECT COALESCE(tier,'(sin dato)') tier, COUNT(*) n
FROM d WHERE en_linkedin GROUP BY 1 ORDER BY 2 DESC
""")

out["top_industrias"] = table("""
SELECT industry, COUNT(*) n
FROM d WHERE en_linkedin AND industry IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 30
""")

out["top_empresas"] = table("""
SELECT company, COUNT(*) n
FROM d WHERE en_linkedin AND company IS NOT NULL
  AND company NOT IN ('autónomo','independiente','profesional independiente','colegio')
GROUP BY 1 ORDER BY 2 DESC LIMIT 50
""")

# ═══ PERFILES CONSOLIDADOS (categorización maestra, cubre los 370k) ═══
# Asigna cada deudor a UNA categoría (orden de prioridad)
out["perfiles"] = table("""
WITH p AS (
    SELECT
        CASE
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
    FROM d
)
SELECT perfil, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct
FROM p GROUP BY 1 ORDER BY 1
""")

# Cruces perfiles × región
out["perfil_x_region"] = table("""
WITH p AS (
    SELECT
        COALESCE(region,'(sin región)') region,
        CASE
            WHEN en_linkedin AND seniority IN ('c-level','director') THEN '1. Ejecutivo'
            WHEN en_linkedin AND seniority = 'manager' THEN '2. Gerente'
            WHEN en_linkedin AND seniority = 'academic' THEN '3. Académico'
            WHEN en_linkedin AND seniority IN ('senior','professional') THEN '4. Profesional'
            WHEN en_linkedin AND seniority = 'operational' THEN '5. Operativo'
            WHEN en_linkedin THEN '6. LinkedIn s/clasif'
            WHEN decil_avaluo >= 8 OR total_propiedades >= 2 THEN '7. Alto patrimonio s/LK'
            ELSE '8. Resto' END perfil
    FROM d
)
SELECT region, perfil, COUNT(*) n FROM p GROUP BY 1,2
""")

# ═══ SERIALIZE ═══
with open(OUT, "w") as f:
    json.dump(out, f, ensure_ascii=False, indent=2, default=str)

print(f"Escrito {OUT} ({OUT.stat().st_size / 1024:.1f} KB)")
print(f"Claves: {list(out.keys())}")
print(f"\nResumen:")
for k, v in out["resumen"].items():
    print(f"  {k}: {v:,}" if isinstance(v, int) else f"  {k}: {v}")
print(f"\nPerfiles (categorización maestra):")
for p in out["perfiles"]:
    print(f"  {p['perfil']:<45} {p['n']:>8,}  {p['pct']:>5}%")
