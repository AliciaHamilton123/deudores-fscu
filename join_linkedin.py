"""Join deudores FSCU con LinkedIn (ruled.rut_status='high')."""
import duckdb
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
LINKEDIN_DB = "/Users/antonio/icare-linkedin-research/data/linkedin_chile.duckdb"

con = duckdb.connect(str(DATA / "deudores_enriched.duckdb"))
con.execute(f"ATTACH '{LINKEDIN_DB}' AS ld (READ_ONLY)")

# 1. Deudores normalizados (rut sin DV para hacer match con ruled)
con.execute(f"""
CREATE OR REPLACE TABLE deudores AS
SELECT
    rut AS rut_dv,
    split_part(rut, '-', 1) AS rut,
    nombre, fecha_nacimiento, sexo, edad,
    comuna, cod_comuna, region, cod_region,
    latitud, longitud, gse, nse, decil_avaluo,
    total_vehiculos, total_propiedades,
    avaluo_total_propiedades, tasacion_total_vehiculos
FROM read_parquet('{DATA}/deudores_personal.parquet')
WHERE rut IS NOT NULL
""")
n_ded = con.execute("SELECT COUNT(*) FROM deudores").fetchone()[0]
print(f"Deudores cargados: {n_ded:,}")

# 2. LinkedIn high-confidence, 1 perfil por RUT (mayor quality, desempate url no nula)
con.execute("""
CREATE OR REPLACE TABLE lk AS
SELECT * FROM (
    SELECT
        rut, rut_confidence, rut_nombre_match,
        fn, fi, la, gender, url, uname,
        job_title, industry, company, cindustry, csize,
        seniority, tier, geo, quality,
        loc, city, state, region AS lk_region,
        headline, occ, connections,
        has_nb, has_exp, has_edu,
        ROW_NUMBER() OVER (
            PARTITION BY rut
            ORDER BY quality DESC NULLS LAST, rut_confidence DESC, url IS NULL
        ) AS rn
    FROM ld.ruled
    WHERE rut_status = 'high' AND rut IS NOT NULL
) WHERE rn = 1
""")
n_lk = con.execute("SELECT COUNT(*) FROM lk").fetchone()[0]
print(f"LinkedIn high (1 perfil/RUT): {n_lk:,}")

# 3. Enriquecido: LEFT JOIN para conservar los 370k, marcar match
con.execute("""
CREATE OR REPLACE TABLE deudores_enriched AS
SELECT
    d.*,
    l.rut IS NOT NULL AS en_linkedin,
    l.rut_confidence AS lk_rut_confidence,
    l.fn AS lk_fn, l.la AS lk_la, l.gender AS lk_gender,
    l.url AS lk_url, l.uname AS lk_uname,
    l.job_title, l.industry, l.company, l.cindustry, l.csize,
    l.seniority, l.tier, l.geo, l.quality,
    l.loc AS lk_loc, l.city AS lk_city, l.state AS lk_state, l.lk_region,
    l.headline, l.occ, l.connections,
    l.has_nb, l.has_exp, l.has_edu
FROM deudores d
LEFT JOIN lk l ON d.rut = l.rut
""")

# Métricas
m = con.execute("""
SELECT
    COUNT(*) total,
    SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) en_linkedin,
    ROUND(100.0 * SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) / COUNT(*), 1) pct,
    SUM(CASE WHEN seniority IS NOT NULL THEN 1 ELSE 0 END) con_seniority,
    SUM(CASE WHEN company IS NOT NULL THEN 1 ELSE 0 END) con_company,
    SUM(CASE WHEN has_edu THEN 1 ELSE 0 END) con_educacion
FROM deudores_enriched
""").fetchone()
print(f"\nTotal deudores: {m[0]:,}")
print(f"En LinkedIn:    {m[1]:,} ({m[2]}%)")
print(f"Con cargo:      {m[3]:,}")
print(f"Con empresa:    {m[4]:,}")
print(f"Con educacion:  {m[5]:,}")

# Export a parquet
con.execute(f"""
COPY deudores_enriched TO '{DATA}/deudores_enriched.parquet' (FORMAT parquet, COMPRESSION zstd)
""")
print(f"\nEscrito: {DATA}/deudores_enriched.parquet")
