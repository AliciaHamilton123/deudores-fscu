"""Primer análisis del cruce FSCU × LinkedIn."""
import duckdb
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
con = duckdb.connect(str(DATA / "deudores_enriched.duckdb"), read_only=True)

def q(title, sql, limit=20):
    print(f"\n{'='*70}\n{title}\n{'='*70}")
    rows = con.execute(sql).fetchall()
    cols = [d[0] for d in con.execute(sql).description]
    widths = [max(len(str(c)), max((len(str(r[i])) for r in rows), default=0)) for i, c in enumerate(cols)]
    print("  ".join(f"{c:<{w}}" for c, w in zip(cols, widths)))
    for r in rows[:limit]:
        print("  ".join(f"{str(v):<{w}}" for v, w in zip(r, widths)))

q("Seniority (66,118 deudores en LinkedIn)", """
SELECT seniority, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct
FROM deudores_enriched WHERE en_linkedin
GROUP BY 1 ORDER BY 2 DESC
""")

q("Tier de empresa", """
SELECT COALESCE(tier,'(sin clasif)') tier, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct
FROM deudores_enriched WHERE en_linkedin
GROUP BY 1 ORDER BY 2 DESC
""")

q("Top 20 industrias (LinkedIn)", """
SELECT industry, COUNT(*) n
FROM deudores_enriched WHERE en_linkedin AND industry IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 20
""")

q("Top 20 empresas con más deudores FSCU", """
SELECT company, COUNT(*) n
FROM deudores_enriched WHERE en_linkedin AND company IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 20
""")

q("NSE × Seniority (decil alto con cargo alto)", """
SELECT
    CASE WHEN decil_avaluo >= 8 THEN 'Decil 8-10 (alto)'
         WHEN decil_avaluo >= 5 THEN 'Decil 5-7'
         WHEN decil_avaluo IS NOT NULL THEN 'Decil 1-4'
         ELSE 'Sin decil' END decil_bucket,
    seniority, COUNT(*) n
FROM deudores_enriched WHERE en_linkedin AND seniority IS NOT NULL
GROUP BY 1,2 ORDER BY 1, 3 DESC
""", limit=40)

q("Match rate por región", """
SELECT region,
    COUNT(*) total,
    SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END) en_lk,
    ROUND(100.0*SUM(CASE WHEN en_linkedin THEN 1 ELSE 0 END)/COUNT(*),1) pct
FROM deudores_enriched
GROUP BY 1 ORDER BY 2 DESC
""")

q("Validación género (FSCU vs LinkedIn)", """
SELECT sexo, lk_gender, COUNT(*) n
FROM deudores_enriched WHERE en_linkedin AND lk_gender IS NOT NULL
GROUP BY 1,2 ORDER BY 3 DESC
""")

q("Deudores C-Level en top-tier (periodísticamente jugoso)", """
SELECT tier, seniority, COUNT(*) n,
    ROUND(AVG(avaluo_total_propiedades)) avg_avaluo,
    ROUND(AVG(tasacion_total_vehiculos)) avg_vehiculos
FROM deudores_enriched
WHERE en_linkedin AND seniority IN ('CEO','C-Level','Founder','Owner','Partner','President')
GROUP BY 1,2 ORDER BY 3 DESC
""")
