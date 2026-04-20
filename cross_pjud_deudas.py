"""Cruce de 335k deudores FSCU contra PJUD filtrando SÓLO cobranza de deuda.

Procedimientos incluidos (todos de cobranza ejecutiva o monitoria):
- Ejecutivo Obligación de Dar
- Ejecutivo Mínima Cuantía
- Gestión Preparatoria (Citac.Conf.Deuda)
- Gestión Preparatoria Notificación Cobro de Factura
- Monitorio
- Ley de Bancos
- Procedimiento Prenda - Ley 20.190

Output: data/deudores_pjud_deudas.parquet
"""
import pymssql, duckdb, pandas as pd, time
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
OUT = DATA / "deudores_pjud_deudas.parquet"
CHUNK = 2000

PROC_DEUDAS = [
    'Ejecutivo Obligación de Dar',
    'Ejecutivo Mínima Cuantía',
    'Gestión Preparatoria (Citac.Conf.Deuda)',
    'Gestión Preparatoria Notificación Cobro de Factura',
    'Monitorio',
    'Ley de Bancos',
    'Procedimiento Prenda - Ley 20.190',
]

def main():
    con_l = duckdb.connect(":memory:")
    # Base: RUTs de 2026 (335k) + también enriched para cubrir todos los que se puedan consultar
    ruts = con_l.execute(f"""
    SELECT DISTINCT rut_dv FROM read_parquet('{DATA}/nominas_consolidado.parquet') WHERE year=2026
    UNION
    SELECT DISTINCT rut_dv FROM read_parquet('{DATA}/deudores_enriched.parquet') WHERE rut_dv IS NOT NULL
    """).df()['rut_dv'].tolist()
    print(f"RUTs a consultar: {len(ruts):,}", flush=True)

    con = pymssql.connect(server='ec2-3-135-172-36.us-east-2.compute.amazonaws.com',
        user='User_Unholster', password='*cdwq%=qYA?i#f3', database='PJUD_Unholster',
        timeout=300, login_timeout=30)
    cur = con.cursor()

    # SQL: sólo cuentas como demandado (DDO.) Y la causa es de cobranza de deuda
    proc_list = "('" + "','".join(p.replace("'", "''") for p in PROC_DEUDAS) + "')"
    sql_tmpl = f"""
    SELECT l.rut AS rut_dv,
        COUNT(*) AS n_causas_deuda,
        SUM(CASE WHEN i.[Proc] = 'Ejecutivo Obligación de Dar' THEN 1 ELSE 0 END) AS n_ejecutivo,
        SUM(CASE WHEN i.[Proc] = 'Ley de Bancos' THEN 1 ELSE 0 END) AS n_ley_bancos,
        MAX(TRY_CAST(RIGHT(l.rol,4) AS INT)) AS ultimo_year
    FROM dbo.litigantes_civil l
    INNER JOIN dbo.info_causas_civil i ON l.cod_tribunal = i.cod_tribunal AND l.rol = i.Rol
    WHERE l.participante = 'DDO.'
      AND i.[Proc] IN {proc_list}
      AND l.rut IN ({{}})
    GROUP BY l.rut
    """
    results = {}
    t0 = time.time()
    nchunks = (len(ruts) + CHUNK - 1) // CHUNK
    for i in range(0, len(ruts), CHUNK):
        batch = ruts[i:i+CHUNK]
        ph = ','.join(['%s'] * len(batch))
        sql = sql_tmpl.format(ph)
        cur.execute(sql, batch)
        for r in cur.fetchall(): results[r[0]] = r
        idx = i // CHUNK + 1
        if idx % 10 == 0 or idx == nchunks:
            dt = time.time() - t0
            eta = dt * (nchunks - idx) / max(idx, 1)
            print(f"  chunk {idx}/{nchunks} · matches={len(results):,} · {dt:.0f}s · ETA {eta/60:.1f}min", flush=True)
    con.close()

    df_hit = pd.DataFrame(results.values(), columns=['rut_dv','n_causas_deuda','n_ejecutivo','n_ley_bancos','ultimo_year_deuda'])
    df = pd.DataFrame({'rut_dv': ruts}).merge(df_hit, on='rut_dv', how='left')
    for c in ['n_causas_deuda','n_ejecutivo','n_ley_bancos']:
        df[c] = df[c].fillna(0).astype(int)
    df['ultimo_year_deuda'] = df['ultimo_year_deuda'].fillna(0).astype(int)
    df['demandado_por_deuda'] = df['n_causas_deuda'] > 0

    df.to_parquet(OUT, index=False)
    print(f"\nEscrito {OUT}")
    print(f"Total consultados: {len(df):,}")
    print(f"Demandados por cobranza de deuda: {df['demandado_por_deuda'].sum():,} ({100*df['demandado_por_deuda'].mean():.1f}%)")
    print(f"Con juicio ejecutivo (obligación de dar): {(df['n_ejecutivo']>0).sum():,}")
    print(f"Con causas Ley de Bancos: {(df['n_ley_bancos']>0).sum():,}")

if __name__ == '__main__':
    main()
