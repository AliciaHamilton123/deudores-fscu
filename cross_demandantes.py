"""Identifica quién DEMANDA a los deudores FSCU.

Para cada causa de cobranza de deuda donde un deudor FSCU es DDO., buscamos el
DTE. (demandante) de la misma causa y agrupamos.

Output: data/demandantes.parquet (por demandante: rut, nombre, n_deudores, n_causas).
"""
import pymssql, duckdb, pandas as pd, time
from pathlib import Path

DATA = Path("/Users/antonio/deudores-fscu/data")
OUT_DEMANDANTES = DATA / "demandantes.parquet"
OUT_UNIV_VS_DEUDOR = DATA / "universidad_demanda_rut.parquet"

PROC_DEUDAS = [
    'Ejecutivo Obligación de Dar', 'Ejecutivo Mínima Cuantía',
    'Gestión Preparatoria (Citac.Conf.Deuda)',
    'Gestión Preparatoria Notificación Cobro de Factura',
    'Monitorio', 'Ley de Bancos', 'Procedimiento Prenda - Ley 20.190',
]
CHUNK = 2000

def main():
    con_l = duckdb.connect(":memory:")
    # Sólo los 72k que tienen causa de cobranza
    ruts = con_l.execute(f"""
    SELECT DISTINCT rut_dv FROM read_parquet('{DATA}/deudores_pjud_deudas.parquet')
    WHERE demandado_por_deuda = TRUE
    """).df()['rut_dv'].tolist()
    print(f"RUTs con causa por deuda: {len(ruts):,}", flush=True)

    con = pymssql.connect(server='ec2-3-135-172-36.us-east-2.compute.amazonaws.com',
        user='User_Unholster', password='*cdwq%=qYA?i#f3', database='PJUD_Unholster',
        timeout=600, login_timeout=30)
    cur = con.cursor()
    proc_list = "('" + "','".join(p.replace("'", "''") for p in PROC_DEUDAS) + "')"

    # Query: agrega por demandante (dte.rut, dte.nombre)
    sql_tmpl = f"""
    SELECT
        dte.rut AS dte_rut,
        MAX(dte.nombre) AS dte_nombre,
        COUNT(DISTINCT ddo.rut) AS n_deudores,
        COUNT(DISTINCT CAST(ddo.cod_tribunal AS VARCHAR) + '-' + ddo.rol) AS n_causas,
        SUM(CASE WHEN i.[Proc] = 'Ejecutivo Obligación de Dar' THEN 1 ELSE 0 END) AS n_ejec,
        SUM(CASE WHEN i.[Proc] = 'Ley de Bancos' THEN 1 ELSE 0 END) AS n_bancos
    FROM dbo.litigantes_civil ddo
    INNER JOIN dbo.litigantes_civil dte
      ON ddo.cod_tribunal = dte.cod_tribunal AND ddo.rol = dte.rol
    INNER JOIN dbo.info_causas_civil i
      ON ddo.cod_tribunal = i.cod_tribunal AND ddo.rol = i.Rol
    WHERE ddo.participante = 'DDO.'
      AND dte.participante = 'DTE.'
      AND i.[Proc] IN {proc_list}
      AND ddo.rut IN ({{}})
    GROUP BY dte.rut
    """

    # Query aux: universidades como DTE con la rut del deudor (para vincular U→deudor)
    # Filtramos universidades por nombre
    sql_univ = f"""
    SELECT
        dte.rut AS dte_rut,
        MAX(dte.nombre) AS dte_nombre,
        ddo.rut AS deudor_rut,
        COUNT(DISTINCT CAST(ddo.cod_tribunal AS VARCHAR) + '-' + ddo.rol) AS n_causas,
        MAX(TRY_CAST(RIGHT(ddo.rol,4) AS INT)) AS ultimo_year
    FROM dbo.litigantes_civil ddo
    INNER JOIN dbo.litigantes_civil dte
      ON ddo.cod_tribunal = dte.cod_tribunal AND ddo.rol = dte.rol
    INNER JOIN dbo.info_causas_civil i
      ON ddo.cod_tribunal = i.cod_tribunal AND ddo.rol = i.Rol
    WHERE ddo.participante = 'DDO.'
      AND dte.participante = 'DTE.'
      AND i.[Proc] IN {proc_list}
      AND ddo.rut IN ({{}})
      AND (dte.nombre LIKE 'UNIVERSIDAD%' OR dte.nombre LIKE 'U.%'
           OR dte.nombre LIKE '%PONTIFICIA%' OR dte.nombre LIKE '%FONDO SOLIDARIO%'
           OR dte.nombre LIKE '%CRÉDITO UNIVERSITARIO%')
    GROUP BY dte.rut, ddo.rut
    """

    dte_agg = {}  # rut -> [nombre, n_deudores_set, n_causas_total, n_ejec, n_bancos]
    univ_pairs = []  # lista de (dte_rut, dte_nombre, deudor_rut, n_causas, ultimo_year)
    t0 = time.time()
    nchunks = (len(ruts) + CHUNK - 1) // CHUNK

    for i in range(0, len(ruts), CHUNK):
        batch = ruts[i:i+CHUNK]
        ph = ','.join(['%s']*len(batch))
        # 1) Agregado por dte
        cur.execute(sql_tmpl.format(ph), batch)
        for row in cur.fetchall():
            dte_rut, dte_nom, n_d, n_c, n_e, n_b = row
            if dte_rut not in dte_agg:
                dte_agg[dte_rut] = [dte_nom, n_d, n_c, n_e, n_b]
            else:
                # combinar
                a = dte_agg[dte_rut]
                a[1] += n_d  # ver nota abajo
                a[2] += n_c
                a[3] += n_e
                a[4] += n_b
        # 2) Pares universidad→deudor
        cur.execute(sql_univ.format(ph), batch)
        for row in cur.fetchall():
            univ_pairs.append(row)
        idx = i // CHUNK + 1
        if idx % 5 == 0 or idx == nchunks:
            dt = time.time() - t0
            eta = dt * (nchunks - idx) / max(idx, 1)
            print(f"  chunk {idx}/{nchunks} · dtes={len(dte_agg):,} univ_pairs={len(univ_pairs):,} · {dt:.0f}s · ETA {eta/60:.1f}min", flush=True)
    con.close()

    # Guardar
    df_dte = pd.DataFrame([(k, *v) for k, v in dte_agg.items()],
        columns=['dte_rut','dte_nombre','n_deudores','n_causas','n_ejecutivo','n_ley_bancos'])
    df_dte.to_parquet(OUT_DEMANDANTES, index=False)

    df_univ = pd.DataFrame(univ_pairs, columns=['dte_rut','dte_nombre','deudor_rut','n_causas','ultimo_year'])
    df_univ.to_parquet(OUT_UNIV_VS_DEUDOR, index=False)

    print(f"\nDemandantes únicos: {len(df_dte):,}")
    print(f"Top 10 demandantes por deudores:")
    top = df_dte.sort_values('n_deudores', ascending=False).head(10)
    print(top[['dte_nombre','n_deudores','n_causas','n_ejecutivo','n_ley_bancos']].to_string(index=False))
    print(f"\nPares universidad→deudor: {len(df_univ):,}")
    print(f"Top universidades demandantes:")
    print(df_univ.groupby(['dte_rut','dte_nombre']).agg(
        deudores=('deudor_rut','nunique'), causas=('n_causas','sum')
    ).sort_values('deudores', ascending=False).head(10).to_string())

if __name__ == '__main__':
    main()
