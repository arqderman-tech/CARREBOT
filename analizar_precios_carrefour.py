"""
analizar_precios_carrefour.py
==============================
Lee output_carrefour/*.csv del día, guarda histórico en
data/precios_compacto.csv y genera los JSONs para la web.

Lógica idéntica al bot de Coto:
- Una fila por producto por día
- Índices % acumulados día a día
- Comparaciones vs día/7d/30d/6m/1y
- Categorías principales
"""

import json
import glob
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import sys

DIR_DATA         = Path("data")
PRECIOS_COMPACTO = DIR_DATA / "precios_compacto.csv"

ORDEN_CATS = [
    "Almacén", "Frescos", "Congelados",
    "Bebidas Con Alcohol", "Bebidas Sin Alcohol",
    "Limpieza", "Cuidado Personal",
]

PERIODOS = {"7d": 7, "30d": 30, "6m": 180, "1y": 365}


def cargar_csvs_hoy():
    hoy = datetime.now().strftime("%Y%m%d")
    patron = f"output_carrefour/carrefour_{hoy}*.csv"
    dfs = []
    for archivo in glob.glob(patron):
        try:
            df = pd.read_csv(archivo, encoding="utf-8-sig")
            dfs.append(df)
            print(f"  Cargado: {archivo} ({len(df)} prods)")
        except Exception as e:
            print(f"  ERROR cargando {archivo}: {e}")
    if not dfs:
        print("ERROR: No se encontraron CSVs de hoy.")
        return None
    return pd.concat(dfs, ignore_index=True)


def preparar_df_dia(df_raw, fecha_str):
    df = df_raw.copy()
    for col in ["precio_actual", "precio_regular"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["precio_regular"])
    df = df[df["precio_regular"] > 0]
    df = df.drop_duplicates(subset=["product_id"], keep="first")
    df["product_id"] = df["product_id"].astype(str)
    df["fecha"] = fecha_str
    # cat_principal ya viene del scraper
    if "cat_principal" not in df.columns:
        df["cat_principal"] = "Sin categoría"
    return df


def guardar_compacto(df_dia, fecha_str):
    DIR_DATA.mkdir(parents=True, exist_ok=True)
    cols = ["product_id", "sku_id", "ean", "nombre", "marca",
            "categoria", "cat_principal", "precio_actual", "precio_regular", "fecha"]
    cols = [c for c in cols if c in df_dia.columns]
    df_guardar = df_dia[cols].copy()

    if PRECIOS_COMPACTO.exists():
        df_hist = pd.read_csv(PRECIOS_COMPACTO, dtype={"product_id": str, "fecha": str})
        df_hist = df_hist[df_hist["fecha"] != fecha_str]
        df_nuevo = pd.concat([df_hist, df_guardar], ignore_index=True)
    else:
        df_nuevo = df_guardar

    df_nuevo.to_csv(PRECIOS_COMPACTO, index=False)
    kb = PRECIOS_COMPACTO.stat().st_size / 1024
    print(f"  precios_compacto.csv: {len(df_nuevo)} filas | {kb:.0f} KB")
    return df_nuevo


def snapshot_anterior(df_hist, fecha_hoy):
    fechas = sorted(df_hist["fecha"].unique(), reverse=True)
    for f in fechas:
        if f < fecha_hoy:
            df = df_hist[df_hist["fecha"] == f].copy()
            print(f"  Snapshot anterior: {f} ({len(df)} prods)")
            return df
    return None


def snapshot_en_fecha(df_hist, fecha_objetivo_str):
    fechas = sorted(df_hist["fecha"].unique())
    candidato = None
    for f in fechas:
        if f <= fecha_objetivo_str:
            candidato = f
    if candidato is None:
        return None
    df = df_hist[df_hist["fecha"] == candidato].copy()
    print(f"  Snapshot para {fecha_objetivo_str}: {candidato} ({len(df)} prods)")
    return df


def calcular_variacion(df_hoy, df_antes):
    df_h = df_hoy[["product_id", "nombre", "marca", "categoria", "cat_principal",
                    "precio_actual", "precio_regular"]].copy()
    df_h = df_h.rename(columns={
        "precio_regular": "precio_hoy",
        "precio_actual":  "precio_actual_hoy",
    })
    df_a = df_antes[["product_id", "precio_regular"]].rename(
        columns={"precio_regular": "precio_antes"})
    df = pd.merge(df_h, df_a, on="product_id", how="inner")
    df = df.dropna(subset=["precio_hoy", "precio_antes"])
    df = df[df["precio_antes"] > 0]
    df["diff_abs"] = (df["precio_hoy"] - df["precio_antes"]).round(2)
    df["diff_pct"] = ((df["diff_abs"] / df["precio_antes"]) * 100).round(2)
    return df


def calcular_variacion_cats(df_var):
    resumen = df_var.groupby("cat_principal").agg(
        variacion_pct_promedio=("diff_pct", "mean"),
        productos_subieron=("diff_pct", lambda x: (x > 0).sum()),
        productos_bajaron=("diff_pct", lambda x: (x < 0).sum()),
        productos_sin_cambio=("diff_pct", lambda x: (x == 0).sum()),
        total_productos=("diff_pct", "count"),
    ).reset_index()
    resumen = resumen.rename(columns={"cat_principal": "categoria"})
    resumen["variacion_pct_promedio"] = resumen["variacion_pct_promedio"].round(2)
    orden = {cat: i for i, cat in enumerate(ORDEN_CATS)}
    resumen["_ord"] = resumen["categoria"].map(lambda x: orden.get(x, 999))
    return resumen.sort_values("_ord").drop(columns="_ord")


def top_productos(df_var, n=20, ascendente=False):
    df = df_var.sort_values("diff_pct", ascending=ascendente).head(n)
    return df[["product_id", "nombre", "marca", "categoria",
               "precio_antes", "precio_hoy", "precio_actual_hoy",
               "diff_abs", "diff_pct"]].to_dict("records")


def generar_graficos_data(df_hist):
    if df_hist.empty:
        return {}
    df_hist = df_hist.copy()
    df_hist["fecha_dt"] = pd.to_datetime(df_hist["fecha"], format="%Y%m%d")
    df_hist = df_hist.sort_values(["fecha_dt", "product_id"])
    hoy = pd.Timestamp.now().normalize()
    resultado = {}

    for periodo, dias in PERIODOS.items():
        fecha_inicio = hoy - timedelta(days=dias)
        df_p = df_hist[df_hist["fecha_dt"] >= fecha_inicio].copy()
        fechas = sorted(df_p["fecha_dt"].unique())
        if not fechas:
            resultado[periodo] = {"total": [], "categorias": {}}
            continue

        fecha_str_0 = fechas[0].strftime("%Y-%m-%d")
        serie_total = [{"fecha": fecha_str_0, "pct": 0.0}]
        acum = 0.0
        for i in range(1, len(fechas)):
            dv = calcular_variacion(
                df_p[df_p["fecha_dt"] == fechas[i]],
                df_p[df_p["fecha_dt"] == fechas[i - 1]]
            )
            var = float(dv["diff_pct"].mean()) if not dv.empty else 0.0
            acum = round(acum + var, 2)
            serie_total.append({"fecha": fechas[i].strftime("%Y-%m-%d"), "pct": acum})

        series_cats = {}
        for cat in ORDEN_CATS:
            df_cat = df_p[df_p["cat_principal"] == cat]
            if df_cat.empty:
                continue
            serie = [{"fecha": fecha_str_0, "pct": 0.0}]
            acum_cat = 0.0
            for i in range(1, len(fechas)):
                dv = calcular_variacion(
                    df_cat[df_cat["fecha_dt"] == fechas[i]],
                    df_cat[df_cat["fecha_dt"] == fechas[i - 1]]
                )
                var = float(dv["diff_pct"].mean()) if not dv.empty else 0.0
                acum_cat = round(acum_cat + var, 2)
                serie.append({"fecha": fechas[i].strftime("%Y-%m-%d"), "pct": acum_cat})
            series_cats[cat] = serie

        resultado[periodo] = {"total": serie_total, "categorias": series_cats}

    return resultado


def main():
    solo_graficos = "--solo-graficos" in sys.argv

    print(f"\n{'='*60}")
    print(f"  ANALISIS CARREFOUR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    if solo_graficos:
        print(f"  MODO: solo gráficos (sin scraping)")
    print(f"{'='*60}\n")

    fecha_hoy = datetime.now().strftime("%Y%m%d")
    DIR_DATA.mkdir(parents=True, exist_ok=True)

    if solo_graficos:
        if not PRECIOS_COMPACTO.exists():
            print("ERROR: No existe precios_compacto.csv")
            return
        df_hist = pd.read_csv(PRECIOS_COMPACTO, dtype={"product_id": str, "fecha": str})
        fecha_hoy = sorted(df_hist["fecha"].unique())[-1]
        df_dia = df_hist[df_hist["fecha"] == fecha_hoy].copy()
        print(f"  Usando fecha más reciente: {fecha_hoy} ({len(df_dia)} prods)")
    else:
        print("[1/5] Cargando CSVs de hoy ...")
        df_raw = cargar_csvs_hoy()
        if df_raw is None:
            return
        df_dia = preparar_df_dia(df_raw, fecha_hoy)
        print("\n[2/5] Guardando precios_compacto ...")
        df_hist = guardar_compacto(df_dia, fecha_hoy)

    print("\n[3/5] Calculando variaciones ...")
    resumen = {
        "fecha": fecha_hoy,
        "total_productos": len(df_dia),
        "variacion_dia": None, "variacion_7d": None,
        "variacion_mes": None, "variacion_6m": None, "variacion_anio": None,
        "categorias_dia": [], "ranking_baja_dia": [],
        "productos_subieron_dia": 0, "productos_bajaron_dia": 0, "productos_sin_cambio_dia": 0,
    }

    df_ayer = snapshot_anterior(df_hist, fecha_hoy)
    if df_ayer is not None:
        dv = calcular_variacion(df_dia, df_ayer)
        if not dv.empty:
            resumen["variacion_dia"]            = round(float(dv["diff_pct"].mean()), 2)
            resumen["productos_subieron_dia"]   = int((dv["diff_pct"] > 0).sum())
            resumen["productos_bajaron_dia"]    = int((dv["diff_pct"] < 0).sum())
            resumen["productos_sin_cambio_dia"] = int((dv["diff_pct"] == 0).sum())
            resumen["ranking_baja_dia"]         = top_productos(dv, 10, True)
            resumen["categorias_dia"]           = calcular_variacion_cats(dv).to_dict("records")
            print(f"  Variación día: {resumen['variacion_dia']}%")
            with open(DIR_DATA / "ranking_dia.json", "w", encoding="utf-8") as f:
                json.dump(top_productos(dv, 20, False), f, ensure_ascii=False, indent=2)

    for label, dias, key, fname in [
        ("7d",  7,   "variacion_7d",   "ranking_7d.json"),
        ("30d", 30,  "variacion_mes",  "ranking_mes.json"),
        ("6m",  180, "variacion_6m",   None),
        ("1y",  365, "variacion_anio", "ranking_anio.json"),
    ]:
        f_target = (datetime.now() - timedelta(days=dias)).strftime("%Y%m%d")
        df_ref = snapshot_en_fecha(df_hist, f_target)
        if df_ref is not None:
            dv = calcular_variacion(df_dia, df_ref)
            if not dv.empty:
                resumen[key] = round(float(dv["diff_pct"].mean()), 2)
                print(f"  Variación {label}: {resumen[key]}%")
                if fname:
                    with open(DIR_DATA / fname, "w", encoding="utf-8") as f:
                        json.dump(top_productos(dv, 20, False), f, ensure_ascii=False, indent=2)

    print("\n[4/5] Guardando resumen.json ...")
    with open(DIR_DATA / "resumen.json", "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2)

    print("\n[5/5] Generando graficos.json ...")
    graficos = generar_graficos_data(df_hist)
    with open(DIR_DATA / "graficos.json", "w", encoding="utf-8") as f:
        json.dump(graficos, f, ensure_ascii=False, indent=2)

    print(f"\n{'='*60}")
    print(f"  LISTO — {resumen['total_productos']} productos")
    for k, v in [("Día", resumen["variacion_dia"]), ("7d", resumen["variacion_7d"]),
                 ("30d", resumen["variacion_mes"]), ("6m", resumen["variacion_6m"]),
                 ("1y", resumen["variacion_anio"])]:
        if v is not None:
            print(f"  {k}: {'📈' if v > 0 else '📉'} {v}%")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
