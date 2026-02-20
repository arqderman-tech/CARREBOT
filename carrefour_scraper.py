import requests
import pandas as pd
import time
from datetime import datetime
from pathlib import Path
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
BASE_URL   = "https://www.carrefour.com.ar"
PAGE_SIZE  = 50
MAX_PRODS  = 2500
MAX_WORKERS = 10     # requests concurrentes
OUTPUT_DIR = Path("output_carrefour")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-AR,es;q=0.9",
    "Referer": "https://www.carrefour.com.ar/",
    "Connection": "keep-alive",
}

# ── TODAS LAS CATEGORÍAS (74) ─────────────────────────────────────────────────
CATEGORIAS = [
    ("Almacén", "Aceites y vinagres", 161, 162), ("Almacén", "Pastas secas", 161, 168),
    ("Almacén", "Arroz y legumbres", 161, 172), ("Almacén", "Harinas", 161, 176),
    ("Almacén", "Enlatados y Conservas", 161, 183), ("Almacén", "Sal aderezos y saborizadores", 161, 190),
    ("Almacén", "Caldos sopas y puré", 161, 195), ("Almacén", "Repostería y postres", 161, 199),
    ("Almacén", "Snacks", 161, 214), ("Almacén", "Comidas instantáneas", 161, 658),
    ("Almacén", "Golosinas y chocolates", 222, 208), ("Almacén", "Galletitas y tostadas", 222, 223),
    ("Almacén", "Budines y magdalenas", 222, 229), ("Almacén", "Yerba", 222, 232),
    ("Almacén", "Café", 222, 233), ("Almacén", "Infusiones", 222, 238),
    ("Almacén", "Azúcar y endulzantes", 222, 242), ("Almacén", "Mermeladas y dulces", 222, 246),
    ("Almacén", "Cereales y barritas", 222, 250), ("Bebidas Con Alcohol", "Cervezas", 255, 256),
    ("Bebidas Con Alcohol", "Vinos", 255, 257), ("Bebidas Con Alcohol", "Fernet y aperitivos", 255, 262),
    ("Bebidas Con Alcohol", "Bebidas blancas", 255, 266), ("Bebidas Con Alcohol", "Espumantes y sidras", 255, 273),
    ("Bebidas Sin Alcohol", "Gaseosas", 255, 277), ("Bebidas Sin Alcohol", "Aguas", 255, 283),
    ("Bebidas Sin Alcohol", "Jugos", 255, 286), ("Bebidas Sin Alcohol", "Bebidas energizantes", 255, 290),
    ("Bebidas Sin Alcohol", "Bebidas isotónicas", 255, 291), ("Frescos", "Leches", 292, 293),
    ("Frescos", "Yogures", 292, 299), ("Frescos", "Mantecas y margarinas", 292, 302),
    ("Frescos", "Cremas de leche", 292, 304), ("Frescos", "Postres", 292, 305),
    ("Frescos", "Huevos", 292, 306), ("Frescos", "Tapas y pastas frescas", 292, 307),
    ("Frescos", "Quesos", 292, 310), ("Frescos", "Fiambres", 292, 318),
    ("Frescos", "Carne vacuna", 321, 322), ("Frescos", "Pollo y granja", 321, 323),
    ("Frescos", "Carne de cerdo", 321, 324), ("Frescos", "Pescados y mariscos", 321, 327),
    ("Frescos", "Frutas", 330, 331), ("Frescos", "Verduras", 330, 332),
    ("Congelados", "Hamburguesas y medallones", 347, 348), ("Congelados", "Nuggets y rebozados", 347, 349),
    ("Congelados", "Papas congeladas", 347, 350), ("Congelados", "Frutas y vegetales cong.", 347, 352),
    ("Congelados", "Comidas congeladas", 347, 356), ("Congelados", "Pescados cong.", 347, 357),
    ("Congelados", "Helados y postres", 347, 358), ("Congelados", "Pollo congelado", 347, 703),
    ("Limpieza", "Limpieza de la ropa", 359, 360), ("Limpieza", "Limpieza pisos y muebles", 359, 367),
    ("Limpieza", "Insecticidas", 359, 376), ("Limpieza", "Limpieza de cocina", 359, 377),
    ("Limpieza", "Lavandinas", 359, 384), ("Limpieza", "Rollos y servilletas", 359, 385),
    ("Limpieza", "Papeles higiénicos", 359, 386), ("Limpieza", "Limpieza de baño", 359, 387),
    ("Limpieza", "Desodorantes de ambiente", 359, 390), ("Limpieza", "Artículos de limpieza", 359, 394),
    ("Cuidado Personal", "Cuidado del cabello", 402, 403), ("Cuidado Personal", "Cuidado dental", 402, 412),
    ("Cuidado Personal", "Jabones", 402, 418), ("Cuidado Personal", "Protección femenina", 402, 422),
    ("Cuidado Personal", "Cuidado de la piel", 402, 427), ("Cuidado Personal", "Antitranspirantes", 402, 435),
    ("Cuidado Personal", "Cuidado corporal", 402, 438), ("Cuidado Personal", "Repelentes", 402, 443),
    ("Cuidado Personal", "Algodones e hisopos", 402, 444), ("Cuidado Personal", "Pañales", 451, 452),
    ("Cuidado Personal", "Toallitas húmedas", 451, 453), ("Cuidado Personal", "Higiene para bebés", 451, 458),
]


def crear_sesion():
    session = requests.Session()
    retry_strategy = Retry(
        total=10,
        backoff_factor=3,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry_strategy))
    return session


def fetch_page(args):
    """Descarga una página y devuelve los SKUs extraídos."""
    session, fq, from_idx, cat_nombre, cat_principal = args
    url = f"{BASE_URL}/api/catalog_system/pub/products/search"
    to_idx = from_idx + PAGE_SIZE - 1

    try:
        r = session.get(url, params={"fq": fq, "_from": from_idx, "_to": to_idx},
                        headers=HEADERS, timeout=30)
        r.raise_for_status()

        total_vtex = 0
        res = r.headers.get("resources", "")
        if "/" in res:
            total_vtex = int(res.split("/")[-1])

        skus = []
        for p in r.json():
            for sku in p.get("items", []):
                sellers = sku.get("sellers", [])
                if not sellers:
                    continue
                offer = sellers[0].get("commertialOffer", {})
                skus.append({
                    "fecha":          datetime.now().strftime("%Y-%m-%d"),
                    "product_id":     p.get("productId", ""),
                    "sku_id":         sku.get("itemId", ""),
                    "ean":            sku.get("ean", ""),
                    "nombre":         sku.get("nameComplete") or p.get("productName", ""),
                    "marca":          p.get("brand", ""),
                    "categoria":      cat_nombre,
                    "precio_actual":  offer.get("Price"),
                    "precio_regular": offer.get("ListPrice"),
                    "disponible":     offer.get("AvailableQuantity", 0),
                    "link":           p.get("link", ""),
                    "cat_principal":  cat_principal,
                })
        return skus, total_vtex, None

    except Exception as e:
        return [], 0, str(e)


def get_productos_categoria(parent_id, child_id, cat_nombre, cat_principal, session):
    fq = f"C:/{parent_id}/{child_id}/"

    # ── Página 0: obtener total real ──────────────────────────────────────────
    skus_p0, total_vtex, err = fetch_page((session, fq, 0, cat_nombre, cat_principal))
    if err:
        print(f" [Error inicial en {cat_nombre}]: {err}")
        return [], 0
    if not skus_p0:
        return [], total_vtex

    # ── Páginas restantes en paralelo ─────────────────────────────────────────
    limit = min(total_vtex, MAX_PRODS)
    offsets = list(range(PAGE_SIZE, limit, PAGE_SIZE))

    if not offsets:
        return skus_p0, total_vtex

    all_skus = list(skus_p0)
    page_args = [(session, fq, off, cat_nombre, cat_principal) for off in offsets]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_page, args): args[2] for args in page_args}
        for future in as_completed(futures):
            skus, _, err = future.result()
            if err:
                print(f" [Error offset {futures[future]} en {cat_nombre}]: {err}")
            else:
                all_skus.extend(skus)

    return all_skus, total_vtex


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    timestamp    = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_filename = OUTPUT_DIR / f"carrefour_{timestamp}.csv"
    csv_lock     = Lock()

    session    = crear_sesion()
    total_cats = len(CATEGORIAS)
    acumulado_skus = 0

    print(f"{'='*60}")
    print(f" CARREBOT - SCRAPER COMPLETO (74 CATEGORÍAS)")
    print(f" Inicia: {datetime.now().strftime('%H:%M:%S')}")
    print(f"{'='*60}\n")

    for i, (cat_principal, cat_nombre, p_id, c_id) in enumerate(CATEGORIAS, 1):
        print(f"[{i:02d}/{total_cats}] {cat_nombre.ljust(30)}", end=" ", flush=True)

        prods_cat, total_web = get_productos_categoria(p_id, c_id, cat_nombre, cat_principal, session)

        if prods_cat:
            df_temp = pd.DataFrame(prods_cat)

            with csv_lock:
                header_necesario = not csv_filename.exists()
                df_temp.to_csv(csv_filename, mode="a", index=False,
                               header=header_necesario, encoding="utf-8-sig")

            acumulado_skus += len(prods_cat)
            print(f"-> {len(prods_cat)} SKUs (Total: {acumulado_skus})")
        else:
            print("-> SIN DATOS/ERROR")

    print(f"\n{'='*60}")
    print(f" PROCESO TERMINADO")
    print(f" Total SKUs guardados: {acumulado_skus}")
    print(f" Archivo: {csv_filename}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
