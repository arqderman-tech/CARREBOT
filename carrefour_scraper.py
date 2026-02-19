"""
carrefour_scraper.py
====================
Scraper para carrefour.com.ar usando la API REST de VTEX.

API: GET /api/catalog_system/pub/products/search?fq=C:/parentId/childId/&_from=0&_to=49
- Paginación de 50 en 50
- Header 'resources: from-to/total' indica el total
- Máximo 2500 productos por subcategoría (límite VTEX)

Genera: output_carrefour/carrefour_YYYYMMDD_HHMMSS.csv
"""

import requests
import pandas as pd
import time
import os
from datetime import datetime
from pathlib import Path

# ── CONFIGURACIÓN ─────────────────────────────────────────────────────────────
BASE_URL   = "https://www.carrefour.com.ar"
PAGE_SIZE  = 50
MAX_PRODS  = 2500   # límite VTEX por query
DELAY      = 0.4    # segundos entre requests
OUTPUT_DIR = Path("output_carrefour")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "es-AR,es;q=0.9",
    "Referer": "https://www.carrefour.com.ar/",
}

# ── CATEGORÍAS A SCRAPEAR ─────────────────────────────────────────────────────
# Formato: (cat_principal, cat_nombre, parent_id, child_id)
# child_id = None → usar solo parent_id (fq=C:/parentId/)
CATEGORIAS = [
    # ── ALMACÉN ───────────────────────────────────────────────────────────────
    ("Almacén", "Aceites y vinagres",           161, 162),
    ("Almacén", "Pastas secas",                 161, 168),
    ("Almacén", "Arroz y legumbres",            161, 172),
    ("Almacén", "Harinas",                      161, 176),
    ("Almacén", "Enlatados y Conservas",        161, 183),
    ("Almacén", "Sal aderezos y saborizadores", 161, 190),
    ("Almacén", "Caldos sopas y puré",          161, 195),
    ("Almacén", "Repostería y postres",         161, 199),
    ("Almacén", "Snacks",                       161, 214),
    ("Almacén", "Comidas instantáneas",         161, 658),
    # ── DESAYUNO Y MERIENDA ───────────────────────────────────────────────────
    ("Almacén", "Golosinas y chocolates",       222, 208),
    ("Almacén", "Galletitas y tostadas",        222, 223),
    ("Almacén", "Budines y magdalenas",         222, 229),
    ("Almacén", "Yerba",                        222, 232),
    ("Almacén", "Café",                         222, 233),
    ("Almacén", "Infusiones",                   222, 238),
    ("Almacén", "Azúcar y endulzantes",         222, 242),
    ("Almacén", "Mermeladas y dulces",          222, 246),
    ("Almacén", "Cereales y barritas",          222, 250),
    # ── BEBIDAS CON ALCOHOL ───────────────────────────────────────────────────
    ("Bebidas Con Alcohol", "Cervezas",         255, 256),
    ("Bebidas Con Alcohol", "Vinos",            255, 257),
    ("Bebidas Con Alcohol", "Fernet y aperitivos", 255, 262),
    ("Bebidas Con Alcohol", "Bebidas blancas",  255, 266),
    ("Bebidas Con Alcohol", "Espumantes y sidras", 255, 273),
    # ── BEBIDAS SIN ALCOHOL ───────────────────────────────────────────────────
    ("Bebidas Sin Alcohol", "Gaseosas",         255, 277),
    ("Bebidas Sin Alcohol", "Aguas",            255, 283),
    ("Bebidas Sin Alcohol", "Jugos",            255, 286),
    ("Bebidas Sin Alcohol", "Bebidas energizantes", 255, 290),
    ("Bebidas Sin Alcohol", "Bebidas isotónicas", 255, 291),
    # ── FRESCOS ───────────────────────────────────────────────────────────────
    ("Frescos", "Leches",                       292, 293),
    ("Frescos", "Yogures",                      292, 299),
    ("Frescos", "Mantecas y margarinas",        292, 302),
    ("Frescos", "Cremas de leche",              292, 304),
    ("Frescos", "Postres",                      292, 305),
    ("Frescos", "Huevos",                       292, 306),
    ("Frescos", "Tapas y pastas frescas",       292, 307),
    ("Frescos", "Quesos",                       292, 310),
    ("Frescos", "Fiambres",                     292, 318),
    ("Frescos", "Carne vacuna",                 321, 322),
    ("Frescos", "Pollo y granja",               321, 323),
    ("Frescos", "Carne de cerdo",               321, 324),
    ("Frescos", "Pescados y mariscos",          321, 327),
    ("Frescos", "Frutas",                       330, 331),
    ("Frescos", "Verduras",                     330, 332),
    # ── CONGELADOS ────────────────────────────────────────────────────────────
    ("Congelados", "Hamburguesas y medallones", 347, 348),
    ("Congelados", "Nuggets y rebozados",       347, 349),
    ("Congelados", "Papas congeladas",          347, 350),
    ("Congelados", "Frutas y vegetales cong.",  347, 352),
    ("Congelados", "Comidas congeladas",        347, 356),
    ("Congelados", "Pescados cong.",            347, 357),
    ("Congelados", "Helados y postres",         347, 358),
    ("Congelados", "Pollo congelado",           347, 703),
    # ── LIMPIEZA ─────────────────────────────────────────────────────────────
    ("Limpieza", "Limpieza de la ropa",         359, 360),
    ("Limpieza", "Limpieza pisos y muebles",    359, 367),
    ("Limpieza", "Insecticidas",                359, 376),
    ("Limpieza", "Limpieza de cocina",          359, 377),
    ("Limpieza", "Lavandinas",                  359, 384),
    ("Limpieza", "Rollos y servilletas",        359, 385),
    ("Limpieza", "Papeles higiénicos",          359, 386),
    ("Limpieza", "Limpieza de baño",            359, 387),
    ("Limpieza", "Desodorantes de ambiente",    359, 390),
    ("Limpieza", "Artículos de limpieza",       359, 394),
    # ── CUIDADO PERSONAL ─────────────────────────────────────────────────────
    ("Cuidado Personal", "Cuidado del cabello", 402, 403),
    ("Cuidado Personal", "Cuidado dental",      402, 412),
    ("Cuidado Personal", "Jabones",             402, 418),
    ("Cuidado Personal", "Protección femenina", 402, 422),
    ("Cuidado Personal", "Cuidado de la piel",  402, 427),
    ("Cuidado Personal", "Antitranspirantes",   402, 435),
    ("Cuidado Personal", "Cuidado corporal",    402, 438),
    ("Cuidado Personal", "Repelentes",          402, 443),
    ("Cuidado Personal", "Algodones e hisopos", 402, 444),
    ("Cuidado Personal", "Pañales",             451, 452),
    ("Cuidado Personal", "Toallitas húmedas",   451, 453),
    ("Cuidado Personal", "Higiene para bebés",  451, 458),
]

# ── FUNCIONES ─────────────────────────────────────────────────────────────────
def get_productos_categoria(parent_id, child_id, cat_nombre, session):
    """Descarga todos los productos de una subcategoría paginando de 50 en 50."""
    fq = f"C:/{parent_id}/{child_id}/"
    productos = []
    from_idx = 0

    while from_idx < MAX_PRODS:
        to_idx = from_idx + PAGE_SIZE - 1
        url = f"{BASE_URL}/api/catalog_system/pub/products/search"
        params = {
            "fq": fq,
            "_from": from_idx,
            "_to": to_idx,
        }
        try:
            r = session.get(url, params=params, headers=HEADERS, timeout=20)
            r.raise_for_status()
            
            # Leer total del header
            resources = r.headers.get("resources", "")
            total = 0
            if "/" in resources:
                total = int(resources.split("/")[-1])

            data = r.json()
            if not data:
                break

            for p in data:
                items = p.get("items", [])
                if not items:
                    continue
                sku = items[0]
                sellers = sku.get("sellers", [])
                if not sellers:
                    continue
                offer = sellers[0].get("commertialOffer", {})
                
                # Solo productos con stock
                if offer.get("AvailableQuantity", 0) <= 0:
                    continue

                productos.append({
                    "product_id":    p.get("productId", ""),
                    "sku_id":        sku.get("itemId", ""),
                    "ean":           sku.get("ean", ""),
                    "nombre":        p.get("productName", ""),
                    "marca":         p.get("brand", ""),
                    "categoria":     cat_nombre,
                    "precio_actual": offer.get("Price"),
                    "precio_regular": offer.get("ListPrice"),
                    "disponible":    offer.get("AvailableQuantity", 0),
                })

            from_idx += PAGE_SIZE
            
            # Si ya bajamos todo, salir
            if total > 0 and from_idx >= min(total, MAX_PRODS):
                break
            if len(data) < PAGE_SIZE:
                break

            time.sleep(DELAY)

        except Exception as e:
            print(f"      ERROR en {fq} offset {from_idx}: {e}")
            time.sleep(2)
            break

    return productos, total


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    fecha_hoy = datetime.now().strftime("%Y%m%d")

    print(f"\n{'='*60}")
    print(f"  SCRAPER CARREFOUR — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    session = requests.Session()
    # Warmup: visitar la home para obtener cookies
    try:
        session.get(f"{BASE_URL}/", headers=HEADERS, timeout=10)
        time.sleep(0.5)
    except:
        pass

    todos = []
    total_cats = len(CATEGORIAS)

    for i, (cat_principal, cat_nombre, parent_id, child_id) in enumerate(CATEGORIAS, 1):
        print(f"[{i:02d}/{total_cats}] {cat_principal} › {cat_nombre} ...", end=" ", flush=True)
        
        productos, total_cat = get_productos_categoria(
            parent_id, child_id, cat_nombre, session
        )
        
        for p in productos:
            p["cat_principal"] = cat_principal
        
        todos.extend(productos)
        print(f"{len(productos)}/{total_cat} productos")
        time.sleep(DELAY)

    if not todos:
        print("\nERROR: No se obtuvieron productos.")
        return

    df = pd.DataFrame(todos)
    
    # Limpiar duplicados por product_id (puede aparecer en varias subcats)
    df = df.drop_duplicates(subset=["product_id"], keep="first")
    
    # Limpiar precios
    for col in ["precio_actual", "precio_regular"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # Filtrar precios inválidos
    df = df[df["precio_regular"] > 0]
    
    output_path = OUTPUT_DIR / f"carrefour_{timestamp}.csv"
    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"\n{'='*60}")
    print(f"  LISTO")
    print(f"  Productos únicos: {len(df)}")
    print(f"  Archivo: {output_path}")
    print(f"{'='*60}\n")
    
    # Resumen por categoría principal
    print("Resumen por categoría:")
    for cat, grp in df.groupby("cat_principal"):
        print(f"  {cat}: {len(grp)} productos")


if __name__ == "__main__":
    main()
