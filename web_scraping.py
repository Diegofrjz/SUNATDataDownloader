# web_scraping.py (Versión Simplificada)
from playwright.sync_api import sync_playwright, Page, TimeoutError as PlaywrightTimeoutError
import atexit
import os
import time

# --- Variables Globales y Funciones de Inicialización/Limpieza (sin cambios) ---
_playwright = None
_browser = None
_page = None

def _initialize_browser_edge():
    """Inicializa Playwright y lanza Microsoft Edge."""
    global _playwright, _browser, _page
    if _page:
        return

    print("Iniciando Playwright...")
    _playwright = sync_playwright().start()

    print("🚀 Lanzando navegador Microsoft Edge...")
    try:
        _browser = _playwright.chromium.launch(
            channel="msedge",
            headless=True
        )
        context = _browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36 Edg/110.0.1587.63'
        )
        _page = context.new_page()
        print("✅ Navegador Edge listo.")
    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO: No se pudo iniciar Microsoft Edge: {e}")
        raise SystemExit("Abortando ejecución.")

def _cleanup():
    """Cierra el navegador y Playwright de forma segura."""
    if _browser:
        print("\nCerrando navegador...")
        _browser.close()
    if _playwright:
        _playwright.stop()
    print("Recursos de Playwright liberados.")

atexit.register(_cleanup)

def guardar_html(ruc: str, html_content: str, ruta_base: str, sufijo: str):
    """Guarda el contenido HTML en la carpeta 'html_consultas'."""
    if not html_content or not ruta_base:
        return
    try:
        directorio_html = os.path.join(ruta_base, "html_consultas")
        os.makedirs(directorio_html, exist_ok=True)
        nombre_archivo = f"RUC_{ruc}{sufijo}.html"
        ruta_archivo = os.path.join(directorio_html, nombre_archivo)
        with open(ruta_archivo, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"📄 HTML guardado como: {nombre_archivo}")
    except Exception as e:
        print(f"⚠️ ADVERTENCIA: No se pudo guardar el archivo HTML para RUC {ruc} ({sufijo}): {e}")

# --- Función Principal de Scraping (sin cambios en su lógica interna) ---
def consultar_y_guardar_todo(ruc: str, ruta_base_guardado: str) -> bool:
    """
    Consulta un RUC, guarda el HTML principal y el de trabajadores.
    Devuelve True si tuvo éxito al obtener el HTML principal, False en caso contrario.
    """
    _initialize_browser_edge()
    url_consulta = "https://e-consultaruc.sunat.gob.pe/cl-ti-itmrconsruc/jcrS00Alias"
    print(f"🔎 Consultando RUC: {ruc}...")
    max_intentos = 3

    for intento in range(max_intentos):
        try:
            # --- FASE 1: OBTENER PÁGINA PRINCIPAL ---
            _page.goto(url_consulta, wait_until='domcontentloaded', timeout=45000)
            _page.locator('input#txtRuc').fill(ruc)
            _page.locator('button#btnAceptar').click()
            _page.wait_for_selector('div.list-group', timeout=45000)
            
            html_principal = _page.content()
            guardar_html(ruc, html_principal, ruta_base_guardado, "_principal")

            # --- FASE 2: OBTENER PÁGINA DE TRABAJADORES ---
            try:
                print("   Buscando botón de 'Cantidad de Trabajadores'...")
                boton_trabajadores = _page.locator('button:has-text("Cantidad de Trabajadores")')
                boton_trabajadores.click()
                print("   ✅ Clic realizado. Esperando página de trabajadores...")
                _page.wait_for_load_state('networkidle', timeout=30000)
                html_trabajadores = _page.content()
                guardar_html(ruc, html_trabajadores, ruta_base_guardado, "_trabajadores")
                _page.go_back()
            except PlaywrightTimeoutError:
                print("   ⚠️ No se encontró el botón de 'Cantidad de Trabajadores' o la página no cargó.")
            except Exception as e_click:
                print(f"   ⚠️ Error al obtener datos de trabajadores: {e_click}")
            
            return True # Éxito

        except Exception as e:
            print(f"   ⚠️ Falló el intento {intento + 1}: {e}")
            if intento < max_intentos - 1:
                time.sleep(3)
            else:
                print(f"❌ Se superaron los {max_intentos} intentos para el RUC {ruc}.")
                return False # Fracaso
    return False

# --- LA FUNCIÓN extraer_datos_de_html() HA SIDO ELIMINADA ---