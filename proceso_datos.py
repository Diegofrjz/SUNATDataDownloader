# proceso_datos.py (Versión con lectura de Excel y generación directa, sinergia duh)
import os
import pandas as pd
from bs4 import BeautifulSoup
from typing import Dict, List, Any, Optional

def obtener_rucs_de_excels(ruta_buzon_eps: str, ruta_clientes_activos: str) -> List[str]:
    """
    Lee dos archivos Excel y devuelve una lista de RUCs que están en el primer archivo (Buzon EPS)
    pero NO están en el segundo archivo (Clientes Activos SAEPS).
    """
    # Nombres de columna esperados
    COLUMNA_RUC_BUZON = 'RUC'  # Columna en el primer Excel
    COLUMNA_RUC_CLIENTES = 'Ruc'  # Columna en el segundo Excel

    try:
        # Leer el primer archivo (LEADS MAIL EPS)
        df_buzon = pd.read_excel(ruta_buzon_eps, dtype=str)
        if COLUMNA_RUC_BUZON not in df_buzon.columns:
            print(f"⚠️ Advertencia: No se encontró la columna '{COLUMNA_RUC_BUZON}' en el archivo Buzon EPS.")
            return []
        
        # Leer el segundo archivo (BASE SAEPS)
        df_clientes = pd.read_excel(ruta_clientes_activos, dtype=str)
        if COLUMNA_RUC_CLIENTES not in df_clientes.columns:
            print(f"⚠️ Advertencia: No se encontró la columna '{COLUMNA_RUC_CLIENTES}' en el archivo Clientes Activos.")
            return []

        # Obtener RUCs del primer archivo (limpiando nulos y espacios)
        rucs_buzon = set(df_buzon[COLUMNA_RUC_BUZON].dropna().apply(str.strip).unique())
        
        # Obtener RUCs del segundo archivo (limpiando nulos y espacios)
        rucs_clientes = set(df_clientes[COLUMNA_RUC_CLIENTES].dropna().apply(str.strip).unique())
        
        # Encontrar RUCs que están en el primer archivo pero NO en el segundo
        rucs_a_procesar = rucs_buzon - rucs_clientes

        # Filtrar solo RUCs válidos (11 dígitos numéricos)
        rucs_validos = [ruc for ruc in rucs_a_procesar if ruc.isdigit() and len(ruc) == 11]
        
        print(f"\nAnálisis de RUCs:")
        print(f"📊 RUCs en Buzón EPS: {len(rucs_buzon)}")
        print(f"📊 RUCs en Base SAEPS: {len(rucs_clientes)}")
        print(f"🎯 RUCs únicos a procesar: {len(rucs_validos)}")
        
        return sorted(rucs_validos)  # Ordenamos la lista para procesamiento consistente

    except FileNotFoundError as e:
        print(f"❌ Error: No se pudo encontrar el archivo {e.filename}. Verifica las rutas.")
        return []
    except Exception as e:
        print(f"❌ Error leyendo los archivos Excel: {e}")
        return []


# Funciones de Parseo del html

def parse_principal_html(html_content: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html_content, 'html.parser')
    datos: Dict[str, Any] = {}
    ruc_header_label = soup.find('h4', string=lambda text: text and 'Número de RUC' in text)
    if ruc_header_label:
        value_container = ruc_header_label.parent.find_next_sibling('div')
        if value_container and value_container.find('h4'):
            full_text = value_container.find('h4').get_text(strip=True)
            try:
                numero_ruc, razon_social = full_text.split(' - ', 1)
                datos['Número de RUC'] = numero_ruc.strip()
                datos['Razón Social'] = razon_social.strip()
            except ValueError:
                datos['Número de RUC'] = full_text.strip()
    items = soup.find_all('div', class_='list-group-item')
    for item in items:
        titulo_tag = item.find(['h4', 'h5'], class_=['list-group-item-heading', 'list-group-item-heading'])
        if titulo_tag:
            clave = titulo_tag.get_text(strip=True).replace(':', '')
            valor_tag = item.find(['div', 'p'], class_=['list-group-item-text', None])
            valor = valor_tag.get_text(strip=True) if valor_tag else ''
            if valor:
                datos[clave] = valor
    return datos

def parse_trabajadores_html(html_content: str, ruc: str = '') -> List[Dict[str, Any]]:
    if 'no existen declaraciones presentadas' in html_content.lower():
        return [{'RUC': ruc, 'Mensaje': 'Sin declaraciones presentadas'}]
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table')
    if not table:
        return [{'RUC': ruc, 'Mensaje': 'No se encontró tabla de trabajadores'}]
    headers = [th.get_text(strip=True) for th in table.find_all('th')]
    rows = []
    for tr in table.find('tbody').find_all('tr'):
        cols = [td.get_text(strip=True) for td in tr.find_all('td')]
        if len(cols) == len(headers):
            row_data = {header: col for header, col in zip(headers, cols)}
            row_data['RUC'] = ruc
            rows.append(row_data)
    return rows

# --- Función Principal de Generación de Excel (MODIFICADA) ---

def convertir_a_numerico(valor: str) -> Any:
    """
    Intenta convertir un valor string a numérico si es posible.
    Retorna el valor original si no se puede convertir.
    """
    try:
        # Primero limpiamos el valor de posibles caracteres especiales
        valor_limpio = valor.strip().replace(',', '').replace(' ', '')
        
        # Si parece ser un número entero
        if valor_limpio.isdigit():
            return int(valor_limpio)
        
        # Si parece ser un número decimal
        try:
            return float(valor_limpio)
        except ValueError:
            pass
        
        # Si no se puede convertir, devolver el valor original
        return valor
    except (AttributeError, ValueError):
        return valor

def convertir_df_a_numerico(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte todas las columnas posibles a tipo numérico en un DataFrame.
    """
    df_converted = df.copy()
    
    for columna in df.columns:
        # Intentamos convertir cada columna a numérico
        try:
            # Aplicamos la conversión a cada valor de la columna
            df_converted[columna] = df[columna].apply(convertir_a_numerico)
            
            # Si todos los valores son numéricos, convertimos la columna completa
            if df_converted[columna].apply(lambda x: isinstance(x, (int, float))).all():
                df_converted[columna] = pd.to_numeric(df_converted[columna])
        except Exception:
            # Si hay algún error, mantenemos la columna como está
            continue
    
    return df_converted

def generar_reporte_desde_htmls(ruta_salida: str, rucs_a_procesar: Optional[List[str]] = None,
                                ruta_buzon_eps: Optional[str] = None,
                                ruta_clientes_activos: Optional[str] = None,
                                ruc_ya_cliente: bool = False):
    """
    Genera un reporte Excel a partir de los HTMLs.
    Si se provee 'rucs_a_procesar', solo incluirá esos RUCs en el reporte.
    Si 'ruc_ya_cliente' es True, genera un reporte sin consultar SUNAT (RUC ya existe en clientes).
    """
    print("\nIniciando la generación del reporte final desde archivos HTML...")
    directorio_salida = os.path.dirname(ruta_salida)
    carpeta_html = os.path.join(directorio_salida, 'html_consultas')

    # Si el RUC ya es cliente, generar un reporte sin datos SUNAT
    if ruc_ya_cliente and rucs_a_procesar:
        ruc_cliente = rucs_a_procesar[0]
        print(f"Generando reporte para RUC ya cliente: {ruc_cliente}")
        
        # Crear DataFrames vacíos para las pestañas SUNAT
        df_principal = pd.DataFrame()
        df_trabajadores = pd.DataFrame()
        
        # Generar la pestaña de VALIDACION FINAL directamente
        df_valid = pd.DataFrame()
        
        try:
            # Obtener info del RUC desde Clientes Activos
            df_clientes = None
            adm_val = ''
            if ruta_clientes_activos and os.path.isfile(ruta_clientes_activos):
                try:
                    df_clientes = pd.read_excel(ruta_clientes_activos, dtype=str)
                    if 'Ruc' in df_clientes.columns:
                        df_clientes['Ruc'] = df_clientes['Ruc'].astype(str).str.strip()
                        # Buscar ADM SAC
                        for cand in ['Adm SAC ACT', 'Adm SAC', 'Adm_SAC', 'ADM SAC ACT']:
                            if cand in df_clientes.columns:
                                try:
                                    adm_val = df_clientes.loc[df_clientes['Ruc'] == ruc_cliente, cand].iloc[0]
                                except Exception:
                                    adm_val = ''
                                break
                except Exception:
                    df_clientes = None
            
            # Crear DataFrame de validación
            df_valid = pd.DataFrame({
                'RUC': [pd.to_numeric(ruc_cliente, errors='coerce')],
                'CANAL': [''],
                'ADM SAC': [adm_val if pd.notna(adm_val) else ''],
                'Razón Social': [''],
                'Tipo Contibuyente': [''],
                'Estado del Contribuyente': [''],
                'Condición del Contribuyente': [''],
                'Cantidad de Trabajadores': ['']
            })
            
            # Asignar RESULTADO = 'Enviar Correo Administrador' para RUCs que ya son clientes
            df_valid['RESULTADO'] = 'Enviar Correo Administrador'
            
            # Convertir RUC a Int64
            df_valid['RUC'] = pd.to_numeric(df_valid['RUC'].astype(str).str.strip(), errors='coerce').astype('Int64')
            
        except Exception as e:
            print(f"⚠️ Error al generar reporte para RUC cliente: {e}")
        
        # Guardar el Excel
        with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
            df_valid.to_excel(writer, sheet_name='VALIDACION FINAL', index=False)
            df_principal.to_excel(writer, sheet_name='Principal_SUNAT', index=False)
            df_trabajadores.to_excel(writer, sheet_name='Trabajadores_SUNAT', index=False)
        
        print(f"✅ Reporte final guardado exitosamente en: {ruta_salida}")
        return

    if not os.path.isdir(carpeta_html):
        raise FileNotFoundError(f"Error: No se encontró la carpeta 'html_consultas'.")

    archivos_html = [f for f in os.listdir(carpeta_html) if f.lower().endswith('.html')]
    datos_principales, datos_trabajadores = [], []

    for nombre_archivo in archivos_html:
        try:
            partes_nombre = nombre_archivo.split('_')
            if len(partes_nombre) < 2 or partes_nombre[0].upper() != 'RUC':
                continue
            
            ruc = partes_nombre[1]
            # Si se especificó una lista de RUCs, ignorar los que no estén en ella
            if rucs_a_procesar and ruc not in rucs_a_procesar:
                continue

            ruta_completa = os.path.join(carpeta_html, nombre_archivo)
            with open(ruta_completa, 'r', encoding='utf-8') as f:
                contenido = f.read()
            
            if nombre_archivo.endswith('_principal.html'):
                datos_principales.append(parse_principal_html(contenido))
            elif nombre_archivo.endswith('_trabajadores.html'):
                datos_trabajadores.extend(parse_trabajadores_html(contenido, ruc=ruc))
        except Exception as e:
            print(f"⚠️ Error procesando el archivo {nombre_archivo}: {e}")

    if not datos_principales and not datos_trabajadores:
        print("⚠️ No se encontraron datos para generar el reporte.")
        return

    # Convertir a DataFrames
    df_principal = pd.DataFrame(datos_principales)
    df_trabajadores = pd.DataFrame(datos_trabajadores)
    
    # Antes de convertir columnas a numérico, limpiar la columna 'Período' en la pestaña de trabajadores
    if 'Período' in df_trabajadores.columns:
        try:
            # Reemplazar guiones y espacios, p.ej. '2025-09' -> '202509'
            df_trabajadores['Período'] = df_trabajadores['Período'].astype(str).str.replace('-', '', regex=False).str.replace(' ', '', regex=False)
            # Conversión estricta: transformar a numérico, forzando valores no válidos a NaN
            # y luego convertir a Int64 nullable para poder mantener NA si existen valores inválidos.
            df_trabajadores['Período'] = pd.to_numeric(df_trabajadores['Período'], errors='coerce').astype('Int64')
        except Exception as e:
            print(f"⚠️ No se pudo normalizar la columna 'Período': {e}")

    # Convertir columnas a numérico donde sea posible
    df_principal = convertir_df_a_numerico(df_principal)
    df_trabajadores = convertir_df_a_numerico(df_trabajadores)
    
    print(f"Procesamiento finalizado. Se incluirán {len(df_principal)} registros en la pestaña principal.")

    # Guardar con tipos de datos correctos
    with pd.ExcelWriter(ruta_salida, engine='openpyxl') as writer:
        # --- Generar pestaña de VALIDACION FINAL ---
        try:
            # Determinar nombres de columna candidatos
            def first_column(df, candidates):
                if df is None or df.empty:
                    return None
                for c in candidates:
                    if c in df.columns:
                        return c
                return None

            ruc_col = first_column(df_principal, ['Número de RUC', 'RUC', 'Ruc'])
            razon_col = first_column(df_principal, ['Razón Social', 'Razon Social', 'Razón_social'])
            tipo_col = first_column(df_principal, ['Tipo Contribuyente', 'Tipo de Contribuyente', 'TipoContribuyente'])

            # Base de RUCs desde la pestaña principal
            if ruc_col is None:
                df_valid = pd.DataFrame(columns=['RUC', 'CANAL', 'ADM SAC', 'Razón Social', 'Tipo Contibuyente'])
            else:
                df_valid = df_principal[[ruc_col]].copy()
                df_valid = df_valid.rename(columns={ruc_col: 'RUC'})

                # Añadir Razón Social y Tipo Contibutiente si existen
                if razon_col:
                    df_valid['Razón Social'] = df_principal[razon_col].astype(str)
                else:
                    df_valid['Razón Social'] = ''

                if tipo_col:
                    df_valid['Tipo Contibuyente'] = df_principal[tipo_col].astype(str)
                else:
                    df_valid['Tipo Contibuyente'] = ''

                # Nuevas columnas: Estado del Contribuyente y Condición del Contribuyente
                estado_col = first_column(df_principal, ['Estado del Contribuyente', 'Estado del Contribuyente ' , 'Estado'])
                condicion_col = first_column(df_principal, ['Condición del Contribuyente', 'Condicion del Contribuyente', 'Condición'])

                if estado_col:
                    df_valid['Estado del Contribuyente'] = df_principal[estado_col].astype(str)
                else:
                    df_valid['Estado del Contribuyente'] = ''

                if condicion_col:
                    df_valid['Condición del Contribuyente'] = df_principal[condicion_col].astype(str)
                else:
                    df_valid['Condición del Contribuyente'] = ''

                # Preparar CANAL desde el primer excel (RUCS Buzon EPS)
                df_buzon = None
                canal_col = None
                if ruta_buzon_eps and os.path.isfile(ruta_buzon_eps):
                    try:
                        df_buzon = pd.read_excel(ruta_buzon_eps, dtype=str)
                        canal_col = None
                        for cand in ['CANAL', 'Canal', 'canal']:
                            if cand in df_buzon.columns:
                                canal_col = cand
                                break
                        # Normalizar columna RUC del buzon
                        if 'RUC' in df_buzon.columns:
                            df_buzon['RUC'] = df_buzon['RUC'].astype(str).str.strip()
                    except Exception:
                        df_buzon = None

                # Preparar ADM SAC desde el segundo excel (Clientes Activos)
                df_clientes = None
                adm_col = None
                if ruta_clientes_activos and os.path.isfile(ruta_clientes_activos):
                    try:
                        df_clientes = pd.read_excel(ruta_clientes_activos, dtype=str)
                        for cand in ['Adm SAC ACT', 'Adm SAC', 'Adm_SAC', 'ADM SAC ACT']:
                            if cand in df_clientes.columns:
                                adm_col = cand
                                break
                        # Normalizar columna Ruc del clientes
                        if 'Ruc' in df_clientes.columns:
                            df_clientes['Ruc'] = df_clientes['Ruc'].astype(str).str.strip()
                    except Exception:
                        df_clientes = None

                # Lookup CANAL and ADM SAC
                # Inicializar columnas
                df_valid['CANAL'] = ''
                df_valid['ADM SAC'] = ''

                # Mapear CANAL
                if df_buzon is not None and canal_col is not None:
                    # Crear mapping de RUC -> CANAL
                    try:
                        mapping_canal = df_buzon.set_index(df_buzon['RUC'])[canal_col].to_dict()
                        df_valid['CANAL'] = df_valid['RUC'].astype(str).map(mapping_canal).fillna('')
                    except Exception:
                        df_valid['CANAL'] = ''

                # Mapear ADM SAC (si no existe en clientes, marcar como NUEVO)
                if df_clientes is not None and adm_col is not None:
                    try:
                        mapping_adm = df_clientes.set_index(df_clientes['Ruc'])[adm_col].to_dict()
                        df_valid['ADM SAC'] = df_valid['RUC'].astype(str).map(mapping_adm)
                        df_valid['ADM SAC'] = df_valid['ADM SAC'].fillna('NUEVO')
                    except Exception:
                        df_valid['ADM SAC'] = 'NUEVO'
                else:
                    # Si no hay dataframe de clientes, marcar todos como NUEVO
                    df_valid['ADM SAC'] = 'NUEVO'

                # --- Nueva columna: Cantidad de Trabajadores ---
                try:
                    # Determinar nombres de columna para período y trabajadores
                    period_col = first_column(df_trabajadores, ['Período', 'Periodo', 'PERIODO'])
                    trabajadores_col = None
                    if not df_trabajadores.empty:
                        for cand in ['N° de Trabajadores', 'N° Trabajadores', 'N° de Trabajadores', 'Numero de Trabajadores', 'N de Trabajadores', 'Nº de Trabajadores', 'Nro. Trabajadores', 'Trabajadores']:
                            if cand in df_trabajadores.columns:
                                trabajadores_col = cand
                                break

                    if period_col and trabajadores_col:
                        # Trabajar con copia y normalizar nombres de columna RUC en df_trabajadores
                        df_trab_trunc = df_trabajadores.copy()

                        # Localizar columna RUC en df_trab_trunc
                        ruc_col_candidates = ['RUC', 'Ruc', 'Ruc.', 'ruc']
                        ruc_col_trab = None
                        for cand in ruc_col_candidates:
                            if cand in df_trab_trunc.columns:
                                ruc_col_trab = cand
                                break

                        if ruc_col_trab is None:
                            # No se encontró columna RUC en trabajadores
                            df_valid['Cantidad de Trabajadores'] = ''
                        else:
                            # Normalizar RUCs como strings sin espacios
                            df_trab_trunc[ruc_col_trab] = df_trab_trunc[ruc_col_trab].astype(str).str.strip()
                            # Renombrar a 'RUC' para facilitar el merge/mapping
                            if ruc_col_trab != 'RUC':
                                df_trab_trunc = df_trab_trunc.rename(columns={ruc_col_trab: 'RUC'})

                            # Asegurar que periodo sea numérico y eliminar filas sin periodo válido
                            df_trab_trunc[period_col] = pd.to_numeric(df_trab_trunc[period_col], errors='coerce')
                            df_trab_trunc = df_trab_trunc.dropna(subset=[period_col])

                            if df_trab_trunc.empty or trabajadores_col not in df_trab_trunc.columns:
                                df_valid['Cantidad de Trabajadores'] = ''
                            else:
                                # Ordenar por periodo descendente y quedarnos con la primera entrada por RUC
                                latest = df_trab_trunc.sort_values(by=period_col, ascending=False).drop_duplicates(subset=['RUC'], keep='first')
                                # Normalizar df_valid RUCs
                                df_valid['RUC'] = df_valid['RUC'].astype(str).str.strip()
                                # Crear mapping RUC -> cantidad
                                mapping_trab = latest.set_index('RUC')[trabajadores_col].to_dict()
                                df_valid['Cantidad de Trabajadores'] = df_valid['RUC'].map(mapping_trab).fillna('')
                    else:
                        # Si no hay datos, dejar en blanco
                        df_valid['Cantidad de Trabajadores'] = ''
                except Exception as e:
                    print(f"⚠️ No se pudo obtener 'Cantidad de Trabajadores': {e}")

                # --- Agregar filas extra para RUCs que estaban en ambos inputs iniciales ---
                # Estos RUCs se obtienen como la intersección entre los RUCs del buzon y los RUCs de clientes activos.
                # Para cada uno, añadimos una fila con CANAL tomado del primer excel y ADM SAC tomado del segundo.
                # Resultado para estas filas será forzado más abajo a 'Enviar Correo Administrador'.
                rucs_cruzados_inter = []
                try:
                    if df_buzon is not None and df_clientes is not None:
                        # Normalizar nombres de columna de RUC en ambos dataframes
                        buzon_ruc_col = 'RUC' if 'RUC' in df_buzon.columns else None
                        clientes_ruc_col = 'Ruc' if 'Ruc' in df_clientes.columns else None

                        if buzon_ruc_col and clientes_ruc_col:
                            set_buzon = set(df_buzon[buzon_ruc_col].dropna().astype(str).str.strip())
                            set_clientes = set(df_clientes[clientes_ruc_col].dropna().astype(str).str.strip())
                            inter = sorted(set_buzon & set_clientes)
                            rucs_cruzados_inter = inter

                            # Evitar duplicados respecto a lo ya presente en df_valid
                            existing = set(df_valid['RUC'].astype(str).str.strip().unique())
                            rows_extra = []
                            for r in inter:
                                if r in existing:
                                    continue
                                # Obtener CANAL desde df_buzon
                                canal_val = ''
                                if canal_col and df_buzon is not None and 'RUC' in df_buzon.columns:
                                    try:
                                        canal_val = df_buzon.loc[df_buzon[buzon_ruc_col].astype(str).str.strip() == r, canal_col].iloc[0]
                                    except Exception:
                                        canal_val = ''

                                # Obtener ADM SAC desde df_clientes
                                adm_val = ''
                                if adm_col and df_clientes is not None and clientes_ruc_col in df_clientes.columns:
                                    try:
                                        adm_val = df_clientes.loc[df_clientes[clientes_ruc_col].astype(str).str.strip() == r, adm_col].iloc[0]
                                    except Exception:
                                        adm_val = ''

                                rows_extra.append({
                                    'RUC': r,
                                    'CANAL': canal_val if pd.notna(canal_val) else '',
                                    'ADM SAC': adm_val if pd.notna(adm_val) else '',
                                    'Razón Social': '',
                                    'Tipo Contibuyente': '',
                                    'Estado del Contribuyente': '',
                                    'Condición del Contribuyente': '',
                                    'Cantidad de Trabajadores': ''
                                })

                            if rows_extra:
                                df_extra = pd.DataFrame(rows_extra)
                                df_valid = pd.concat([df_valid, df_extra], ignore_index=True)
                except Exception as e:
                    print(f"⚠️ No se pudo agregar filas extra de RUCs cruzados: {e}")

            # Asegurar orden de columnas
            desired_cols = ['RUC', 'CANAL', 'ADM SAC', 'Razón Social', 'Tipo Contibuyente', 'Estado del Contribuyente', 'Condición del Contribuyente', 'Cantidad de Trabajadores']
            for c in desired_cols:
                if c not in df_valid.columns:
                    df_valid[c] = ''
            df_valid = df_valid[desired_cols]

            # --- Nueva columna: RESULTADO ---
            try:
                # Asumo la precedencia: 1) Tipo Contribuyente == 'PERSONA NATURAL SIN NEGOCIO' -> Derivar a Mary Huanay
                # 2) ADM SAC != 'NUEVO' -> Enviar Correo a Líder
                # 3) Sino, usar Cantidad de Trabajadores: <50 -> Asignar Nuevo, >=50 -> Enviar Correo a Líder
                tipo_ser = df_valid['Tipo Contibuyente'].astype(str).str.strip().str.upper()
                adm_ser = df_valid['ADM SAC'].astype(str).str.strip()
                # Convertir cantidad a numérico, NaN -> treated as 0 for decision
                cantidad_num = pd.to_numeric(df_valid['Cantidad de Trabajadores'], errors='coerce')
                cantidad_num_filled = cantidad_num.fillna(0)

                df_valid['RESULTADO'] = ''

                # 1) Tipo Contribuyente special case
                mask_tipo = tipo_ser == 'PERSONA NATURAL SIN NEGOCIO'
                df_valid.loc[mask_tipo, 'RESULTADO'] = 'Derivar a Mary Huanay'

                # 2) ADM SAC not NEW (aplica solo donde no se haya asignado RESULTADO)
                mask_remaining = df_valid['RESULTADO'] == ''
                mask_adm = adm_ser.str.upper() != 'NUEVO'
                df_valid.loc[mask_remaining & mask_adm, 'RESULTADO'] = 'Enviar Correo a Líder'

                # 3) Basado en cantidad de trabajadores para los restantes
                mask_remaining = df_valid['RESULTADO'] == ''
                df_valid.loc[mask_remaining & (cantidad_num_filled < 50), 'RESULTADO'] = 'Asignar Nuevo'
                df_valid.loc[mask_remaining & (cantidad_num_filled >= 50), 'RESULTADO'] = 'Enviar Correo a Líder'
            except Exception as e:
                print(f"⚠️ No se pudo calcular la columna 'RESULTADO': {e}")

            # Si existen RUCs que se agregaron por estar en ambos inputs iniciales,
            # forzamos su RESULTADO a 'Enviar Correo Administrador'.
            try:
                if 'rucs_cruzados_inter' in locals() and rucs_cruzados_inter:
                    mask_cruzados = df_valid['RUC'].astype(str).str.strip().isin(rucs_cruzados_inter)
                    df_valid.loc[mask_cruzados, 'RESULTADO'] = 'Enviar Correo Administrador'
            except Exception as e:
                print(f"⚠️ No se pudo forzar RESULTADO para rucs cruzados: {e}")

            # Asegurar que la columna RUC sea numérica (Int64 nullable) en la pestaña de validación
            try:
                # Limpiar espacios y convertir a numérico
                df_valid['RUC'] = pd.to_numeric(df_valid['RUC'].astype(str).str.strip(), errors='coerce').astype('Int64')
            except Exception as e:
                print(f"⚠️ No se pudo convertir 'RUC' a numérico en VALIDACION FINAL: {e}")

            # Escribir la hoja de validación PRIMERO para que sea la primera pestaña
            df_valid.to_excel(writer, sheet_name='VALIDACION FINAL', index=False)
            # Luego escribir las demás pestañas (referencia de origen)
            df_principal.to_excel(writer, sheet_name='Principal_SUNAT', index=False)
            df_trabajadores.to_excel(writer, sheet_name='Trabajadores_SUNAT', index=False)
        except Exception as e:
            print(f"⚠️ No se pudo generar la pestaña 'VALIDACION FINAL': {e}")
    print(f"✅ Reporte final guardado exitosamente en: {ruta_salida}")