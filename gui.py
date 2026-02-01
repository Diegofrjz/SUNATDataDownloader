
# gui.py
import customtkinter as ctk
from tkinter import filedialog
import proceso_datos as logica_datos # Renombrado para mayor claridad
import web_scraping as ws
import pandas as pd
import sys
import threading
import os
import traceback
import time


class TextRedirector:
    """Redirige writes a una CTkTextbox de forma segura usando .after()."""
    def __init__(self, textbox, tag=None, orig_stream=None):
        self.textbox = textbox
        self.tag = tag
        self.orig = orig_stream

    def write(self, message):
        # Mantener también el comportamiento original (opcional)
        if self.orig:
            try:
                self.orig.write(message)
            except Exception:
                pass
        # Enviar al textbox en el hilo principal
        def append():
            try:
                self.textbox.configure(state="normal")
                self.textbox.insert("end", message)
                self.textbox.see("end")
                self.textbox.configure(state="disabled")
            except Exception:
                pass
        try:
            self.textbox.after(0, append)
        except Exception:
            append()

    def flush(self):
        if self.orig:
            try:
                self.orig.flush()
            except Exception:
                pass


class App(ctk.CTk):
    def __init__(self):
        super().__init__()

        # --- CONFIGURACIÓN DE LA VENTANA PRINCIPAL ---
        self.title("ASIGNACION DE LEADS - EPS REGULAR")
        self.geometry("1000x600")
        self.minsize(900, 520)
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")

        # --- VARIABLES PARA ALMACENAR RUTAS ---
        self.ruta_buzon_eps = ""
        self.ruta_clientes_activos = ""
        self.ruta_base_bpm = ""  # NUEVO: para la base BPM
        self.ruta_guardado = "" # NUEVO: para el archivo de salida
        self.ruc_directo = ""  # NUEVO: valor del RUC ingresado

        # Guardar streams originales
        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr

        # --- WIDGETS DE LA INTERFAZ ---
        self.crear_widgets()

    def crear_widgets(self):
        """Crea y posiciona todos los elementos de la GUI."""
        # Frame principal con padding reducido para look minimalista
        container = ctk.CTkFrame(self, corner_radius=8)
        container.pack(fill="both", expand=True, padx=16, pady=16)

        # Grid: columna 0 = controles, columna 1 = consola
        container.grid_columnconfigure(0, weight=0, minsize=360)
        container.grid_columnconfigure(1, weight=1)

        left_frame = ctk.CTkFrame(container, fg_color="transparent")
        left_frame.grid(row=0, column=0, sticky="nsew", padx=(0,12), pady=6)

        right_frame = ctk.CTkFrame(container)
        right_frame.grid(row=0, column=1, sticky="nsew", padx=(12,0), pady=6)

        # --- Controles minimalistas ---
        header = ctk.CTkLabel(left_frame, text="VALIDADOR LEADS SUNAT", font=ctk.CTkFont(size=18, weight="bold"))
        header.pack(anchor="w", pady=(6,12))

        help_lbl = ctk.CTkLabel(left_frame, text="Selecciona los archivos y la ubicación de salida.\nLa consola mostrará el progreso.", text_color="#b8c1d4", justify="left")
        help_lbl.pack(anchor="w", pady=(0,12))

        # Campo nuevo: entrada directa de RUC para búsqueda rápida
        ruc_label = ctk.CTkLabel(left_frame, text="Búsqueda individual por RUC (opcional)")
        ruc_label.pack(anchor="w", pady=(6,2))
        self.entry_ruc = ctk.CTkEntry(left_frame, placeholder_text="Ingrese número de RUC", corner_radius=6)
        self.entry_ruc.pack(fill="x", pady=(0,8))
        # Actualizar estado cuando el usuario escribe
        try:
            # bind KeyRelease para mantener compatibilidad con tkinter events
            self.entry_ruc.bind("<KeyRelease>", lambda e: self.validar_entrada_ruc())
        except Exception:
            pass

        # Buttons y etiquetas
        self.btn_buzon = ctk.CTkButton(left_frame, text="1 • Leads Buzon EPS (RUCS)", command=self.seleccionar_buzon_eps, corner_radius=6)
        self.btn_buzon.pack(fill="x", pady=(6,4))
        self.lbl_buzon = ctk.CTkLabel(left_frame, text="No seleccionado", text_color="#9aa7bf")
        self.lbl_buzon.pack(anchor="w", pady=(0,8))

        self.btn_clientes = ctk.CTkButton(left_frame, text="2 • Clientes Activos (SAEPS)", command=self.seleccionar_clientes_activos, corner_radius=6)
        self.btn_clientes.pack(fill="x", pady=(6,4))
        self.lbl_clientes = ctk.CTkLabel(left_frame, text="No seleccionado", text_color="#9aa7bf")
        self.lbl_clientes.pack(anchor="w", pady=(0,8))

        self.btn_base_bpm = ctk.CTkButton(left_frame, text="3 • Base BPM", command=self.seleccionar_base_bpm, corner_radius=6)
        self.btn_base_bpm.pack(fill="x", pady=(6,4))
        self.lbl_base_bpm = ctk.CTkLabel(left_frame, text="No seleccionado", text_color="#9aa7bf")
        self.lbl_base_bpm.pack(anchor="w", pady=(0,8))

        # Boton ubicacion guardado (cambiar color tmb aqui para la ux)
        self.btn_guardar = ctk.CTkButton(left_frame, text="4 • Ubicación Guardado", command=self.seleccionar_ruta_guardado, corner_radius=6)
        self.btn_guardar.pack(fill="x", pady=(6,4))
        self.lbl_guardar = ctk.CTkLabel(left_frame, text="No seleccionado", text_color="#9aa7bf")
        self.lbl_guardar.pack(anchor="w", pady=(0,14))

        # Acción principal y estado
        self.btn_procesar = ctk.CTkButton(left_frame, text="Iniciar Proceso", command=self.iniciar_proceso, state="disabled", fg_color="#0afd83", hover_color="#0b8b89", corner_radius=8)
        self.btn_procesar.pack(fill="x", pady=(8,6))

        self.lbl_estado = ctk.CTkLabel(left_frame, text="Esperando selección...", height=40, text_color="#cfece9")
        self.lbl_estado.pack(fill="x", pady=(8,4))

        # --- Consola a la derecha ---
        console_header = ctk.CTkLabel(right_frame, text="Consola", anchor="w", font=ctk.CTkFont(size=14, weight="bold"))
        console_header.pack(fill="x", pady=(6,6), padx=8)

        # CTkTextbox si está disponible, si no se podría usar tkinter.Text
        try:
            self.console = ctk.CTkTextbox(right_frame, wrap="word", state="disabled", corner_radius=6)
        except Exception:
            import tkinter as tk
            self.console = tk.Text(right_frame, wrap="word", state="disabled", bg="#1b2b35", fg="#e6eef4", insertbackground="#e6eef4", relief="flat")
        self.console.pack(fill="both", expand=True, padx=8, pady=(0,8))

        # Botones rápidos (limpiar consola)
        bottom_bar = ctk.CTkFrame(right_frame, fg_color="transparent")
        bottom_bar.pack(fill="x", padx=8, pady=(0,8))
        limpiar_btn = ctk.CTkButton(bottom_bar, text="Limpiar", width=80, command=self.limpiar_consola, corner_radius=6)
        limpiar_btn.pack(side="right")

    def seleccionar_buzon_eps(self):
        """Abre el diálogo para seleccionar el archivo RUCS BUZON EPS."""
        ruta = filedialog.askopenfilename(
            title="Seleccionar RUCS Buzon EPS",
            filetypes=[("Archivos de Excel", "*.xlsx *.xls")]
        )
        if ruta:
            self.ruta_buzon_eps = ruta
            self.lbl_buzon.configure(text=os.path.basename(ruta))
            self.verificar_rutas()

    def seleccionar_clientes_activos(self):
        """Abre el diálogo para seleccionar el archivo CLIENTES ACTIVOS (SAEPS)."""
        ruta = filedialog.askopenfilename(
            title="Seleccionar CLIENTES ACTIVOS (SAEPS)",
            filetypes=[("Archivos de Excel", "*.xlsx *.xls")]
        )
        if ruta:
            self.ruta_clientes_activos = ruta
            self.lbl_clientes.configure(text=os.path.basename(ruta))
            self.verificar_rutas()

    def seleccionar_base_bpm(self):
        """Abre el diálogo para seleccionar el archivo BASE BPM."""
        ruta = filedialog.askopenfilename(
            title="Seleccionar BASE BPM",
            filetypes=[("Archivos de Excel", "*.xlsx *.xls")]
        )
        if ruta:
            self.ruta_base_bpm = ruta
            self.lbl_base_bpm.configure(text=os.path.basename(ruta))
            self.verificar_rutas()

    def seleccionar_ruta_guardado(self):
        """Abre el diálogo para seleccionar dónde guardar el archivo final."""
        ruta = filedialog.asksaveasfilename(
            title="Guardar REPORTE FINAL como...",
            defaultextension=".xlsx",
            filetypes=[("Archivo Excel", "*.xlsx")]
        )
        if ruta:
            self.ruta_guardado = ruta
            self.lbl_guardar.configure(text=os.path.basename(ruta))
            self.verificar_rutas()

    def verificar_rutas(self):
        """Activa el botón de procesar solo si las TRES rutas han sido seleccionadas."""
        # Condición 1: búsqueda directa por RUC + ubicación de guardado
        ruc_val = ""
        try:
            ruc_val = self.entry_ruc.get().strip()
        except Exception:
            ruc_val = ""

        # Si hay un RUC ingresado, bloquear solo el botón de Leads Buzon EPS
        if ruc_val:
            self.btn_buzon.configure(state="disabled")
        else:
            self.btn_buzon.configure(state="normal")

        # Para búsqueda individual por RUC: requiere Clientes Activos y Guardado
        if ruc_val and self.ruta_clientes_activos and self.ruta_guardado:
            self.btn_procesar.configure(state="normal")
            self.lbl_estado.configure(text="Listo para búsqueda por RUC", text_color="#b8f0e9")
            return

        # Condición 2: flujo tradicional con ambos excels seleccionados
        if self.ruta_buzon_eps and self.ruta_clientes_activos and self.ruta_guardado:
            self.btn_procesar.configure(state="normal")
            self.lbl_estado.configure(text="Listo para procesar archivos", text_color="#b8f0e9")
        else:
            self.btn_procesar.configure(state="disabled")
            self.lbl_estado.configure(text="Esperando selección...", text_color="#cfece9")

    def limpiar_consola(self):
        try:
            self.console.configure(state="normal")
            self.console.delete("1.0", "end")
            self.console.configure(state="disabled")
        except Exception:
            pass

    def validar_entrada_ruc(self):
        """Valida que solo se permitan números en el campo RUC y actualiza estados de botones."""
        try:
            valor_actual = self.entry_ruc.get()
            # Filtrar solo dígitos
            valor_limpio = ''.join(c for c in valor_actual if c.isdigit())
            # Si el valor cambió, actualizarlo
            if valor_actual != valor_limpio:
                self.entry_ruc.delete(0, "end")
                self.entry_ruc.insert(0, valor_limpio)
        except Exception:
            pass
        # Actualizar estados
        self.verificar_rutas()

    def ruc_existe_en_clientes_activos(self, ruc: str) -> bool:
        """Verifica si un RUC existe en el archivo Clientes Activos."""
        if not self.ruta_clientes_activos:
            return False
        try:
            df_clientes = pd.read_excel(self.ruta_clientes_activos, dtype=str)
            if 'Ruc' not in df_clientes.columns:
                return False
            rucs_clientes = set(df_clientes['Ruc'].dropna().apply(str.strip).unique())
            return ruc in rucs_clientes
        except Exception:
            return False

    def iniciar_proceso(self):
        """Lanza el proceso en un hilo y redirige stdout/stderr a la consola."""
        # Deshabilitar botones
        self.btn_procesar.configure(state="disabled")
        self.btn_buzon.configure(state="disabled")
        self.btn_clientes.configure(state="disabled")
        self.btn_guardar.configure(state="disabled")
        self.lbl_estado.configure(text="Procesando...", text_color="#fff")

        # Redirigir outputs
        sys.stdout = TextRedirector(self.console, orig_stream=self._orig_stdout)
        sys.stderr = TextRedirector(self.console, orig_stream=self._orig_stderr)

        # Ejecutar en hilo para no bloquear la GUI
        thread = threading.Thread(target=self._run_proceso_thread, daemon=True)
        thread.start()

    def _run_proceso_thread(self):
        try:
            ruc_val = self.entry_ruc.get().strip()
            ruta_directorio_base = os.path.dirname(self.ruta_guardado) if self.ruta_guardado else os.getcwd()

            if ruc_val:
                # --- FLUJO 1: BÚSQUEDA DIRECTA DE UN SOLO RUC ---
                print(f"--- Iniciando Búsqueda Directa para RUC: {ruc_val} ---")
                
                # Paso 0: Validar si el RUC existe en Clientes Activos
                if self.ruc_existe_en_clientes_activos(ruc_val):
                    print(f"⚠️ El RUC {ruc_val} ya existe en Clientes Activos (SAEPS)")
                    print(f"✅ Generando reporte con resultado: Enviar Correo Administrador")
                    # Generar reporte con el RUC marcado como "Enviar Correo Administrador"
                    logica_datos.generar_reporte_desde_htmls(
                        ruta_salida=self.ruta_guardado,
                        rucs_a_procesar=[ruc_val],
                        ruta_buzon_eps=self.ruta_buzon_eps,
                        ruta_clientes_activos=self.ruta_clientes_activos,
                        ruc_ya_cliente=True  # Marcar que ya es cliente
                    )
                else:
                    # Paso 1: Consultar y guardar HTMLs
                    exito = ws.consultar_y_guardar_todo(ruc_val, ruta_directorio_base)
                    
                    # Paso 2: Generar Excel inmediatamente si la consulta fue exitosa
                    if exito:
                        logica_datos.generar_reporte_desde_htmls(
                            ruta_salida=self.ruta_guardado,
                            rucs_a_procesar=[ruc_val], # Procesar solo el RUC actual
                            ruta_buzon_eps=self.ruta_buzon_eps,
                            ruta_clientes_activos=self.ruta_clientes_activos
                        )
                    else:
                        print(f"❌ No se pudo generar el reporte porque la consulta para {ruc_val} falló.")

            else:
                # --- FLUJO 2: PROCESAMIENTO EN LOTE DESDE ARCHIVOS EXCEL ---
                print(f"--- Iniciando Proceso en Lote desde Archivos Excel ---")
                
                # Paso 1: Obtener la lista de RUCs desde los archivos
                lista_rucs = logica_datos.obtener_rucs_de_excels(
                    ruta_buzon_eps=self.ruta_buzon_eps,
                    ruta_clientes_activos=self.ruta_clientes_activos
                )

                if not lista_rucs:
                    print("No se encontraron RUCs para procesar. Proceso detenido.")
                    raise ValueError("No hay RUCs para procesar.")

                # Paso 2: Consultar cada RUC de la lista
                rucs_procesados_ok = []
                for i, ruc in enumerate(lista_rucs, 1):
                    print(f"\n[{i}/{len(lista_rucs)}] Procesando RUC: {ruc}")
                    exito = ws.consultar_y_guardar_todo(ruc, ruta_directorio_base)
                    if exito:
                        rucs_procesados_ok.append(ruc)
                    time.sleep(1) # Pequeña pausa para no saturar el servidor

                # Paso 3: Generar un único reporte consolidado
                if rucs_procesados_ok:
                    logica_datos.generar_reporte_desde_htmls(
                        ruta_salida=self.ruta_guardado,
                        rucs_a_procesar=rucs_procesados_ok,
                        ruta_buzon_eps=self.ruta_buzon_eps,
                        ruta_clientes_activos=self.ruta_clientes_activos
                    )
                else:
                    print("❌ No se pudo consultar exitosamente ningún RUC de la lista.")

            # Mensaje final de éxito
            self.after(0, lambda: self.lbl_estado.configure(text="Proceso completado ✔", text_color="#a7f3d0"))

        except Exception as e:
            traceback.print_exc()
            self.after(0, lambda: self.lbl_estado.configure(text=f"Error: Proceso detenido", text_color="#ffb4b4"))
        
        finally:
            # Restaurar la GUI al finalizar
            def _finalizar():
                sys.stdout = self._orig_stdout
                sys.stderr = self._orig_stderr
                self.btn_procesar.configure(state="normal")
                self.btn_buzon.configure(state="normal")
                self.btn_clientes.configure(state="normal")
                self.btn_guardar.configure(state="normal")
            self.after(0, _finalizar)