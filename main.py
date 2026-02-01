# main.py
import sys

def iniciar_aplicacion_principal():
    """
    Importa y ejecuta la aplica ción principal de CustomTkinter.
    """
    from gui import App
    app = App()
    app.mainloop()


if __name__ == "__main__":
    # Arrancar la app principal directamente
    try:
        iniciar_aplicacion_principal()
    except Exception as e:
        print(f"Error al iniciar la aplicación principal: {e}")
        sys.exit(1)