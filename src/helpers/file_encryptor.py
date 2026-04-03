"""
Clase para encriptar archivos usando Fernet (AES-128-CBC).
La clave se lee desde la variable de entorno FERNET_KEY definida en .env.
"""

import os
from pathlib import Path

from cryptography.fernet import Fernet
from dotenv import load_dotenv

# Cargar variables de entorno desde .env (raiz del proyecto)
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_ENV_PATH)


class FileEncryptor:
    """
    Encripta archivos con Fernet (criptografia simetrica AES-128-CBC + HMAC).

    Uso basico:
        from helpers import FileEncryptor

        enc = FileEncryptor()                       # usa FERNET_KEY del .env
        enc.encrypt("datos.csv")                    # genera datos.csv.enc
        enc.encrypt("datos.csv", "salida.bin")      # nombre personalizado
    """

    # Extension que se anade al archivo encriptado por defecto
    EXTENSION = ".enc"

    def __init__(self, key: str | None = None) -> None:
        """
        Args:
            key: Clave Fernet en base-64.
                 Si no se proporciona, se lee de la variable FERNET_KEY del .env.
        """
        self._key = key or os.getenv("FERNET_KEY")
        if not self._key:
            raise ValueError(
                "No se encontro la clave de encriptacion. "
                "Define FERNET_KEY en el archivo .env o pasala como argumento."
            )
        self._fernet = Fernet(self._key.encode() if isinstance(self._key, str) else self._key)

    # ------------------------------------------------------------------
    # Metodos publicos
    # ------------------------------------------------------------------
    def encrypt(self, input_path: str, output_path: str | None = None) -> str:
        """
        Encripta un archivo completo.

        Args:
            input_path:  Ruta del archivo a encriptar.
            output_path: Ruta destino (opcional). Si no se indica, se agrega
                         la extension '.enc' al nombre original.

        Returns:
            Ruta absoluta del archivo encriptado generado.
        """
        input_path = Path(input_path).resolve()
        if not input_path.is_file():
            raise FileNotFoundError(f"No se encontro el archivo: {input_path}")

        if output_path is None:
            output_path = input_path.with_suffix(input_path.suffix + self.EXTENSION)
        else:
            output_path = Path(output_path).resolve()

        data = input_path.read_bytes()
        encrypted = self._fernet.encrypt(data)
        output_path.write_bytes(encrypted)

        print(f"  Archivo encriptado: {output_path}")
        return str(output_path)

    def encrypt_bytes(self, data: bytes) -> bytes:
        """Encripta bytes en memoria y devuelve el resultado."""
        return self._fernet.encrypt(data)

    @staticmethod
    def generate_key() -> str:
        """Genera una nueva clave Fernet (util para inicializar el .env)."""
        return Fernet.generate_key().decode()

