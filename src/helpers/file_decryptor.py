"""
Clase para desencriptar archivos previamente encriptados con FileEncryptor.
La clave se lee desde la variable de entorno FERNET_KEY definida en .env.
"""

import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv

# Cargar variables de entorno desde .env (raiz del proyecto)
_ENV_PATH = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_ENV_PATH)


class FileDecryptor:
    """
    Desencripta archivos cifrados con Fernet (AES-128-CBC + HMAC).

    Uso basico:
        from helpers import FileDecryptor

        dec = FileDecryptor()                           # usa FERNET_KEY del .env
        dec.decrypt("datos.csv.enc")                    # genera datos.csv
        dec.decrypt("datos.csv.enc", "restaurado.csv")  # nombre personalizado
    """

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
                "No se encontro la clave de desencriptacion. "
                "Define FERNET_KEY en el archivo .env o pasala como argumento."
            )
        self._fernet = Fernet(self._key.encode() if isinstance(self._key, str) else self._key)

    # ------------------------------------------------------------------
    # Metodos publicos
    # ------------------------------------------------------------------
    def decrypt(self, input_path: str, output_path: str | None = None) -> str:
        """
        Desencripta un archivo previamente cifrado con FileEncryptor.

        Args:
            input_path:  Ruta del archivo encriptado (.enc).
            output_path: Ruta destino (opcional). Si no se indica, se elimina
                         la extension '.enc' del nombre.

        Returns:
            Ruta absoluta del archivo desencriptado generado.

        Raises:
            cryptography.fernet.InvalidToken: Si la clave es incorrecta o
                                              el archivo fue alterado.
        """
        input_path = Path(input_path).resolve()
        if not input_path.is_file():
            raise FileNotFoundError(f"No se encontro el archivo: {input_path}")

        if output_path is None:
            if input_path.suffix == self.EXTENSION:
                output_path = input_path.with_suffix("")
            else:
                output_path = input_path.with_name(input_path.stem + "_decrypted" + input_path.suffix)
        else:
            output_path = Path(output_path).resolve()

        encrypted_data = input_path.read_bytes()

        try:
            decrypted = self._fernet.decrypt(encrypted_data)
        except InvalidToken:
            raise ValueError(
                "No se pudo desencriptar el archivo. "
                "Verifica que la clave FERNET_KEY sea la misma que se uso para encriptar."
            )

        output_path.write_bytes(decrypted)

        print(f"  Archivo desencriptado: {output_path}")
        return str(output_path)

    def decrypt_bytes(self, data: bytes) -> bytes:
        """Desencripta bytes en memoria y devuelve el resultado."""
        try:
            return self._fernet.decrypt(data)
        except InvalidToken:
            raise ValueError(
                "No se pudo desencriptar los datos. "
                "Verifica que la clave FERNET_KEY sea correcta."
            )

