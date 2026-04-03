"""
Clase encargada de la extraccion de datos:
  - Descarga del archivo .zip.enc desde una URL remota
  - Desencriptacion del archivo usando Fernet
  - Descompresion del archivo ZIP en datasets/csv
"""

import os
import zipfile
from pathlib import Path

import requests
from dotenv import load_dotenv

from helpers.file_decryptor import FileDecryptor

# Cargar variables de entorno desde .env (raiz del proyecto)
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_ENV_PATH = _PROJECT_ROOT / ".env"
load_dotenv(_ENV_PATH)


class DataExtraction:
    """
    Extrae datos desde una fuente remota encriptada.

    Pipeline:
        1. download() - Descarga el archivo .zip.enc desde URL remota
        2. decrypt()  - Desencripta el archivo usando Fernet (AES-128)
        3. unzip()    - Descomprime el ZIP en datasets/csv

    Uso:
        from data_extraction import DataExtraction

        extraction = DataExtraction()
        extraction.run()  # Ejecuta: download() -> decrypt() -> unzip()
    """

    def __init__(
        self,
        url: str | None = None,
        ruta_zip_enc: str | None = None,
        ruta_zip: str | None = None,
        ruta_destino: str | None = None,
    ) -> None:
        """
        Args:
            url:          URL del archivo .zip.enc a descargar.
                          Si no se proporciona, se lee de DATA_URL en .env.
            ruta_zip_enc: Ruta donde se guardara el archivo encriptado descargado.
            ruta_zip:     Ruta donde se guardara el archivo ZIP desencriptado.
            ruta_destino: Carpeta destino para extraer el contenido del ZIP.
        """
        datasets = _PROJECT_ROOT / "datasets"

        self._url = url or os.getenv("DATA_SOURCE_URL")
        self._ruta_zip_enc = Path(ruta_zip_enc) if ruta_zip_enc else datasets / "csv" / "datos.zip.enc"
        self._ruta_zip = Path(ruta_zip) if ruta_zip else datasets / "csv" / "datos.zip"
        self._ruta_destino = Path(ruta_destino) if ruta_destino else datasets / "csv"

        # Crear carpetas si no existen
        self._ruta_zip_enc.parent.mkdir(parents=True, exist_ok=True)
        self._ruta_destino.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Metodos publicos
    # ------------------------------------------------------------------
    def download(self) -> str:
        """
        Descarga el archivo encriptado desde la URL configurada.

        Returns:
            Ruta absoluta del archivo descargado.

        Raises:
            ValueError: Si no hay URL configurada.
            requests.HTTPError: Si la descarga falla.
        """
        if not self._url:
            raise ValueError(
                "No se encontro la URL del archivo. "
                "Define DATA_URL en el archivo .env o pasala como argumento."
            )

        print(f"  Descargando archivo desde URL remota...")
        print(f"    URL: {self._url}")
        print(f"    Destino: {self._ruta_zip_enc}")

        response = requests.get(self._url, stream=True, timeout=300)
        response.raise_for_status()

        # Guardar en chunks para archivos grandes
        with open(self._ruta_zip_enc, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"  Descarga completada: {self._ruta_zip_enc}")
        return str(self._ruta_zip_enc)

    def decrypt(self) -> str:
        """
        Desencripta el archivo .zip.enc usando Fernet.

        Returns:
            Ruta absoluta del archivo ZIP desencriptado.
        """
        print(f"  Desencriptando archivo...")
        print(f"    Origen:  {self._ruta_zip_enc}")
        print(f"    Destino: {self._ruta_zip}")

        decryptor = FileDecryptor()
        decryptor.decrypt(str(self._ruta_zip_enc), str(self._ruta_zip))

        print(f"  Desencriptacion completada: {self._ruta_zip}")
        return str(self._ruta_zip)

    def unzip(self) -> str:
        """
        Descomprime el archivo ZIP en la carpeta destino.

        Returns:
            Ruta absoluta de la carpeta donde se extrajeron los archivos.
        """
        print(f"  Descomprimiendo archivo ZIP...")
        print(f"    Origen:  {self._ruta_zip}")
        print(f"    Destino: {self._ruta_destino}")

        with zipfile.ZipFile(self._ruta_zip, "r") as zip_ref:
            zip_ref.extractall(self._ruta_destino)

        print(f"  Descompresion completada: {self._ruta_destino}")
        return str(self._ruta_destino)

    def run(self) -> None:
        """
        Ejecuta el pipeline completo de extraccion:
            download() -> decrypt() -> unzip()
        """
        print("=" * 60)
        print("INICIANDO EXTRACCION DE DATOS")
        print("=" * 60)

        self.download()
        self.decrypt()
        self.unzip()

        print("=" * 60)
        print("EXTRACCION COMPLETADA")
        print("=" * 60)

