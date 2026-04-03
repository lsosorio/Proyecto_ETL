"""
Clase encargada de la transformacion de datos:
  - Conversion de CSV Latin-1 a UTF-8
  - Normalizacion de tildes y caracteres especiales
  - Consultas agregadas al PyG (filtros, agrupaciones, formato largo/ancho)
  - Generacion de archivo Parquet optimizado
"""

from pathlib import Path
from functools import reduce

import polars as pl

# ── Configuracion de Polars ──────────────────────────────────────────
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(1000)

# ── Rutas base del proyecto ──────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DataTransformation:
    """
    Transforma los datos del PyG: limpieza, normalizacion y agregaciones.

    Uso:
        from data_transformation import DataTransformation

        transform = DataTransformation()
        transform.run()  # pipeline completo: CSV → UTF-8 → Parquet

        # O consultas sobre el parquet ya generado:
        lf = transform.cargar_parquet()
        df = transform.consultar_pyg(lf, filtros={"Ano": 2023}, ...)
    """

    # Patrones para normalizar tildes
    _PATRONES = ['á', 'é', 'í', 'ó', 'ú', 'Á', 'É', 'Í', 'Ó', 'Ú', 'ü', 'Ü', 'ñ', 'Ñ', '�']
    _REEMPLAZOS = ['a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U', 'u', 'U', 'n', 'N', '']

    # Schema overrides para el CSV
    _SCHEMA_OVERRIDES = {
        "VARIACIONES": pl.String,
        "Negocio": pl.String,
        "Porc_Distribucion_T1_SUM": pl.Float64,
        "Porc_Distribucion_T2_SUM": pl.Float64,
        "Porc_Distribucion_T3_SUM": pl.Float64,
        "Porc_Distribucion_T4_SUM": pl.Float64,
        "Objeto": pl.String,
        "Cia": pl.String,
    }

    def __init__(
        self,
        ruta_csv: str | None = None,
        ruta_csv_utf8: str | None = None,
        ruta_parquet: str | None = None,
    ) -> None:
        """
        Args:
            ruta_csv:      Ruta del CSV original (Latin-1).
            ruta_csv_utf8: Ruta del CSV convertido a UTF-8.
            ruta_parquet:  Ruta del archivo Parquet de salida.
        """
        datasets = _PROJECT_ROOT / "datasets"

        self._ruta_csv = Path(ruta_csv) if ruta_csv else datasets / "csv" / "PyG Anonimizado.csv"
        self._ruta_csv_utf8 = Path(ruta_csv_utf8) if ruta_csv_utf8 else datasets / "csv" / "PyG Anonimizado UTF8.csv"
        self._ruta_parquet = Path(ruta_parquet) if ruta_parquet else datasets / "clean" / "PyG Anonimizado.parquet"

        # Crear carpetas si no existen
        self._ruta_parquet.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Metodos de transformacion
    # ------------------------------------------------------------------
    def convertir_csv_a_utf8(self, chunk_size: int = 1024 * 1024) -> str:
        """
        Convierte el CSV de Latin-1 a UTF-8 leyendo en chunks.

        Args:
            chunk_size: Tamano del chunk en bytes (default 1MB).

        Returns:
            Ruta del archivo UTF-8 generado.
        """
        print(f"  Convirtiendo CSV a UTF-8...")
        print(f"    Origen:  {self._ruta_csv}")
        print(f"    Destino: {self._ruta_csv_utf8}")

        with open(self._ruta_csv, encoding="latin1") as f_in:
            with open(self._ruta_csv_utf8, encoding="utf8", mode="w") as f_out:
                while True:
                    chunk = f_in.read(chunk_size)
                    if not chunk:
                        break
                    f_out.write(chunk)

        print("  Conversion a UTF-8 completada.")
        return str(self._ruta_csv_utf8)

    def normalizar_tildes(self, lazy_frame: pl.LazyFrame) -> pl.LazyFrame:
        """
        Elimina tildes y caracteres especiales de todas las columnas String.

        Args:
            lazy_frame: LazyFrame a normalizar.

        Returns:
            LazyFrame con tildes normalizadas.
        """
        return lazy_frame.with_columns(
            pl.col(pl.String).str.replace_many(self._PATRONES, self._REEMPLAZOS)
        )

    def normalizar_nombres_columnas(self, lazy_frame: pl.LazyFrame) -> pl.LazyFrame:
        """
        Normaliza los nombres de columnas: espacios por guiones bajos.

        Args:
            lazy_frame: LazyFrame a normalizar.

        Returns:
            LazyFrame con nombres de columnas normalizados.
        """
        nuevos_nombres = {col: col.replace(" ", "_") for col in lazy_frame.collect_schema().names()}
        return lazy_frame.rename(nuevos_nombres)

    def generar_parquet(self, forzar: bool = False) -> str:
        """
        Genera el archivo Parquet a partir del CSV.

        Args:
            forzar: Si True, regenera aunque ya exista.

        Returns:
            Ruta del archivo Parquet generado.
        """
        if self._ruta_parquet.exists() and not forzar:
            print(f"  Parquet ya existe: {self._ruta_parquet}")
            return str(self._ruta_parquet)

        print("  Generando archivo Parquet...")

        # Convertir a UTF-8 si no existe
        if not self._ruta_csv_utf8.exists():
            self.convertir_csv_a_utf8()

        # Leer CSV y aplicar transformaciones
        lf = pl.scan_csv(
            str(self._ruta_csv_utf8),
            encoding="utf8",
            separator="|",
            schema_overrides=self._SCHEMA_OVERRIDES,
            null_values=["NULL", "null", ""],
            infer_schema_length=10000,
        )
        lf = self.normalizar_tildes(lf)
        lf = self.normalizar_nombres_columnas(lf)

        # Guardar como Parquet
        lf.sink_parquet(str(self._ruta_parquet), compression="zstd")

        print(f"  Parquet generado: {self._ruta_parquet}")

        return str(self._ruta_parquet)

    def cargar_parquet(self) -> pl.LazyFrame:
        """
        Carga el archivo Parquet como LazyFrame.

        Returns:
            LazyFrame del archivo Parquet.
        """
        if not self._ruta_parquet.exists():
            raise FileNotFoundError(
                f"No se encontro el archivo Parquet: {self._ruta_parquet}. "
                "Ejecuta generar_parquet() primero."
            )
        return pl.scan_parquet(str(self._ruta_parquet))

    def consultar_pyg(
        self,
        lazy_frame: pl.LazyFrame,
        filtros: dict = None,
        agrupar_por: list[str] = None,
        incluir_trimestres: bool = True,
        incluir_total_anual: bool = True,
        formato_largo: bool = False,
    ) -> pl.DataFrame:
        """
        Consulta generica para el archivo PyG.

        Args:
            lazy_frame:         LazyFrame del archivo parquet.
            filtros:            Diccionario {columna: valor} - equivalente al WHERE.
                                valor puede ser:
                                  - un escalar        → igualdad  (col == valor)
                                  - una tupla (op, v) → operador  (col op v)
                                    operadores soportados: "!=", ">", "<", ">=", "<="
                                Ejemplos:
                                  {"Ano": 2019}                         → Ano == 2019
                                  {"Nombre_Cia": ("!=", "PRESUPUESTOS")} → Nombre_Cia != "PRESUPUESTOS"
            agrupar_por:        Lista de columnas para agrupar - equivalente al GROUP BY.
            incluir_trimestres: Si incluye las columnas de trimestres.
            incluir_total_anual: Si incluye la columna TOTAL_ANUAL.
            formato_largo:      Si True, convierte las columnas de meses a filas (MES, TOTAL_MES).

        Returns:
            DataFrame con los resultados de la consulta.
        """
        # --- Operadores soportados ---
        _operadores = {
            "!=": lambda c, v: c != v,
            ">": lambda c, v: c > v,
            "<": lambda c, v: c < v,
            ">=": lambda c, v: c >= v,
            "<=": lambda c, v: c <= v,
        }

        # --- WHERE: aplicar filtros si existen ---
        if filtros:
            condiciones = []
            for col, val in filtros.items():
                if isinstance(val, tuple):
                    op, valor = val
                    condiciones.append(_operadores[op](pl.col(col), valor))
                else:
                    condiciones.append(pl.col(col) == val)
            filtro_combinado = reduce(lambda a, b: a & b, condiciones)
            query = lazy_frame.filter(filtro_combinado)
        else:
            query = lazy_frame

        # --- GROUP BY ---
        group_by_cols = agrupar_por if agrupar_por else []

        # --- Agregaciones base: meses ---
        agregaciones = [
            pl.col("Vr_Ene_SUM").sum().abs().alias("ENERO"),
            pl.col("Vr_Feb_SUM").sum().abs().alias("FEBRERO"),
            pl.col("Vr_Mar_SUM").sum().abs().alias("MARZO"),
            pl.col("Vr_Abr_SUM").sum().abs().alias("ABRIL"),
            pl.col("Vr_May_SUM").sum().abs().alias("MAYO"),
            pl.col("Vr_Jun_SUM").sum().abs().alias("JUNIO"),
            pl.col("Vr_Jul_SUM").sum().abs().alias("JULIO"),
            pl.col("Vr_Ago_SUM").sum().abs().alias("AGOSTO"),
            pl.col("Vr_Sep_SUM").sum().abs().alias("SEPTIEMBRE"),
            pl.col("Vr_Oct_SUM").sum().abs().alias("OCTUBRE"),
            pl.col("Vr_Nov_SUM").sum().abs().alias("NOVIEMBRE"),
            pl.col("Vr_Dic_SUM").sum().abs().alias("DICIEMBRE"),
        ]

        # --- Trimestres (opcional) ---
        if incluir_trimestres:
            agregaciones += [
                pl.col("Vr_T1_SUM").sum().abs().alias("TRIMESTRE_1"),
                pl.col("Vr_T2_SUM").sum().abs().alias("TRIMESTRE_2"),
                pl.col("Vr_T3_SUM").sum().abs().alias("TRIMESTRE_3"),
                pl.col("Vr_T4_SUM").sum().abs().alias("TRIMESTRE_4"),
            ]

        # --- Total anual (opcional) ---
        if incluir_total_anual:
            agregaciones.append(
                (
                    pl.col("Vr_Ene_SUM") + pl.col("Vr_Feb_SUM") + pl.col("Vr_Mar_SUM") +
                    pl.col("Vr_Abr_SUM") + pl.col("Vr_May_SUM") + pl.col("Vr_Jun_SUM") +
                    pl.col("Vr_Jul_SUM") + pl.col("Vr_Ago_SUM") + pl.col("Vr_Sep_SUM") +
                    pl.col("Vr_Oct_SUM") + pl.col("Vr_Nov_SUM") + pl.col("Vr_Dic_SUM")
                ).sum().abs().alias("TOTAL_ANUAL")
            )

        # --- Ejecutar consulta ---
        if group_by_cols:
            df = (
                query
                .group_by(group_by_cols)
                .agg(agregaciones)
                .sort(group_by_cols)
                .collect(engine="streaming")
            )
        else:
            df = (
                query
                .select(agregaciones)
                .collect(engine="streaming")
            )

        # --- Formato largo: unpivot de columnas de meses a filas ---
        if formato_largo:
            cols_valor = [
                "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
                "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
            ]
            if incluir_trimestres:
                cols_valor += ["TRIMESTRE_1", "TRIMESTRE_2", "TRIMESTRE_3", "TRIMESTRE_4"]
            if incluir_total_anual:
                cols_valor += ["TOTAL_ANUAL"]

            cols_indice = [c for c in group_by_cols if c in df.columns]
            df = df.unpivot(
                on=cols_valor,
                index=cols_indice,
                variable_name="MES",
                value_name="TOTAL_MES",
            )

        return df

    # ------------------------------------------------------------------
    # Pipeline completo
    # ------------------------------------------------------------------
    def run(self, forzar: bool = False) -> str:
        """
        Ejecuta el pipeline completo de transformacion.

        Args:
            forzar: Si True, regenera el Parquet aunque ya exista.

        Returns:
            Ruta del archivo Parquet generado.
        """
        print("=" * 60)
        print("  DATA TRANSFORMATION - Inicio")
        print("=" * 60)

        resultado = self.generar_parquet(forzar=forzar)

        # Mostrar estadisticas
        lf = self.cargar_parquet()
        total = lf.select(pl.len().alias("total")).collect()
        print(f"  Total de registros: {total['total'][0]:,}")

        print("=" * 60)
        print("  DATA TRANSFORMATION - completado")
        print("=" * 60)

        return resultado

