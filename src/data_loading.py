"""
Clase encargada de la carga de datos a SQLite:
  - Creacion de tablas (formato ancho y largo)
  - Insercion de DataFrames de Polars a SQLite
"""

import sqlite3
from pathlib import Path

import polars as pl

# ── Rutas base del proyecto ──────────────────────────────────────────
_PROJECT_ROOT = Path(__file__).resolve().parents[1]


class DataLoading:
    """
    Carga datos transformados a una base de datos SQLite.

    Uso:
        from data_loading import DataLoading
        from data_transformation import DataTransformation

        transform = DataTransformation()
        lf = transform.cargar_parquet()
        df = transform.consultar_pyg(lf, ...)

        loader = DataLoading()
        loader.cargar_tabla_largo(df, "ventas_largo")
    """

    # ──────────────────────────────────────────────────────────────────
    # Constantes: Columnas y DDL
    # ──────────────────────────────────────────────────────────────────

    # Formato ANCHO (con meses como columnas)
    COLUMNAS_DF = [
        "Ano", "Negocio", "Linea", "Pais_Territorio_Negocio",
        "Nivel1", "Nivel2", "Nivel3", "Nivel4", "Nivel9",
        "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO",
        "JUNIO", "JULIO", "AGOSTO", "SEPTIEMBRE",
        "OCTUBRE", "NOVIEMBRE", "DICIEMBRE",
        "TRIMESTRE_1", "TRIMESTRE_2", "TRIMESTRE_3", "TRIMESTRE_4",
        "TOTAL_ANUAL",
    ]
    COLUMNAS_SQL = [c.lower() for c in COLUMNAS_DF]

    DDL_TABLA = """
    CREATE TABLE IF NOT EXISTS {tabla} (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        ano                     INTEGER  NOT NULL,
        negocio                 TEXT,
        linea                   TEXT,
        pais_territorio_negocio TEXT,
        nivel1                  TEXT,
        nivel2                  TEXT,
        nivel3                  TEXT,
        nivel4                  TEXT,
        nivel9                  TEXT,
        enero                   REAL,
        febrero                 REAL,
        marzo                   REAL,
        abril                   REAL,
        mayo                    REAL,
        junio                   REAL,
        julio                   REAL,
        agosto                  REAL,
        septiembre              REAL,
        octubre                 REAL,
        noviembre               REAL,
        diciembre               REAL,
        trimestre_1             REAL,
        trimestre_2             REAL,
        trimestre_3             REAL,
        trimestre_4             REAL,
        total_anual             REAL
    );
    """

    # Formato LARGO (unpivot: MES / TOTAL_MES)
    COLUMNAS_LARGO = [
        "Ano", "Negocio", "Linea", "Pais_Territorio_Negocio",
        "Nivel1", "Nivel2", "Nivel3", "Nivel4", "Nivel9",
        "MES", "TOTAL_MES",
    ]

    DDL_TABLA_LARGO = """
    CREATE TABLE IF NOT EXISTS {tabla} (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        ano                     INTEGER  NOT NULL,
        negocio                 TEXT,
        linea                   TEXT,
        pais_territorio_negocio TEXT,
        nivel1                  TEXT,
        nivel2                  TEXT,
        nivel3                  TEXT,
        nivel4                  TEXT,
        nivel9                  TEXT,
        mes                     TEXT     NOT NULL,
        total_mes               REAL
    );
    """

    def __init__(self, ruta_db: str | None = None) -> None:
        """
        Args:
            ruta_db: Ruta de la base de datos SQLite.
                     Por defecto: <proyecto>/datasets/clean/pyg.db
        """
        if ruta_db:
            self._ruta_db = Path(ruta_db)
        else:
            self._ruta_db = _PROJECT_ROOT / "datasets" / "clean" / "pyg.db"

        # Crear carpeta si no existe
        self._ruta_db.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Conexion
    # ------------------------------------------------------------------
    def conectar(self) -> sqlite3.Connection:
        """
        Abre una conexion a la base de datos SQLite.

        Returns:
            Conexion SQLite.
        """
        return sqlite3.connect(str(self._ruta_db))

    # ------------------------------------------------------------------
    # Formato ANCHO
    # ------------------------------------------------------------------
    def crear_tabla(self, conn: sqlite3.Connection, tabla: str) -> None:
        """
        Crea una tabla en formato ancho si no existe.

        Args:
            conn:  Conexion SQLite.
            tabla: Nombre de la tabla.
        """
        conn.execute(self.DDL_TABLA.format(tabla=tabla))
        conn.commit()
        print(f"  Tabla '{tabla}' lista.")

    def insertar_df(
        self,
        df: pl.DataFrame,
        tabla: str,
        conn: sqlite3.Connection,
    ) -> int:
        """
        Inserta un DataFrame en formato ancho a SQLite.

        Args:
            df:    DataFrame de Polars.
            tabla: Nombre de la tabla destino.
            conn:  Conexion SQLite.

        Returns:
            Numero de filas insertadas.
        """
        cols_sql = ", ".join(self.COLUMNAS_SQL)
        placeholders = ", ".join(["?" for _ in self.COLUMNAS_SQL])
        sql = f"INSERT INTO {tabla} ({cols_sql}) VALUES ({placeholders})"

        filas = df.select(self.COLUMNAS_DF).rows()
        conn.executemany(sql, filas)
        conn.commit()

        print(f"  {len(filas):,} filas insertadas en '{tabla}'.")
        return len(filas)

    def cargar_tabla(self, df: pl.DataFrame, tabla: str) -> int:
        """
        Crea la tabla (si no existe) e inserta el DataFrame (formato ancho).

        Args:
            df:    DataFrame de Polars.
            tabla: Nombre de la tabla.

        Returns:
            Numero de filas insertadas.
        """
        with self.conectar() as conn:
            self.crear_tabla(conn, tabla)
            return self.insertar_df(df, tabla, conn)

    # ------------------------------------------------------------------
    # Formato LARGO
    # ------------------------------------------------------------------
    def crear_tabla_largo(self, conn: sqlite3.Connection, tabla: str) -> None:
        """
        Crea una tabla en formato largo si no existe.

        Args:
            conn:  Conexion SQLite.
            tabla: Nombre de la tabla.
        """
        conn.execute(self.DDL_TABLA_LARGO.format(tabla=tabla))
        conn.commit()
        print(f"  Tabla '{tabla}' (formato largo) lista.")

    def insertar_df_largo(
        self,
        df: pl.DataFrame,
        tabla: str,
        conn: sqlite3.Connection,
    ) -> int:
        """
        Inserta un DataFrame en formato largo (MES, TOTAL_MES) a SQLite.

        Si el DataFrame no tiene algunas columnas de nivel (ej. Nivel2-9),
        las rellena con None para que el INSERT sea uniforme.

        Args:
            df:    DataFrame de Polars en formato largo.
            tabla: Nombre de la tabla destino.
            conn:  Conexion SQLite.

        Returns:
            Numero de filas insertadas.
        """
        # Agregar columnas faltantes como null
        for col in self.COLUMNAS_LARGO:
            if col not in df.columns:
                df = df.with_columns(pl.lit(None).alias(col))

        cols_sql = ", ".join([c.lower() for c in self.COLUMNAS_LARGO])
        placeholders = ", ".join(["?" for _ in self.COLUMNAS_LARGO])
        sql = f"INSERT INTO {tabla} ({cols_sql}) VALUES ({placeholders})"

        filas = df.select(self.COLUMNAS_LARGO).rows()
        conn.executemany(sql, filas)
        conn.commit()

        print(f"  {len(filas):,} filas insertadas en '{tabla}'.")
        return len(filas)

    def cargar_tabla_largo(self, df: pl.DataFrame, tabla: str) -> int:
        """
        Crea la tabla (si no existe) e inserta el DataFrame (formato largo).

        Args:
            df:    DataFrame de Polars en formato largo (con MES, TOTAL_MES).
            tabla: Nombre de la tabla.

        Returns:
            Numero de filas insertadas.
        """
        with self.conectar() as conn:
            self.crear_tabla_largo(conn, tabla)
            return self.insertar_df_largo(df, tabla, conn)

    # ------------------------------------------------------------------
    # Utilidades
    # ------------------------------------------------------------------
    def ejecutar_query(self, sql: str) -> list:
        """
        Ejecuta una consulta SQL y retorna los resultados.

        Args:
            sql: Consulta SQL a ejecutar.

        Returns:
            Lista de tuplas con los resultados.
        """
        with self.conectar() as conn:
            cursor = conn.execute(sql)
            return cursor.fetchall()

    def listar_tablas(self) -> list[str]:
        """
        Lista todas las tablas en la base de datos.

        Returns:
            Lista de nombres de tablas.
        """
        sql = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        resultados = self.ejecutar_query(sql)
        return [r[0] for r in resultados]

    def contar_registros(self, tabla: str) -> int:
        """
        Cuenta los registros en una tabla.

        Args:
            tabla: Nombre de la tabla.

        Returns:
            Numero de registros.
        """
        sql = f"SELECT COUNT(*) FROM {tabla}"
        resultado = self.ejecutar_query(sql)
        return resultado[0][0]

    def eliminar_tabla(self, tabla: str) -> None:
        """
        Elimina una tabla de la base de datos.

        Args:
            tabla: Nombre de la tabla a eliminar.
        """
        with self.conectar() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {tabla}")
            conn.commit()
            print(f"  Tabla '{tabla}' eliminada.")

    def info(self) -> None:
        """Muestra informacion de la base de datos."""
        print(f"\n  Base de datos: {self._ruta_db}")
        tablas = self.listar_tablas()
        if tablas:
            print(f"  Tablas ({len(tablas)}):")
            for tabla in tablas:
                count = self.contar_registros(tabla)
                print(f"    - {tabla}: {count:,} registros")
        else:
            print("  (sin tablas)")

