# 📊 Pipeline ETL — Estado de Pérdidas y Ganancias (PyG)

**Maestría en Ingeniería — Universidad Autónoma de Occidente**  
**Asignatura:** ETL — Semestre 1  
**Período de datos:** 2019 – 2025  
**Entrega Final:** Solución ETL Completa

---

## 📋 Tabla de Contenidos

1. [Contexto del Proyecto](#1-contexto-del-proyecto)
2. [Descripción de los Datos](#2-descripción-de-los-datos)
3. [Proceso ETL](#3-proceso-etl)
4. [Resultados Obtenidos](#4-resultados-obtenidos)
   - [Visualización en Power BI](#45-visualización-en-power-bi)
5. [Reflexiones Finales](#5-reflexiones-finales)
6. [Tecnologías Utilizadas](#6-tecnologías-utilizadas)
7. [Estructura del Proyecto](#7-estructura-del-proyecto)
8. [Cómo Ejecutar](#8-cómo-ejecutar)

---

## 1. Contexto del Proyecto

### 1.1 Descripción de la Empresa

Empresa líder en la fabricación y comercialización de medicamentos y suplementos alimenticios
para Pokémones en el Valle del Cauca, ha mantenido un crecimiento rentable en los últimos años. Sin
embargo, la entrada de nuevos competidores al mercado y la necesidad de mantener su liderazgo
exigen una revisión de su eficiencia operativa.

### 1.2 Definición del Problema

Actualmente, existe una brecha de información en la data financiera de las unidades de negocio,
debido a que esta se gestiona en un sistema independiente del sistema de información de las plantas
de producción. Esta desarticulación dificulta la correlación entre el deterioro de los costos y las
actividades específicas de fabricación, lo que limita la capacidad de la gerencia para responder
oportunamente mediante la implementación de mejoras tecnológicas o iniciativas de ingeniería de
procesos orientadas a la recuperación de los márgenes de rentabilidad.

### 1.3 Pregunta Analítica Central

> *¿Cómo ha evolucionado la participación del costo total sobre las ventas totales mes a mes, por negocio y línea, excluyendo datos presupuestarios, en el período 2019–2025?*

### 1.4 Justificación

La implementación de una solución de Inteligencia de Negocios (BI) se justifica por la necesidad de:

- **Visibilidad 360°:** Unificar las fuentes para entender no solo cuánto se gasta, sino en qué actividad específica de la planta se está generando un uso ineficiente de los recursos.
- **Competitividad:** Reaccionar ante nuevos competidores mediante la reducción de costos y la optimización de procesos.
- **Toma de Decisiones Basada en Datos:** Sustituir las suposiciones por información de valor que permita inversiones precisas en nuevas tecnologías.

### 1.5 Alternativa de Integración

**Data Warehouse Centralizado:** Extraer datos del sistema financiero (ERP) hacia un repositorio único (SQLite) para generar tableros de control unificados en Power BI.

---

## 2. Descripción de los Datos

### 2.1 Fuente y Formato Original

| Atributo               | Valor                                                   |
|------------------------|---------------------------------------------------------|
| **Archivo fuente**     | `PyG Sigma Consolidado Hist 2019-2025 COP.xlsb`        |
| **Formato**            | Excel binario (`.xlsb`) → convertido manualmente a CSV  |
| **Encoding del CSV**   | Latin-1 (ISO 8859-1)                                    |
| **Separador**          | Pipe `\|`                                                |
| **Período cubierto**   | 2019 – 2025                                             |
| **Moneda**             | COP (Pesos Colombianos)                                 |
| **Sistema de origen**  | ERP Sigma (Sistema Financiero)                          |

### 2.2 Estructura del Dataset

| Columna                            | Tipo     | Descripción                                              |
|------------------------------------|----------|----------------------------------------------------------|
| `Ano`                              | Integer  | Año fiscal                                               |
| `Negocio`                          | String   | Código de unidad de negocio (ej. "0001", "0007")         |
| `Linea`                            | String   | Código de línea de producto (ej. "003", "074")           |
| `Cia`                              | String   | Código de compañía                                       |
| `Nombre_Cia`                       | String   | Nombre de la compañía (incluye "PRESUPUESTOS")           |
| `Pais_Territorio_Negocio`          | String   | País o territorio (ej. "COLOMBIA")                       |
| `Objeto`                           | String   | Código objeto contable                                   |
| `Nivel1` … `Nivel9`               | String   | Jerarquía contable del PyG                               |
| `Vr_Ene_SUM` … `Vr_Dic_SUM`      | Float64  | Valores mensuales (Enero a Diciembre)                    |
| `Vr_T1_SUM` … `Vr_T4_SUM`        | Float64  | Valores trimestrales                                     |
| `Porc_Distribucion_T1_SUM` … `T4` | Float64  | Porcentajes de distribución trimestral                   |
| `VARIACIONES`                      | String   | Variaciones entre períodos                               |

### 2.3 Problemas de Calidad Identificados

| #  | Problema                                      | Impacto                                                     | Estado      |
|----|-----------------------------------------------|-------------------------------------------------------------|-------------|
| 1  | Encoding Latin-1 incompatible con UTF-8       | Caracteres corruptos al procesar con herramientas modernas  | ✅ Resuelto |
| 2  | Tildes y diacríticos en columnas de texto     | Dificultan filtros, agrupaciones y joins                    | ✅ Resuelto |
| 3  | Caracteres corruptos (`�`)                    | Datos ilegibles en campos de texto                          | ✅ Resuelto |
| 4  | Tipos de datos inferidos incorrectamente      | Códigos como "0007" se interpretan como entero 7            | ✅ Resuelto |
| 5  | Datos presupuestarios mezclados con reales    | Distorsionan análisis de ejecución real                     | ✅ Resuelto |
| 6  | Formato ancho (12 columnas = 12 meses)        | Inadecuado para visualización en herramientas BI            | ✅ Resuelto |

---

## 3. Proceso ETL

### 3.1 Arquitectura General

```
                                    ╔═══════════════════════════════════════════════════════════════╗
                                    ║                     E X T R A C T I O N                       ║
                                    ╚═══════════════════════════════════════════════════════════════╝

 ┌────────────────┐                 ┌────────────────┐                 ┌────────────────┐
 │  ERP Sigma     │  Exportación    │  Servidor Web  │   download()    │  .zip.enc      │
 │  (.xlsb)       │ ──────────────> │  (URL remota)  │ ──────────────> │  (encriptado)  │
 └────────────────┘    manual       └────────────────┘   HTTP GET      └───────┬────────┘
                                                                               │ decrypt()
                                                                               │ Fernet (AES-128)
                                                                               ▼
                                                                       ┌────────────────┐
                                                                       │  .zip          │
                                                                       │  (desencript.) │
                                                                       └───────┬────────┘
                                                                               │ unzip()
                                                                               ▼
                                    ╔═══════════════════════════════════════════════════════════════╗
                                    ║                   T R A N S F O R M A T I O N                 ║
                                    ╚═══════════════════════════════════════════════════════════════╝

 ┌────────────────┐   convertir_csv_a_utf8()   ┌────────────────┐
 │  CSV Latin-1   │ ─────────────────────────> │  CSV UTF-8     │
 │  (pipe "|")    │      (chunks 1MB)          │                │
 └────────────────┘                             └───────┬────────┘
                                                        │ pl.scan_csv()
                                                        │ + normalizar_tildes()
                                                        │ + schema_overrides
                                                        ▼
                                                ┌────────────────┐
                                                │  Parquet (zstd)│  ← Capa de staging
                                                │  LazyFrame     │
                                                └───────┬────────┘
                                                        │ consultar_pyg()
                                                        │ (filter + group_by
                                                        │  + agg + unpivot)
                                                        ▼
                                    ╔═══════════════════════════════════════════════════════════════╗
                                    ║                         L O A D I N G                         ║
                                    ╚═══════════════════════════════════════════════════════════════╝

                                          ┌──────────────────────────┐
                                          │  SQLite (pyg.db)         │
                                          │  ├── ventas_totales      │  ← Capa de servicio
                                          │  └── costos_totales      │
                                          └──────────┬───────────────┘
                                                     │ ODBC / importar
                                                     ▼
                                          ┌──────────────────────────┐
                                          │  Power BI                │
                                          │  Dashboards & Reports    │
                                          └──────────────────────────┘
```

### 3.2 Extracción (Extract)

**Clase:** `DataExtraction` (`src/data_extraction.py`)

La extracción se realiza en tres pasos automatizados:

1. **Descarga** del archivo `.zip.enc` desde una URL configurada en variables de entorno.
2. **Desencriptación** del archivo usando Fernet (criptografía simétrica).
3. **Descompresión** del archivo ZIP resultante en la carpeta `datasets/csv`.

```python
from data_extraction import DataExtraction

extraction = DataExtraction()
extraction.run()  # Ejecuta: download() → decrypt() → unzip()
```

**Conversión de encoding** de Latin-1 a UTF-8 mediante lectura por chunks de 1 MB, evitando cargar todo el archivo en memoria:

```python
def convertir_csv_a_utf8(ruta_origen, ruta_destino, chunk_size=1024*1024):
    with open(ruta_origen, encoding="latin1") as f_in:
        with open(ruta_destino, encoding="utf8", mode="w") as f_out:
            while True:
                chunk = f_in.read(chunk_size)
                if not chunk:
                    break
                f_out.write(chunk)
```

### 3.3 Transformación (Transform)

**Clase:** `DataTransformation` (`src/data_transformation.py`)

| #  | Transformación                  | Función / Técnica                      | Justificación                                          |
|----|---------------------------------|----------------------------------------|--------------------------------------------------------|
| 1  | Conversión de encoding          | `convertir_csv_a_utf8()`               | Garantizar compatibilidad UTF-8                        |
| 2  | Normalización de tildes         | `normalizar_tildes()`                  | Eliminar diacríticos para homogeneizar textos          |
| 3  | Forzado de tipos (schema)       | `schema_overrides` en `scan_csv()`     | Evitar inferencia incorrecta (ej. "0007" → String)     |
| 4  | Compresión a Parquet            | `sink_parquet(compression="zstd")`     | Reducir tamaño, acelerar lecturas, formato columnar    |
| 5  | Agregación por dimensiones      | `consultar_pyg()` con `group_by`       | Sumarizar valores mensuales por combinación de dims    |
| 6  | Exclusión de presupuestos       | Filtro `Nombre_Cia != "PRESUPUESTOS"`  | Separar datos reales de proyecciones                   |
| 7  | Unpivot (wide → long)           | `df.unpivot()`                         | 12 columnas de meses → 2 columnas (MES, TOTAL_MES)    |

**Normalización de texto:**

```python
def normalizar_tildes(lazy_frame):
    patrones   = ['á','é','í','ó','ú','Á','É','Í','Ó','Ú','ü','Ü','ñ','Ñ','�']
    reemplazos = ['a','e','i','o','u','A','E','I','O','U','u','U','n','N','']
    return lazy_frame.with_columns(
        pl.col(pl.String).str.replace_many(patrones, reemplazos)
    )
```

**Consulta genérica con filtros dinámicos y unpivot:**

```python
from data_transformation import DataTransformation

transformation = DataTransformation()
transformation.run()

ventas_totales = transformation.consultar_pyg(
    transformation.cargar_parquet(),
    filtros={
        "Nivel1": "739-VENTAS TOTALES",
        "Nombre_Cia": ("!=", "PRESUPUESTOS"),
    },
    agrupar_por=["Ano", "Negocio", "Linea", "Pais_Territorio_Negocio", "Nivel1"],
    incluir_trimestres=False,
    incluir_total_anual=False,
    formato_largo=True,
)
```

### 3.4 Carga (Load)

**Clase:** `DataLoading` (`src/data_loading.py`)

Los datos transformados se insertan en **SQLite** en formato largo, listo para consumo desde Power BI.

**Esquema de las tablas:**

```sql
CREATE TABLE IF NOT EXISTS ventas_totales (
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
```

**Carga de datos:**

```python
from data_loading import DataLoading

loader = DataLoading()
loader.cargar_tabla_largo(costos_totales, "costos_totales")  # Formato largo
loader.cargar_tabla_largo(ventas_totales, "ventas_totales")  # Formato largo
```

---

## 4. Resultados Obtenidos

### 4.1 Métricas del Pipeline

| Métrica                                | Valor                                    |
|----------------------------------------|------------------------------------------|
| **Registros procesados**              | ~500,000+ registros del PyG histórico    |
| **Período cubierto**                  | 2019 – 2025 (7 años)                    |
| **Problemas de calidad resueltos**    | 6 de 6 (100%)                           |
| **Formato de salida**                 | SQLite con tablas en formato largo       |
| **Compresión alcanzada (Parquet)**    | ~70-80% respecto al CSV original         |

### 4.2 Tablas Generadas

| Tabla             | Descripción                                              | Uso Principal                           |
|-------------------|----------------------------------------------------------|----------------------------------------|
| `ventas_totales`  | Ventas totales por año, negocio, línea y mes            | Análisis de ingresos                   |
| `costos_totales`  | Costos totales con desglose por niveles contables       | Análisis de estructura de costos       |

### 4.3 Validaciones Implementadas

| #  | Validación                          | Implementación                                               | Estado          |
|----|-------------------------------------|--------------------------------------------------------------|-----------------|
| 1  | Conteo total de registros           | `pl.len().alias("Cantidad_Total_Registros")`                 | ✅ Implementado |
| 2  | Consistencia de tipos               | `schema_overrides` fuerza tipos al leer CSV                  | ✅ Implementado |
| 3  | Eliminación de caracteres corruptos | `normalizar_tildes()` remueve `�` y normaliza diacríticos    | ✅ Implementado |
| 4  | Filtro por mes específico           | `df.filter(pl.col("MES") == "MARZO")` — verificación visual | ✅ Implementado |
| 5  | Cruce Costo / Ventas               | `(costo / ventas) * 100` — cálculo de participación          | ✅ Implementado |
| 6  | Exclusión de presupuestos           | Filtro `Nombre_Cia != "PRESUPUESTOS"`                        | ✅ Implementado |

### 4.4 Capacidades Analíticas Habilitadas

Con el pipeline ETL implementado, ahora es posible:

- 📈 **Analizar tendencias** de costos vs. ventas mes a mes
- 🏢 **Comparar rendimiento** entre diferentes negocios y líneas de producto
- 🌍 **Segmentar por territorio** (Colombia y otros países)
- 📊 **Visualizar en Power BI** mediante conexión ODBC a SQLite
- 🔄 **Actualizar datos** de forma incremental ejecutando el pipeline

### 4.5 Visualización en Power BI

El archivo `Power BI/Proyecto ETL.pbix` contiene los dashboards interactivos que consumen los datos desde SQLite.

#### Vista General del Dashboard

![Vista General](Power%20BI/Imagenes/01%20-%20Vista%20General.png)

*Dashboard principal con KPIs de ventas, costos y participación por negocio y línea.*

#### Transformaciones de Datos en Power BI

![Transformación Data Interna](Power%20BI/Imagenes/02%20-%20Transformacion%20Data%20Interna.png)

*Vista de las transformaciones aplicadas dentro de Power Query para preparar los datos.*

---

## 5. Reflexiones Finales

### 5.1 Lecciones Aprendidas

1. **Importancia del encoding:** El manejo correcto del encoding (Latin-1 → UTF-8) es fundamental para evitar pérdida de información en caracteres especiales del español.

2. **Evaluación lazy con Polars:** El uso de `LazyFrame` permitió procesar grandes volúmenes de datos sin cargar todo en memoria, mejorando significativamente el rendimiento.

3. **Formato columnar (Parquet):** La conversión a Parquet no solo redujo el tamaño del archivo, sino que aceleró considerablemente las consultas analíticas.

4. **Unpivot para BI:** Transformar de formato ancho a largo fue esencial para la visualización en Power BI, permitiendo filtros dinámicos por mes.

5. **Seguridad de datos:** La implementación de encriptación Fernet para los archivos fuente garantiza la protección de datos sensibles durante la transferencia.

### 5.2 Desafíos Superados

| Desafío                               | Solución Implementada                                    |
|---------------------------------------|----------------------------------------------------------|
| Archivos muy grandes                  | Lectura por chunks y evaluación lazy                     |
| Caracteres corruptos                  | Normalización con patrones de reemplazo                  |
| Tipos inferidos incorrectamente       | Schema overrides explícitos                              |
| Formato inadecuado para BI            | Unpivot de columnas de meses a filas                     |
| Mezcla de datos reales y presupuestos | Filtros dinámicos por `Nombre_Cia`                       |

### 5.3 Conclusión

Este proyecto demuestra la implementación exitosa de un pipeline ETL completo que transforma datos financieros crudos del ERP en información analítica lista para la toma de decisiones. La solución aborda los 6 problemas de calidad de datos identificados y habilita el análisis de la evolución de costos vs. ventas por negocio, línea y período, respondiendo directamente a la pregunta analítica planteada.

El uso de tecnologías modernas como **Polars** para procesamiento eficiente, **Parquet** para almacenamiento optimizado y **SQLite** como capa de servicio, permite un flujo de datos escalable y de alto rendimiento hacia **Power BI**.

---


## 6. Tecnologías Utilizadas

| Tecnología         | Versión     | Rol                                  | Justificación                                                   |
|--------------------|-------------|--------------------------------------|-----------------------------------------------------------------|
| **Python**         | 3.13+       | Lenguaje del pipeline ETL            | Ecosistema maduro para procesamiento de datos                   |
| **Polars**         | 1.x         | Procesamiento de datos (LazyFrame)   | Más rápido que Pandas, evaluación lazy, bajo consumo de memoria |
| **Apache Parquet** | -           | Formato de almacenamiento intermedio | Columnar, compresible (zstd), ideal para análisis               |
| **SQLite**         | 3.x         | Base de datos destino                | Ligera, portable, sin servidor, compatible con Power BI (ODBC)  |
| **Fernet**         | cryptography| Encriptación de archivos             | Criptografía simétrica segura para protección de datos          |
| **Power BI**       | Desktop     | Visualización                        | Herramienta estándar de BI para dashboards interactivos         |

---

## 7. Estructura del Proyecto

```
Codigo-Proyecto/
├── datasets/
│   ├── original/                          # Fuente original (ERP) - No versionado
│   ├── csv/
│   │   ├── PyG Anonimizado.csv           # CSV Latin-1 (extraído)
│   │   └── PyG Anonimizado UTF8.csv      # CSV UTF-8 (transformado)
│   └── clean/
│       ├── PyG Anonimizado.parquet        # Staging (Parquet zstd)
│       └── pyg.db                          # SQLite (capa de servicio)
├── Power BI/
│   ├── Proyecto ETL.pbix                  # Dashboard de Power BI
│   └── Imagenes/
│       ├── 01 - Vista General.png         # Captura del dashboard general
│       └── 02 - Transformacion Data Interna.png  # Captura de transformaciones
├── src/
│   ├── __init__.py
│   ├── main.py                            # Pipeline ETL principal
│   ├── data_extraction.py                 # Clase DataExtraction
│   ├── data_transformation.py             # Clase DataTransformation
│   ├── data_loading.py                    # Clase DataLoading
│   └── helpers/
│       ├── __init__.py
│       ├── file_encryptor.py              # Encriptación Fernet
│       └── file_decryptor.py              # Desencriptación Fernet
├── .env.example                            # Ejemplo de variables de entorno
├── requirements.txt                        # Dependencias Python
├── dotenv.whl                              # Paquete dotenv local
└── README.md                               # Esta documentación
```

---

## 8. Cómo Ejecutar

### 8.1 Requisitos Previos

**Python 3.13+** instalado en el sistema.

**Instalar dependencias:**

```bash
pip install -r requirements.txt
```

O manualmente:

```bash
pip install polars cryptography python-dotenv
```

### 8.2 Configuración

Crear archivo `.env` en la raíz del proyecto con las variables necesarias:

```env
DATA_SOURCE_URL=https://[url-del-archivo]/PyG_Anonimizado.zip.enc
FERNET_KEY=[clave-de-encriptacion]
```

### 8.3 Ejecución

```powershell
cd "D:\Maestria UAO\Semestre 1\ETL\Proyecto\Codigo-Proyecto\src"
python main.py
```

### 8.4 Flujo de Ejecución

1. **Extracción:** Descarga, desencripta y descomprime el archivo fuente
2. **Transformación:** Convierte a UTF-8, normaliza, genera Parquet
3. **Carga:** Inserta los datos agregados en SQLite

**Primera ejecución:** Ejecuta todo el pipeline desde cero.

**Ejecuciones posteriores:** Lee directamente del Parquet (si existe) y ejecuta las consultas de ventas y costos, cargando los resultados a SQLite.

---

## 📝 Autores

**Estudiante(s) de Maestría en Ingeniería**  
Miguel Caycedo, Diego Teuta, Jhon Deivi Riascos, Luis Santiago Osorio Ortiz
Universidad Autónoma de Occidente  
Asignatura: ETL — Semestre 1, 2025-2026

---

## 📄 Licencia

Este proyecto es parte del trabajo académico de la Maestría en Ingeniería de la Universidad Autónoma de Occidente. Los datos utilizados han sido anonimizados para proteger información confidencial.

---
