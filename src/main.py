from data_extraction import DataExtraction
from data_transformation import DataTransformation
from data_loading import DataLoading

extraction = DataExtraction()
extraction.run()

transformation = DataTransformation()
transformation.run()

# Creamos los dataframes que tienen los campos largos
costos_totales = transformation.consultar_pyg(
    transformation.cargar_parquet(), {
            "Nivel1": "1123-COSTO TOTAL",
            "Nombre_Cia": ("!=", "PRESUPUESTOS")
        },
        ["Ano", "Negocio", "Linea", "Pais_Territorio_Negocio", "Nivel1", "Nivel2", "Nivel3", "Nivel4", "Nivel9"],
        False,
        False,
        True)

print(costos_totales)

ventas_totales = transformation.consultar_pyg(
    transformation.cargar_parquet(),
            {
                "Nivel1": "739-VENTAS TOTALES",
                "Nombre_Cia": ("!=", "PRESUPUESTOS")
            },
            ["Ano", "Negocio", "Linea", "Pais_Territorio_Negocio", "Nivel1"],
            False,
            False,
            True)

print(ventas_totales)

loader = DataLoading()

loader.cargar_tabla_largo(costos_totales, "costos_totales_largo")   # Formato largo (MES, TOTAL_MES)
loader.cargar_tabla_largo(ventas_totales, "ventas_largo")   # Formato largo (MES, TOTAL_MES)