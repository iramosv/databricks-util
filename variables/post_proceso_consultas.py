import re
import numpy as np
import pandas as pd
from typing import Iterable, Optional

# --- Module-Level Global Variable ---
# Initialize the DataFrame variable to None or create an empty DataFrame.
# It will hold the loaded metadata table.
DF_METADATA = pd.read_excel("/Workspace/Users/iramosv@dian.gov.co/utils/variables/Variables_22JUL2025.xlsx")

# Heuristic cleaner for a single cell (only applied to strings)
def clean_cell(s: str, keep_leading_zero_ids: bool = True):
    s = s.strip()

    # 1) Special case: exactly "0E-10" -> 0
    if s.upper() == "0E-10":
        return 0

    # 2) Optionally keep IDs like "001234" as text
    if keep_leading_zero_ids and re.fullmatch(r"\d+", s) and len(s) > 1 and s[0] == "0":
        return s  # likely an ID/code

    # 3) Try numeric conversion
    try:
        val = pd.to_numeric(s, errors="raise")
        # Cast floats that are actually integers: 123.00000000000 -> 123
        if isinstance(val, float) and val.is_integer():
            return int(val)
        return val
    except Exception:
        # Not numeric-looking: leave as text
        return s

def clean_df_cell_by_cell(df: pd.DataFrame, keep_leading_zero_ids: bool = True) -> pd.DataFrame:
    """
    Cleans a DataFrame cell by cell, handling null values and converting data
    to a common string format before applying the cleaning logic.
    """
    d = df.copy()

    # Identify columns that are not purely numeric
    cols_to_clean = [col for col in d.columns if d[col].dtype == 'object']

    for col in cols_to_clean:
        # Step 1: Replace any null-like values (None, NaN) with an empty string
        d[col] = d[col].fillna('')

        # Step 2: Now that nulls are handled, safely apply the cleaning logic
        d[col] = d[col].astype(str).apply(lambda x: clean_cell(x, keep_leading_zero_ids))

    return d


def lookup_name_by_year(
    format_id: int,
    var_code: str,
    anogravable: int,
    compose_fields: Iterable[str] = ["Descripción de la Variable",
                                     "Número Casilla",
                                     "Año Gravable Desde",
                                     "Año Gravable Hasta"
                                     ],
    sep: str = "_",
    current_year: Optional[int] = None,
):
    """
    Encuentra el metadato de una variable basándose en su código y un año fiscal específico.

    Esta función busca una única fila coincidente en un DataFrame que contiene
    información de variables fiscales. La coincidencia se basa en un ID de formato,
    un código de variable y un año fiscal dado. Maneja rangos de años y la palabra
    clave 'Vigente' convirtiéndola al año actual.

    Args:
        df1: Un pandas DataFrame que contiene metadatos de variables. Las columnas esperadas
            incluyen: 'Código del Formato', 'Código de la Variable',
            'Año Gravable Desde', 'Año Gravable Hasta', y 'Descripción de la Casilla'.
        format_id: El ID entero del formato fiscal a coincidir (ej. 110, 210).
        var_code: El código de la variable a encontrar en formato de cadena, se espera que sea
            "VAR_####" o "<prefijo>_VAR_####".
        anogravable: El año fiscal específico a buscar.
        compose_fields: Un iterable opcional de nombres de columnas de `df1` para ser
            usadas para crear un nombre compuesto. Los valores de estos campos serán
            añadidos al prefijo de la variable y al número. Por defecto, es una tupla vacía.
        sep: El separador de cadena usado para unir las partes del nombre compuesto.
            Por defecto, es "_".
        current_year: Un entero opcional que representa el año actual. Si no se
            proporciona, la función usará el año actual del sistema.

    Returns:
        Un diccionario que contiene las siguientes claves si se encuentra una coincidencia:
            - 'name_var': El valor de la columna 'Descripción de la Casilla'.
            - 'composed_name': Un nuevo nombre compuesto del prefijo de la variable,
              el número y los valores de `compose_fields`.
            - 'meta': Un diccionario con 'prefix' y 'var_number' para
              uso posterior.
        Devuelve None si no se encuentra ninguna fila coincidente o si `var_code` no
        coincide con el formato esperado.
    """

    if current_year is None:
        current_year = pd.Timestamp.today().year

    # --- extrae el prefijo y el código numérico de var_code (debe terminar con VAR_####)
    # captura: grupo(1)=prefijo (puede estar vacío), grupo(2)=dígitos
    m = re.search(r'^(.*)VAR_(\d+)$', str(var_code).strip(), flags=re.IGNORECASE)
    if not m:
        return None  # patrón no encontrado
    prefix_raw = m.group(1) or ""
    prefix = prefix_raw.rstrip("_")  # elimina el guion bajo final si está presente
    var_number = int(m.group(2))

    d = DF_METADATA.copy()

    # --- normaliza 'Vigente' -> año_actual y convierte a entero que admite nulos
    for col in ['Año Gravable Hasta']:
        mask_vig = d[col].astype(str).str.strip().str.casefold().eq('vigente')
        d.loc[mask_vig, col] = current_year
        d[col] = pd.to_numeric(d[col], errors='coerce').astype('Int64')

    # asegura que 'Año Gravable Desde' también sea numérico
    d['Año Gravable Desde'] = pd.to_numeric(d['Año Gravable Desde'], errors='coerce').astype('Int64')

    # si tu df almacena el código de la variable como número, asegura que sea numérico
    d['Código de la Variable'] = pd.to_numeric(d['Código de la Variable'], errors='coerce').astype('Int64')

    # --- construye la máscara (rango inclusivo; Hasta puede ser NA/sin límite)
    mask = (
        (d['Código del Formato'] == format_id) &
        (d['Código de la Variable'] == var_number) &
        (d['Año Gravable Desde'].notna()) &
        (d['Año Gravable Desde'] <= anogravable) &
        (d['Año Gravable Hasta'].isna() | (anogravable <= d['Año Gravable Hasta']))
    )

    # Filtra el DataFrame usando la máscara construida anteriormente.
    # Si no hay filas que coincidan, retorna None.
    hit = d.loc[mask]
    if hit.empty:
        return None

    # Si hay coincidencias, toma la primera fila encontrada.
    row = hit.iloc[0]

    # tu "NAME_VAR" (ajusta la columna si la nombraste de forma diferente)
    name_var = row.get('Descripción de la Casilla')

    # --- compone un nombre a partir del prefijo + los campos seleccionados

    # For each field in compose_fields, get its value from the matched row.
    # If the value is not NaN, strip whitespace and add it to the parts list.
    parts = []
    for col in compose_fields:
        val = row.get(col)
        if pd.notna(val):
            parts.append(str(val).strip())
    # Join all collected parts with the specified separator to form the composed_name.
    # If no parts were added, composed_name will be None.
    composed_name = sep.join(parts) if parts else None

    return {
        "name_var": name_var,
        "composed_name": composed_name,
        "meta": {
            "prefix": prefix,
            "var_number": var_number,
        }
    }




















    