import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional

from pyspark.sql import SparkSession, functions as F, types as T

# ------------------------------------------------------------------------------
# 🪵 Logger Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("UTB_Validator")

# ------------------------------------------------------------------------------
# 0️⃣ Truth record embebido por defecto (puedes pasar --truth_json para usar otro)
# ------------------------------------------------------------------------------
DEFAULT_TRUTH = {
    "source_file": "dbfs:/Volumes/logistics/bronze/raw/txt/source=UTB/1637351047899_UTB%20ME-FL-FL.txt",
    "broker_name": "USA Truck Brokers Inc.",
    "broker_phone": "305-819-3000",
    "broker_fax": "305-819-7146",
    "broker_address": "14750 NW 77 Court Suite 200",
    "broker_city": "Miami Lakes",
    "broker_state": "FL",
    "broker_zipcode": "33016",
    "broker_email": "accounting@usatruckbrokers.com; talzate@usatruckbrokers.com",
    "loadConfirmationNumber": "301238",
    "totalCarrierPay": "3800.00",
    "carrier_name": "GTT FREIGHT CORP LLC",
    "carrier_mc": "1311415",
    "carrier_address": "120 9th st #1126",
    "carrier_city": "SAN ANTONIO",
    "carrier_state": "TX",
    "carrier_zipcode": "78215",
    "carrier_phone": "786-796-0858",
    "carrier_fax": "",
    "carrier_contact": "Alejandro Arboleda (dispatcher",
    "pickup_customer_1": "DINGLEY PRESS LEWISTON",
    "pickup_address_1": "40 WESTMINSTER ST",
    "pickup_city_1": "LEWISTON",
    "pickup_state_1": "ME",
    "pickup_zipcode_1": "04240",
    "pickup_start_datetime_1": "2021-11-19T08:00:00",
    "pickup_end_datetime_1": "2021-11-19T23:00:00",
    "delivery_customer_1": "YBOR",
    "delivery_customer_2": "WEST PALM BEACH P&DC",
    "delivery_address_1": "1801 GRANT ST",
    "delivery_address_2": "3200 Summit Blvd",
    "delivery_city_1": "TAMPA",
    "delivery_city_2": "WEST PALM BEACH",
    "delivery_state_1": "FL",
    "delivery_state_2": "FL",
    "delivery_zipcode_1": "33605",
    "delivery_zipcode_2": "33406",
    "delivery_start_datetime_1": "2021-11-21T12:00:00",
    "delivery_start_datetime_2": "2021-11-21T19:00:00",
    "delivery_end_datetime_1": "2021-11-21T12:00:00",
    "delivery_end_datetime_2": "2021-11-21T19:00:00",
}

# ------------------------------------------------------------------------------
# Utilidades de normalización y comparación
# ------------------------------------------------------------------------------


def normalize_row_dict(row: Dict[str, Any]) -> Dict[str, Optional[str]]:
    """
    Normaliza un diccionario de fila: strip y lower en cadenas; None si valor ausente.
    Mantiene tipos simples convertidos a str si es necesario.
    """
    out = {}
    for k, v in row.items():
        if v is None:
            out[k] = None
            continue
        # Guardamos como string para la comparación, pero trim + lower
        if isinstance(v, str):
            s = v.strip()
            out[k] = s.lower() if s != "" else ""
        else:
            # Para otros tipos (int/float), convertir a str y comparar
            out[k] = str(v).strip()
    return out


def compare_values(key: str, truth: Optional[str], target: Optional[str]) -> bool:
    """
    Comparador tolerante:
    - None/'' tratados como equivalentes si ambos vacíos/None.
    - Si parece una lista de emails (contiene '@' o ';'), compara como sets.
    - Si es claramente numérico (solo dígitos y . , $), normaliza y compara numéricamente.
    - Por defecto compara strings normalizados.
    """
    # Normalizar None/empty
    t1 = truth if truth is not None else ""
    t2 = target if target is not None else ""

    # Ambos vacíos -> match
    if t1 == "" and t2 == "":
        return True

    # Heurística de emails: si cualquiera contiene '@' o ';' y ambos no vacíos
    if ("@" in t1 or "@" in t2 or ";" in t1 or ";" in t2):
        # Separar por ';' o ',' y limpiar
        def emails_to_set(s: str):
            parts = [p.strip().lower() for p in s.replace(",", ";").split(";") if p.strip()]
            return set(parts)

        set1 = emails_to_set(t1)
        set2 = emails_to_set(t2)
        return set1 == set2

    # Heurística numérica: quitar símbolos comunes y comparar como float si es posible
    def normalize_number(s: str):
        import re
        s_clean = re.sub(r"[^\d\.\-]", "", s)  # dejar dígitos, punto y signo negativo
        try:
            return float(s_clean)
        except Exception:
            return None

    n1 = normalize_number(t1)
    n2 = normalize_number(t2)
    if n1 is not None and n2 is not None:
        return abs(n1 - n2) < 1e-6

    # Fallback simple: igualdad de strings ya normalizados
    return t1 == t2


# ------------------------------------------------------------------------------
# Lógica principal
# ------------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="UTB Extractor Validator")
    parser.add_argument(
        "--source_table",
        required=True,
        help="Spark table name for validation (ej: logistics.bronze.truckr_loads)"
    )
    parser.add_argument(
        "--load_id",
        required=False,
        help="loadConfirmationNumber a validar (si no se pasa, se usa el del truth embebido)"
    )
    parser.add_argument(
        "--truth_json",
        required=False,
        help="(opcional) ruta a un JSON con el registro truth. Si se pasa, reemplaza el truth embebido."
    )
    args = parser.parse_args(argv)

    source_table = args.source_table

    # Crear SparkSession
    spark = SparkSession.builder.appName("UTB_Extractor_Validator").getOrCreate()

    # Cargar truth
    truth_record: Dict[str, Any] = DEFAULT_TRUTH.copy()
    if args.truth_json:
        logger.info(f"Cargando truth desde {args.truth_json}")
        try:
            with open(args.truth_json, "r", encoding="utf-8") as fh:
                loaded = json.load(fh)
                if not isinstance(loaded, dict):
                    logger.error("El JSON de truth debe contener un objeto/dict en la raíz.")
                    raise SystemExit(2)
                truth_record.update(loaded)
        except FileNotFoundError:
            logger.error(f"No se encontró el archivo {args.truth_json}")
            raise SystemExit(2)

    # Si se pasa load_id por argumento, lo usamos
    if args.load_id:
        truth_record["loadConfirmationNumber"] = args.load_id

    load_id = truth_record.get("loadConfirmationNumber")
    if not load_id:
        logger.error("No se especificó loadConfirmationNumber en truth ni en argumentos.")
        raise SystemExit(2)

    # Intentar cargar la tabla
    try:
        target_df = spark.table(source_table)
    except Exception as e:
        logger.error(f"No se pudo leer la tabla {source_table}: {e}")
        raise SystemExit(2)

    # Normalizar solo columnas string
    string_cols = [c for c, t in target_df.dtypes if t == "string"]
    logger.info(f"Columnas string detectadas: {string_cols}")

    def normalize_df(df):
        return df.select(*[
            F.trim(F.lower(F.col(c))).alias(c) if c in string_cols else F.col(c)
            for c in df.columns
        ])

    target_df = normalize_df(target_df)

    # Filtrar por loadConfirmationNumber (si columna existe)
    if "loadConfirmationNumber" not in target_df.columns:
        logger.error("La tabla objetivo no contiene la columna 'loadConfirmationNumber'.")
        raise SystemExit(2)

    matches_df = target_df.filter(F.col("loadConfirmationNumber") == load_id)

    total_matches = matches_df.count()
    if total_matches == 0:
        logger.error(f"No se encontró ningún registro para loadConfirmationNumber={load_id}")
        # Mostrar esperados por campo
        for k, v in truth_record.items():
            logger.error(f"Esperado {k} = '{v}'")
        raise SystemExit(1)
    elif total_matches > 1:
        logger.warning(f"Se encontraron {total_matches} filas para loadConfirmationNumber={load_id}. Se comparará la primera y se listará el conteo.")
    else:
        logger.info(f"Encontrado 1 registro para loadConfirmationNumber={load_id}")

    # Tomar la primera fila (limit(1).collect())
    first_row = matches_df.limit(1).collect()[0].asDict()

    # Normalizar diccionarios para comparación
    norm_truth = normalize_row_dict(truth_record)
    norm_target = normalize_row_dict(first_row)

    # Usar campos base en truth para iterar (asegura que comparamos lo esperado)
    fields = list(norm_truth.keys())

    results = []
    mismatches = []

    for col in fields:
        truth_val = norm_truth.get(col)
        target_val = norm_target.get(col)
        ok = compare_values(col, truth_val, target_val)
        status = "✅ Match" if ok else "❌ Mismatch"
        results.append((col, status, truth_record.get(col), first_row.get(col)))
        if not ok:
            mismatches.append((col, truth_record.get(col), first_row.get(col)))

    # Logging de resultados
    logger.info(f"Resultados de validación para loadConfirmationNumber={load_id}:")
    for field, status, truth_raw, target_raw in results:
        if status.startswith("✅"):
            logger.info(f"{field:30} | {status:10} | truth='{truth_raw}' | target='{target_raw}'")
        else:
            logger.error(f"{field:30} | {status:10} | truth='{truth_raw}' | target='{target_raw}'")

    # Si hay errores, fallar (código 1)
    if mismatches:
        logger.error(f"Validation failed: {len(mismatches)} campos no coinciden")
        for fld, exp, got in mismatches:
            logger.error(f"  - {fld}: expected='{exp}' got='{got}'")
        raise SystemExit(1)
    else:
        logger.info("✅ Todas las columnas coinciden (según reglas de comparación).")
        raise SystemExit(0)


if __name__ == "__main__":
    main()
