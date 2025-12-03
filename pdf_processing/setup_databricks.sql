-- ============================================================================
-- Script de Configuración para Databricks
-- ============================================================================
-- Este script crea los Volumes y Tablas necesarios para el pipeline de PDFs
-- Ejecutar en un notebook de Databricks SQL o en la consola SQL
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Crear Schema/Catalog (si no existe)
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS logistics;
CREATE SCHEMA IF NOT EXISTS logistics.bronze;

-- ----------------------------------------------------------------------------
-- 2. Crear Volumes para almacenar PDFs y TXTs
-- ----------------------------------------------------------------------------
-- Nota: Los Volumes se crean desde la UI o usando la API
-- Catalog → Volumes → Create Volume
-- 
-- Volumes necesarios:
-- - logistics.bronze.raw (para PDFs y TXTs)
-- 
-- Estructura esperada:
-- /Volumes/logistics/bronze/raw/pdf/source={source_name}/
-- /Volumes/logistics/bronze/raw/txt/source={source_name}/

-- ----------------------------------------------------------------------------
-- 3. Crear Tabla Delta para cargas (loads)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS logistics.bronze.truckr_loads (
  -- Metadata
  source_file STRING COMMENT "Ruta del archivo fuente",
  processed_at STRING COMMENT "Timestamp de procesamiento",
  
  -- Broker Information
  broker_name STRING,
  broker_phone STRING,
  broker_fax STRING,
  broker_address STRING,
  broker_city STRING,
  broker_state STRING,
  broker_zipcode STRING,
  broker_email STRING,
  
  -- Load Information
  loadConfirmationNumber STRING COMMENT "Número de confirmación de carga",
  totalCarrierPay STRING COMMENT "Pago total al transportista",
  
  -- Carrier Information
  carrier_name STRING,
  carrier_mc STRING COMMENT "MC Number del transportista",
  carrier_address STRING,
  carrier_city STRING,
  carrier_state STRING,
  carrier_zipcode STRING,
  carrier_phone STRING,
  carrier_fax STRING,
  carrier_contact STRING,
  
  -- Pickup Information (hasta 3 pickups)
  pickup_customer_1 STRING,
  pickup_address_1 STRING,
  pickup_city_1 STRING,
  pickup_state_1 STRING,
  pickup_zipcode_1 STRING,
  pickup_start_datetime_1 STRING,
  pickup_end_datetime_1 STRING,
  
  pickup_customer_2 STRING,
  pickup_address_2 STRING,
  pickup_city_2 STRING,
  pickup_state_2 STRING,
  pickup_zipcode_2 STRING,
  pickup_start_datetime_2 STRING,
  pickup_end_datetime_2 STRING,
  
  pickup_customer_3 STRING,
  pickup_address_3 STRING,
  pickup_city_3 STRING,
  pickup_state_3 STRING,
  pickup_zipcode_3 STRING,
  pickup_start_datetime_3 STRING,
  pickup_end_datetime_3 STRING,
  
  -- Delivery Information (hasta 3 deliveries)
  delivery_customer_1 STRING,
  delivery_address_1 STRING,
  delivery_city_1 STRING,
  delivery_state_1 STRING,
  delivery_zipcode_1 STRING,
  delivery_start_datetime_1 STRING,
  delivery_end_datetime_1 STRING,
  
  delivery_customer_2 STRING,
  delivery_address_2 STRING,
  delivery_city_2 STRING,
  delivery_state_2 STRING,
  delivery_zipcode_2 STRING,
  delivery_start_datetime_2 STRING,
  delivery_end_datetime_2 STRING,
  
  delivery_customer_3 STRING,
  delivery_address_3 STRING,
  delivery_city_3 STRING,
  delivery_state_3 STRING,
  delivery_zipcode_3 STRING,
  delivery_start_datetime_3 STRING,
  delivery_end_datetime_3 STRING
) 
USING DELTA
LOCATION 'dbfs:/mnt/logistics/bronze/truckr_loads'
TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);

-- ----------------------------------------------------------------------------
-- 4. Verificar creación
-- ----------------------------------------------------------------------------
SHOW TABLES IN logistics.bronze;

-- Ver estructura de la tabla
DESCRIBE EXTENDED logistics.bronze.truckr_loads;

-- ----------------------------------------------------------------------------
-- 5. (Opcional) Crear índices o particiones si es necesario
-- ----------------------------------------------------------------------------
-- ALTER TABLE logistics.bronze.truckr_loads 
-- PARTITION BY (broker_state);

-- ============================================================================
-- NOTAS IMPORTANTES:
-- ============================================================================
-- 1. Los Volumes deben crearse desde la UI de Databricks:
--    Catalog → Volumes → Create Volume
--
-- 2. Ajustar la LOCATION según tu configuración de almacenamiento
--
-- 3. Verificar permisos de acceso a los Volumes y Tablas
--
-- 4. El job espera que los PDFs estén en:
--    /Volumes/logistics/bronze/raw/pdf/source={source_name}/
--    donde {source_name} puede ser UTB, CY, etc.
-- ============================================================================

