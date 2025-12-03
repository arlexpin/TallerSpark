# Guía de Despliegue en Databricks

Esta guía explica cómo desplegar el proyecto de procesamiento de PDFs en Databricks.

## 📋 Prerrequisitos

1. **Cuenta de Databricks** con permisos para:
   - Crear Jobs
   - Configurar Git Integration
   - Crear/Modificar Volumes
   - Crear/Modificar Tablas Delta

2. **Repositorio Git** (GitHub, GitLab, etc.)
   - El código debe estar en un repositorio Git
   - Acceso configurado desde Databricks

3. **Dependencias**:
   - PyMuPDF==1.26.4
   - pymupdf4llm==0.0.27
   - PySpark (incluido en Databricks)

## 🗂️ Estructura del Proyecto

El proyecto debe tener la siguiente estructura en el repositorio Git:

```
pdf_processing/
├── src/
│   └── pipelines/
│       └── pdf_processing/
│           ├── pdf_to_text_with_pymupdf4llm.py
│           ├── text_to_columns.py
│           └── text_to_columns_validator.py
└── pdf_processing_git.yml
```

**⚠️ IMPORTANTE**: Los scripts Python deben estar en `src/pipelines/pdf_processing/` según el YAML.

## 📝 Pasos de Despliegue

### Paso 1: Preparar la Estructura del Repositorio

Si tus archivos están en la raíz, necesitas reorganizarlos:

```bash
# Crear la estructura de directorios
mkdir -p src/pipelines/pdf_processing

# Mover los scripts a la ubicación correcta
mv pdf_to_text_with_pymupdf4llm.py src/pipelines/pdf_processing/
mv text_to_columns.py src/pipelines/pdf_processing/
mv text_to_columns_validator.py src/pipelines/pdf_processing/
```

### Paso 2: Configurar Git Integration en Databricks

1. **Ir a Databricks Workspace → Settings → Git Integration**
2. **Configurar tu proveedor Git** (GitHub, GitLab, etc.)
3. **Autenticar** con tu cuenta de Git
4. **Verificar** que puedes acceder al repositorio

### Paso 3: Crear/Configurar Volumes (si no existen)

El job espera que existan Volumes en la ruta:
- `/Volumes/logistics/bronze/raw/pdf/source={source_name}/`
- `/Volumes/logistics/bronze/raw/txt/source={source_name}/`

**Crear Volumes en Databricks SQL/Notebook**:

```python
# Crear el volumen para PDFs
CREATE VOLUME IF NOT EXISTS logistics.bronze.raw_pdf;

# Crear el volumen para TXT
CREATE VOLUME IF NOT EXISTS logistics.bronze.raw_txt;
```

O usar la UI: **Catalog → Volumes → Create Volume**

### Paso 4: Crear la Tabla Delta (si no existe)

El job escribe a `logistics.bronze.truckr_loads`. Crear la tabla:

```sql
CREATE TABLE IF NOT EXISTS logistics.bronze.truckr_loads (
  source_file STRING,
  broker_name STRING,
  broker_phone STRING,
  broker_fax STRING,
  broker_address STRING,
  broker_city STRING,
  broker_state STRING,
  broker_zipcode STRING,
  broker_email STRING,
  loadConfirmationNumber STRING,
  totalCarrierPay STRING,
  carrier_name STRING,
  carrier_mc STRING,
  carrier_address STRING,
  carrier_city STRING,
  carrier_state STRING,
  carrier_zipcode STRING,
  carrier_phone STRING,
  carrier_fax STRING,
  carrier_contact STRING,
  pickup_customer_1 STRING,
  pickup_address_1 STRING,
  pickup_city_1 STRING,
  pickup_state_1 STRING,
  pickup_zipcode_1 STRING,
  pickup_start_datetime_1 STRING,
  pickup_end_datetime_1 STRING,
  delivery_customer_1 STRING,
  delivery_address_1 STRING,
  delivery_city_1 STRING,
  delivery_state_1 STRING,
  delivery_zipcode_1 STRING,
  delivery_start_datetime_1 STRING,
  delivery_end_datetime_1 STRING,
  processed_at STRING
) USING DELTA;
```

### Paso 5: Desplegar el Job usando Databricks CLI

#### Opción A: Usando Databricks CLI (Recomendado)

1. **Instalar Databricks CLI**:
```bash
pip install databricks-cli
```

2. **Autenticar**:
```bash
databricks configure --token
# Ingresa tu Workspace URL y Personal Access Token
```

3. **Desplegar el job**:
```bash
cd pdf_processing
databricks jobs deploy pdf_processing_git.yml
```

#### Opción B: Usando Databricks API

```bash
# Obtener tu token de Databricks
export DATABRICKS_TOKEN="tu_token"
export DATABRICKS_HOST="https://tu-workspace.cloud.databricks.com"

# Desplegar usando la API
curl -X POST \
  "$DATABRICKS_HOST/api/2.1/jobs/create" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \
  -H "Content-Type: application/json" \
  -d @pdf_processing_git.yml
```

#### Opción C: Usando la UI de Databricks

1. **Ir a Workflows → Jobs → Create Job**
2. **Seleccionar "Add configuration from file"**
3. **Subir el archivo `pdf_processing_git.yml`**
4. **Ajustar parámetros si es necesario**:
   - `base_path`: Ruta base de los Volumes
   - `source_name`: Nombre de la fuente (UTB, CY, etc.)
   - `loads_table`: Nombre de la tabla destino

### Paso 6: Configurar el Environment

El job requiere un **Environment** con las dependencias. Crear en Databricks:

1. **Ir a Compute → Environments → Create Environment**
2. **Nombre**: `dev` (o el que uses en el YAML)
3. **Especificar dependencias**:
   ```
   PyMuPDF==1.26.4
   pymupdf4llm==0.0.27
   ```
4. **Guardar**

### Paso 7: Subir PDFs al Volume

Antes de ejecutar el job, subir los PDFs al volumen:

```python
# En un notebook de Databricks
dbutils.fs.cp(
  "file:/tmp/tus_pdfs/",
  "/Volumes/logistics/bronze/raw/pdf/source=UTB/",
  recurse=True
)
```

O usar la UI: **Catalog → Volumes → logistics.bronze.raw → Upload**

### Paso 8: Ejecutar el Job

1. **Ir a Workflows → Jobs → pdf_processing_git**
2. **Click en "Run now"**
3. **Ajustar parámetros si es necesario**:
   - `base_path`: `/Volumes/logistics/bronze/raw`
   - `source_name`: `UTB` (o `CY`)
   - `loads_table`: `logistics.bronze.truckr_loads`
4. **Ejecutar**

## 🔧 Configuración del YAML

### Ajustar Rutas

Si tus rutas son diferentes, edita `pdf_processing_git.yml`:

```yaml
parameters:
  - name: base_path
    default: /Volumes/logistics/bronze/raw  # Ajustar según tu estructura
  - name: source_name
    default: UTB  # Cambiar según la fuente
  - name: loads_table
    default: logistics.bronze.truckr_loads  # Cambiar según tu esquema
```

### Ajustar Repositorio Git

Si tu repo es diferente, edita:

```yaml
git_source:
  git_url: https://github.com/tu-usuario/tu-repo.git
  git_provider: gitHub
  git_branch: dev  # o main, master, etc.
```

## ✅ Verificación

Después del despliegue, verificar:

1. **Job creado**: Workflows → Jobs → Ver `pdf_processing_git`
2. **Tasks configuradas**: Debe tener 3 tasks (pdf_to_txt, txt_to_columns, txt_to_columns_validator)
3. **Dependencias**: Verificar que el environment tiene PyMuPDF y pymupdf4llm
4. **Ejecutar prueba**: Run now con un PDF de prueba

## 🐛 Troubleshooting

### Error: "File not found: src/pipelines/pdf_processing/..."

**Solución**: Verificar que los scripts están en la estructura correcta en Git.

### Error: "Volume not found"

**Solución**: Crear los Volumes antes de ejecutar el job.

### Error: "Table not found"

**Solución**: Crear la tabla Delta antes de ejecutar.

### Error: "Module not found: PyMuPDF"

**Solución**: Verificar que el Environment tiene las dependencias instaladas.

### Error: "Git authentication failed"

**Solución**: Reconfigurar Git Integration en Databricks Settings.

## 📚 Recursos Adicionales

- [Databricks Jobs Documentation](https://docs.databricks.com/workflows/jobs/index.html)
- [Databricks Git Integration](https://docs.databricks.com/repos/index.html)
- [Databricks Volumes](https://docs.databricks.com/volumes/index.html)

