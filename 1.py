Perfecto. Aquí tienes el **paso a paso detallado para hacer push del Docker image de tu Flex Template a Artifact Registry (o Container Registry) sin usar `gcloud dataflow flex-template build` o `gcloud dataflow flex-template run` (es decir, sin `submit`)**, ideal para controlar costos y mantener control manual del deployment.

---

### 🔧 REQUISITOS PREVIOS

* Tener habilitado **Artifact Registry** o **Container Registry**.
* Tener el archivo `Dockerfile` y `main.py` en la raíz de tu proyecto.
* Tener `requirements.txt` o `setup.py` si usas dependencias.
* Haber autenticado Docker con GCP:

```bash
gcloud auth configure-docker
```

---

## ✅ PASO A PASO: **Build y Push sin `submit`**

### 1. Define variables de entorno

```bash
export PROJECT_ID=rxo-dataeng-datalake-np
export REGION=us-central1
export IMAGE_NAME=sql-pqt-to-gcs
export REPO_NAME=dataflow-flex-template
export IMAGE_TAG=latest
export FULL_IMAGE_URI=${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/${IMAGE_NAME}:${IMAGE_TAG}
```

> 💡 Asegúrate de haber creado el repositorio si no existe:

```bash
gcloud artifacts repositories create ${REPO_NAME} \
  --repository-format=docker \
  --location=${REGION} \
  --description="Flex Templates for Dataflow"
```

---

### 2. Compila el Docker image localmente

Desde la raíz de tu proyecto (donde está `Dockerfile`):

```bash
docker build -t ${FULL_IMAGE_URI} .
```

---

### 3. Haz push del Docker image a Artifact Registry

```bash
docker push ${FULL_IMAGE_URI}
```

---

### 4. Crea el archivo `metadata.json`

Este archivo se necesita para lanzar el Flex Template desde Airflow o vía API:

```json
{
  "sdk_info": {
    "language": "PYTHON"
  },
  "name": "SQL Server to GCS Flex Template",
  "description": "Reads from SQL Server and writes Parquet to GCS using Beam",
  "image": "us-central1-docker.pkg.dev/rxo-dataeng-datalake-np/dataflow-flex-template/sql-pqt-to-gcs:latest",
  "environment": {
    "tempLocation": "gs://rxo-dataeng-datalake-np-dataflow/temp",
    "zone": "us-central1-a"
  },
  "metadata": {
    "parameters": [
      {"name": "gcp_project", "label": "GCP Project ID", "help_text": "Project ID"},
      {"name": "database", "label": "Database", "help_text": "SQL Server database name"},
      {"name": "schema", "label": "Schema", "help_text": "SQL Server schema name"},
      {"name": "table", "label": "Table", "help_text": "Table name"},
      {"name": "secret_id", "label": "Secret ID", "help_text": "Secret ID with credentials"},
      {"name": "output_gcs_path", "label": "Output GCS Path", "help_text": "Where to write parquet files"},
      {"name": "query", "label": "Query override", "help_text": "Optional custom query"},
      {"name": "primary_key", "label": "Primary Key", "help_text": "Used for ordering"},
      {"name": "type_of_extraction", "label": "Extraction Type", "help_text": "full or change_tracking"},
      {"name": "reload_flag", "label": "Reload", "help_text": "true or false"},
      {"name": "batch_size", "label": "Batch size", "help_text": "Rows per chunk"},
      {"name": "chunk_size", "label": "Chunk size", "help_text": "Chunk rows per write"}
    ]
  }
}
```

Guárdalo como:

```
sql-pqt-to-gcs/metadata.json
```

---

### 5. Sube el archivo `metadata.json` a GCS

```bash
gsutil cp sql-pqt-to-gcs/metadata.json gs://rxo-dataeng-datalake-np-dataflow/templates/sql-pqt-to-gcs.json
```

---

### ✅ ¿Qué lograste?

Ya tienes:

* Imagen de Docker publicada en Artifact Registry.
* Archivo `metadata.json` en GCS.
* Lista para ser lanzada desde Airflow (con `DataflowStartFlexTemplateOperator`) o desde consola/API.

---

¿Quieres que ahora te prepare el `Dockerfile`, `requirements.txt` o algún archivo de los módulos (`connection.py`, `sql_reader.py`, etc.)?
