## Udesa - Ingeneriea de Software y Datos

# Documentación del Proceso: Medallion Architecture con Airflow + dbt + DuckDB

Este documento describe el paso a paso que seguimos para configurar y ejecutar el pipeline de arquitectura medallion utilizando Apache Airflow, dbt y DuckDB.

---

## 1. Configuración del Entorno

### 1.1 Creación del entorno Conda

Creamos un entorno Conda con Python 3.10:

```bash
conda create -n eval_ii_ing python=3.10 -y
conda activate eval_ii_ing
```

### 1.2 Instalación de dependencias

Instalamos las dependencias del proyecto:

```bash
pip install --upgrade pip
pip install -r requirements.txt
```
---

## 2. Implementación del DAG de Airflow

### 2.1 Estructura del DAG

Completamos el archivo `medallion_medallion_dag.py` con tres tasks principales:

1. **`bronze_clean_task`**: Lee el CSV crudo del día y genera un archivo Parquet limpio usando Pandas. (Utilizan la funcion provista en transactions.clean_daily_transactions)
2. **`silver_dbt_run_task`**: Ejecuta `dbt run` para cargar los datos en DuckDB y generar modelos intermedios.
3. **`gold_dbt_tests_task`**: Ejecuta `dbt test` y escribe los resultados de calidad en un archivo JSON.

## 2. Ejecución de Airflow

### 2.1 Variables de entorno

Configuramos las variables de entorno necesarias:

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export DBT_PROFILES_DIR=$(pwd)/profiles
export DUCKDB_PATH=$(pwd)/warehouse/medallion.duckdb
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### 3.2 Error encontrado:  crash en macOS

Al iniciar Airflow, el DAG processor generaba un error
**Solución**: Agregamos la variable de entorno:
```bash
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
```

### 3.3 Iniciar Airflow

```bash
airflow standalone
```
Credenciales de acceso:
- **URL**: http://localhost:8080
- **Usuario**: admin
- **Contraseña**: (ver en `airflow_home/simple_auth_manager_passwords.json.generated`)
---

## 4. Errores encontrados en dbt y sus soluciones

### 4.1 Error: Path incorrecto para archivos parquet

**Problema**: dbt archivo no encontrado:
```
IO Error: No files found that match the pattern "/data/clean/transactions_20251206_clean.parquet"
```

**Causa**: El path `/data/clean/...` era relativo a la raíz del sistema en lugar del directorio del proyecto.

**Solución**: Confirmamos que al pasar las variables con `--vars` y paths absolutos, el problema se resolvió.

### 4.3 Error: Archivo no encontrado para la fecha

**Problema**: El DAG fallaba porque no existía el archivo CSV para la fecha de ejecución.

**Causa**: Solo existía `transactions_20251201.csv` Y el DAG buscaba archivos con la fecha del día.

**Solución**: Creamos copias del archivo para las fechas adicionales:
```bash
cp data/raw/transactions_20251201.csv data/raw/transactions_20251205.csv
cp data/raw/transactions_20251201.csv data/raw/transactions_20251206.csv
```
---

## 5. Verificación de resultados

### 5.1 Archivos generados

```bash
# Bronze layer - Parquet limpio
ls data/clean/
# → transactions_20251201_clean.parquet, transactions_20251205_clean.parquet

# Gold layer - Resultados de calidad
cat data/quality/dq_results_20251205.json
```

### 5.2 Consultar DuckDB

```bash
duckdb warehouse/medallion.duckdb -c "SELECT * FROM fct_customer_transactions LIMIT 5"
```

---

## 7. Archivos importantes modificados

| Archivo | Cambio |
|---------|--------|
| `dags/medallion_medallion_dag.py` | Implementación completa del DAG con bronze/silver/gold tasks |
| `profiles/profiles.yml` | Path actualizado para el entorno local |
| `.gitignore` | Creado con exclusiones para airflow_home, warehouse, profiles, etc. |
| `dbt/tests` | Se agrego un test que verifica que todas las transacciones sean del 2025 o posterior |
| `dbt/staging/schema.yaml` | se agregó el test para validar las fechas|


```
┌────────────────────────────────────────────────────────────────────────┐
│                    FLUJO TÍPICO DE DATOS                               │
│                                                                        │
│  ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────┐ │
│  │ Fuentes │───▶│Ingestión│───▶│Transform│───▶│  Store  │───▶│BI/ML│ │
│  │  (APIs, │    │  (ETL/  │    │  (dbt,  │    │  (DWH)  │    │     │ │
│  │  DBs)   │    │  ELT)   │    │ Spark)  │    │         │    │     │ │
│  └─────────┘    └─────────┘    └─────────┘    └─────────┘    └─────┘ │
└────────────────────────────────────────────────────────────────────────┘
```


```
┌─────────────────────────────────────────────────────────────────────────┐
│                      ARQUITECTURA MEDALLION                              │
│                                                                          │
│  ┌───────────────┐   ┌───────────────┐   ┌───────────────┐             │
│  │    BRONZE     │   │    SILVER     │   │     GOLD      │             │
│  │   (Raw Data)  │──▶│ (Clean Data)  │──▶│(Business Data)│             │
│  └───────────────┘   └───────────────┘   └───────────────┘             │
│         │                   │                    │                      │
│    Datos crudos        Datos limpios       Datos agregados             │
│    tal cual llegan     y validados         listos para BI              │
│                                                                          │
│  Ejemplo:              Ejemplo:            Ejemplo:                      │
│  - CSV con errores     - Nulos removidos   - Ventas por mes             │
│  - JSON de API         - Tipos correctos   - KPIs de negocio            │
│  - Logs sin procesar   - Deduplicados      - Dashboards                 │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 8. Comandos útiles

```bash
# Activar entorno
conda activate eval_ii_ing

# Iniciar Airflow (con fix para macOS)
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
airflow standalone

# Ejecutar dbt manualmente
cd dbt
dbt run --vars '{"clean_dir": "/path/to/data/clean", "ds_nodash": "20251201"}'
dbt test --vars '{"clean_dir": "/path/to/data/clean", "ds_nodash": "20251201"}'

# Listar DAGs
airflow dags list

# Triggerearlo (run_id único)
airflow dags trigger medallion_pipeline --run-id manual_$(date +%s)
```

---

## 9. Lecciones aprendidas

1. **macOS y fork()**: Siempre exportar `OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES` antes de correr Airflow.
2. **Variables de dbt**: Pasarlas explícitamente con `--vars` para evitar problemas de resolución de paths.
3. **Catchup en Airflow**: Tener en cuenta que genera ejecuciones para todas las fechas desde `start_date`.
4. **Re-ejecución**: Usar "Clear" en la UI en lugar de triggerearlo de nuevo para la misma fecha.


#### Autores
Damian Ilkow  ilkowdamian@gmail.com
Flores Jorge jfflores@udesa.edu.ar