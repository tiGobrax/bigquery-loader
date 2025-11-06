from fastapi import APIRouter, UploadFile, File, Form
from fastapi.responses import JSONResponse
from loguru import logger
from app.services.bigquery_service import BigQueryService

bq_service = BigQueryService()
router = APIRouter()


@router.post("/load/json")
async def load_from_json(payload: dict):
    """
    Exemplo de corpo JSON:
    {
        "project_id": "gobrax-data",
        "dataset_id": "raw_generic",
        "table_name": "hubspot_contacts",
        "data": [{...}, {...}]
    }
    """
    try:
        project = payload.get("project_id")
        dataset = payload.get("dataset_id")
        table = payload.get("table_name")
        data = payload.get("data", [])

        if not all([project, dataset, table, data]):
            return JSONResponse(
                status_code=400,
                content={"error": "Campos obrigatórios: project_id, dataset_id, table_name, data"}
            )

        result = bq_service.load_table_from_json(project, dataset, table, data)
        return JSONResponse(content=result)

    except Exception as e:
        logger.error(f"Erro na carga JSON: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@router.post("/load/parquet")
async def load_from_parquet(
    project_id: str = Form(...),
    dataset_id: str = Form(...),
    table_name: str = Form(...),
    file: UploadFile = File(...)
):
    """
    Recebe upload de arquivo .parquet e carrega no BigQuery.
    """
    try:
        file_path = f"/tmp/{file.filename}"
        with open(file_path, "wb") as f:
            f.write(await file.read())

        result = bq_service.load_table_from_parquet(project_id, dataset_id, table_name, file_path)
        return JSONResponse(content=result)

    except Exception as e:
        logger.error(f"Erro na carga Parquet: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
