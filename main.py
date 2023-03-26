import os
import json
from google.cloud import documentai
from google.cloud import storage
from google.cloud import bigquery
from typing import List, Sequence
# import logging
# logging.basicConfig(level=logging.DEBUG)


def list_files(bucket_name, origen):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f"{origen}/")
    pdf_files = [blob.name.split("/")[-1] for blob in blobs if blob.name.lower().endswith('.pdf')]
    return pdf_files


def online_process(project_id: str, location: str, processor_id: str, bucket_name: str, origen: str, pdf_file: str,
                   mime_type: str) -> documentai.Document:

    opts = {"api_endpoint": f"{location}-documentai.googleapis.com"}
    documentai_client = documentai.DocumentProcessorServiceClient(client_options=opts)
    resource_name = documentai_client.processor_path(project_id, location, processor_id)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(origen+"/"+pdf_file)
    image_content = blob.download_as_bytes()

    # Load Binary Data into Document AI RawDocument Object
    raw_document = documentai.RawDocument(
        content=image_content, mime_type=mime_type
    )

    request = documentai.ProcessRequest(
        name=resource_name, raw_document=raw_document
    )

    result = documentai_client.process_document(request=request)
    return result.document


def get_table_data(rows: Sequence[documentai.Document.Page.Table.TableRow], text: str) -> List[List[str]]:
    all_values: List[List[str]] = []
    for row in rows:
        current_row_values: List[str] = []
        for cell in row.cells:
            current_row_values.append(
                text_anchor_to_text(cell.layout.text_anchor, text)
            )
        all_values.append(current_row_values)
    return all_values


def text_anchor_to_text(text_anchor: documentai.Document.TextAnchor, text: str) -> str:
    response = ""
    # If a text segment spans several lines, it will
    # be stored in different text segments.
    for segment in text_anchor.text_segments:
        start_index = int(segment.start_index)
        end_index = int(segment.end_index)
        response += text[start_index:end_index]
    return response.strip().replace("\n", " ")


def print_entity(entity: documentai.Document.Entity):
    key = entity.type_
    text_value = entity.text_anchor.content
    confidence = entity.confidence
    entity_dict = {
        key: text_value
    }
    # print (json.dumps(entity_dict))
    return entity_dict


def move_to_processed(project_id: str, bucket_name: str, origen: str, destino: str, file_name: str):
    cliente_gcs = storage.Client(project=project_id)
    bucket = cliente_gcs.bucket(bucket_name)
    blob_origen = bucket.blob(f"{origen}/{file_name}")
    blob_destino = bucket.blob(f"{destino}/{file_name}")
    blob_destino.upload_from_string(blob_origen.download_as_string())
    blob_origen.delete()
    print(f"El archivo {file_name} se ha movido de {origen} a {destino} en el bucket {bucket_name}.")


def insert_to_db(project_id:str, dataset_id:str, table_id:str, my_array):
    # Crear una instancia de la tabla
    cliente_bq = bigquery.Client(project=project_id)
    tabla_ref = cliente_bq.dataset(dataset_id).table(table_id)
    tabla = cliente_bq.get_table(tabla_ref)

    # Insertar los datos en la tabla
    error = cliente_bq.insert_rows_json(tabla, my_array)

    if not error:
        print("Los datos se insertaron correctamente en la tabla de BigQuery")
    else:
        print(f"Ocurrió un error al insertar los datos: {error}")


if __name__ == '__main__':
    print(" ***** ANALIZANDO POLIZAS DE EGRESOS ***** ")
    PROJECT_ID = os.environ['PROJECT_ID']
    LOCATION = os.environ['LOCATION']
    PROCESSOR_ID = os.environ['PROCESSOR_ID']
    BUCKET_NAME = os.environ['BUCKET_NAME']
    DATASET_ID = os.environ['DATASET_ID']
    TABLE_ID = os.environ['TABLE_ID']
    ORIGEN = os.environ['ORIGEN']
    DESTINO = os.environ['DESTINO']

    print(f'PROJECT_ID: {PROJECT_ID}')
    print(f'LOCATION: {LOCATION}')
    print(f'PROCESSOR_ID: {PROCESSOR_ID}')
    print(f'BUCKET_NAME: {BUCKET_NAME}')
    print(f'DATASET_ID: {DATASET_ID}')
    print(f'TABLE_ID: {TABLE_ID}')
    print(f'ORIGEN: {ORIGEN}')
    print(f'DESTINO: {DESTINO}')

    pdf_files = list_files(BUCKET_NAME, ORIGEN)
    my_array = []
    my_processed_files = []
    for pdf_file in pdf_files:
        print(f"pdf_file: {pdf_file}")
        MIME_TYPE = "application/pdf"
        document = online_process(project_id=PROJECT_ID, location=LOCATION, processor_id=PROCESSOR_ID,
                                  bucket_name=BUCKET_NAME, origen=ORIGEN, pdf_file=pdf_file, mime_type=MIME_TYPE)

        print(f"Found {len(document.entities)} entities:")

        primary_dict = {}
        secondary_dict = {}
        final_dict = {}

        for entity in document.entities:
            if len(entity.properties) == 0:
                my_tmp_dict1 = print_entity(entity)
                primary_dict.update(my_tmp_dict1)
            else:
                for prop in entity.properties:
                    my_tmp_dict2 = print_entity(prop)
                    secondary_dict.update(my_tmp_dict2)
                final_dict.update(primary_dict)
                final_dict.update(secondary_dict)
                my_array.append(final_dict)
                secondary_dict = {}
                final_dict = {}
        print(f"{my_array}")
        insert_to_db(PROJECT_ID, DATASET_ID, TABLE_ID, my_array)
        move_to_processed(PROJECT_ID, BUCKET_NAME, ORIGEN, DESTINO, pdf_file)
        my_processed_files.append(pdf_file)
        my_array = []

    headers = {
        'Content-Type': 'application/json'
    }
    payload = json.dumps(my_processed_files)
    print(f"payload -> {payload} - headers -> {headers}")
    # return payload, headers
