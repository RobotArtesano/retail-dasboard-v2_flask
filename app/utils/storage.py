# app/utils/storage.py
import os
import boto3
from botocore.exceptions import ClientError
from datetime import datetime

def get_s3_client():
    """Inicializa y retorna el cliente de Boto3 configurado para Cloudflare R2."""
    return boto3.client(
        service_name='s3',
        endpoint_url=os.environ.get('R2_ENDPOINT_URL'),
        aws_access_key_id=os.environ.get('R2_ACCESS_KEY'),
        aws_secret_access_key=os.environ.get('R2_SECRET_KEY'),
        region_name='auto' # R2 usa 'auto' como región
    )

def backup_file_to_r2(file_bytes: bytes, original_filename: str, upload_type: str, user_id: int) -> bool:
    """
    Sube el archivo crudo a R2 organizándolo por carpetas.
    Retorna True si fue exitoso, False si falló.
    """
    bucket_name = os.environ.get('R2_BUCKET_NAME')
    if not bucket_name:
        print("Error: R2_BUCKET_NAME no está configurado.")
        return False

    s3 = get_s3_client()
    
    # Generamos un nombre único y organizado: ej. "user_1/sales/20260310_153022_ventas.csv"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_filename = original_filename.replace(" ", "_")
    object_name = f"user_{user_id}/{upload_type}/{timestamp}_{safe_filename}"

    try:
        # Subimos los bytes directamente a la nube
        s3.put_object(
            Bucket=bucket_name,
            Key=object_name,
            Body=file_bytes
        )
        return True
    except ClientError as e:
        print(f"Error al subir archivo a R2: {e}")
        return False