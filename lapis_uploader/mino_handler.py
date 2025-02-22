from minio import Minio
from minio.error import S3Error
import os



def create_client():

    client = Minio(
        endpoint="s3.learnbasics.fun",
        # endpoint="172.27.0.24:9000",
        access_key="pythonminio",
        secret_key="KhTbfEi2N7jkUKuhJnioqYbmgdtzngL4q5n0VTNy",
        secure=True
    )

    return client
  

if __name__ == "__main__":
    try:
        client = create_client()
        
        bucket_list = client.list_buckets()
        
        print(bucket_list)
    except S3Error as exc:
        print("error occurred.", exc)