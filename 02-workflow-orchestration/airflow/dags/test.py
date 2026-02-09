from airflow.providers.google.cloud.hooks.gcs import GCSHook

hook = GCSHook()
hook.list("your-bucket-name")
