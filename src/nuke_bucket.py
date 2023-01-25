"""
Delete all the S3 state in the development environmants

Thsi is script is stand-alone due to its gravity.

"""
import boto3

def delete_all_versions(bucket_name: str, prefix: str):
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)
    if prefix is None:
        bucket.object_versions.delete()
    else:
        bucket.object_versions.filter(Prefix=prefix).delete()

delete_all_versions("hopdevel-scimma-housekeeping", None) # empties the entire bucket
delete_all_versions("hopdevel-scimma-housekeeping-backup", None) # empties the entire bucket
#delete_all_versions("my_bucket", "my_prefix/") # deletes all objects matching the prefix (can be only one if only one matches)
