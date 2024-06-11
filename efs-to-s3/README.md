# efs-to-s3

This image uploads every directory under `/input` path to given bucket. You can use this image and only configure it's mount points and let it automatially upload to S3.

Required parameters(as environment variables):

```
i8i_OUTPUT_S3_BUCKET(required)
i8i_OUTPUT_S3_PREFIX(required)
REGION
```
