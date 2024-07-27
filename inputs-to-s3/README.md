# inputs-to-s3

![alt text](https://i8i-content.s3.amazonaws.com/docs/images/inputsToS3.svg)

This image zips and uploads every directory under `/input` path to S3 bucket. You can use this image and only configure it's mount points and let it automatially upload to S3.

Image URI [public.ecr.aws/i8i/inputs-to-s3:latest](https://gallery.ecr.aws/i8i/inputs-to-s3)

Required parameters(environment variables):

```
i8i_STORAGE_S3_BUCKET(required)
i8i_OUTPUT_S3_PREFIX(required)
```
