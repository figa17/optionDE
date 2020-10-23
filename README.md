# Option Data Engineer

### Build

- Build docker image

    `sudo docker build -t optionde .`
    
- Run image

    `sudo docker run optionde`

### Publish gcp function

    gcloud functions deploy java-gcs-function \
    --entry-point MainFunction \
    --runtime java11 \
    --memory 512MB \
    --trigger-resource optio-de \
    --trigger-event google.storage.object.finalize