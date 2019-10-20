# Sample

This demo application with apache kafka, redis and java

## Development

Before you can build this project, you must install and configure the following dependencies on your machine:

###### java 1.8
###### docker
###### minikube
###### kubecli

### Packaging as jar

To build the final jar and optimize the sample application for production, run:

    ./mvn clean package



You can also fully dockerize your application and all the services that it depends on.
To achieve this, first build a docker image of your app by running:

    ./mvn verify jib:dockerBuild

Then run:

    docker-compose -f src/main/docker/app.yml up -d

Stop:

    docker-compose -f src/main/docker/app.yml down


For more information refer to [Using Docker and Docker-Compose][]

[using docker and docker-compose]: https://www.jhipster.tech/documentation-archive/v6.0.1/docker-compose
