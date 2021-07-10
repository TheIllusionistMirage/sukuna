# Sukuna

Sukuna is an experimental log based (SSTable) key-value storage engine.

## Dependencies

* JDK 16 or higher
* Maven 3.8.1 or higher

## Build

```bash
$ git clone https://github.com/TheIllusionistMirage/sukuna.git
$ cd sukuna
$ ./build.sh
```

## Trying It Out

### Starting the Service

`sukuna-service` is the default TCP based DB service program.

```bash
$ cd sukuna-service
$ java -jar ./target/sukuna-service-<version>-shaded.jar  # Where version is the application version in sukuna-service/pom.xml
```

### Using the Client

`sukuna-cli-client` is the default TCP based DB client program.

```bash
$ cd sukuna-cli-client
$ java -jar ./target/sukuna-cli-client-<version>-shaded.jar  # Where version is the application version in sukuna-cli-client/pom.xml
```

Currently only two operations are supported: 

* `get <key>`
* `set <key> <value>`