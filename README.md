# Sukuna

Sukuna is an experimental log based (SSTable), disk backed, key-value storage engine.

## Dependencies

* JDK 16 or higher
* Maven 3.8.1 or higher

## Build

```bash
$ git clone https://github.com/TheIllusionistMirage/sukuna.git
$ cd sukuna
$ ./build.sh
```

This will build three JAR files corresponding to the modules `sukuna-engine`, `sukuna-service` and `sukuna-cli-client`.

The JAR for `sukuna-engine` can be embedded into any Java application that wants to control how the engine is hosted and things like setup 

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

## Documentation

You can find the documentation related to this project in the [`design`](design) directory.

## License

Sukuna is licensed under GNU AGPLv3, see [`LICENSE`](LICENSE) for more details.