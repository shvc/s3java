# s3cli-java
s3cli

# Build
```shell
gradle jar
```
# Run
```shell
java -jar build/libs/s3j-1.0.jar 
```

# Usage
```shell
java -jar build/libs/s3cli-1.0.jar help   
Usage: s3cli [-hV] [--path-style] [-a=<accessKey>]
             [--connection-timeout=<connectionTimeout>] [-e=<endpoint>]
             [--execution-timeout=<executionTimeout>] [-r=<region>]
             [--request-timeout=<requestTimeout>] [-s=<secretKey>]
             [--socket-timeout=<socketTimeout>] [COMMAND]
S3 command line tool
  -a, --ak=<accessKey>    S3 access key
                            Default: root
      --connection-timeout=<connectionTimeout>
                          S3 Client connection timeout
                            Default: 10000
  -e, --endpoint=<endpoint>
                          S3 endpoint
                            Default: http://192.168.0.8:9000
      --execution-timeout=<executionTimeout>
                          S3 Client execution timeout
                            Default: 0
  -h, --help              Show this help message and exit.
      --path-style        S3 Client path style
                            Default: true
  -r, --region=<region>   S3 endpoint
                            Default: cn-north-1
      --request-timeout=<requestTimeout>
                          S3 Client request timeout
                            Default: 0
  -s, --sk=<secretKey>    S3 secret key
                            Default: ChangeMe
      --socket-timeout=<socketTimeout>
                          S3 Client socket timeout
                            Default: 50000
  -V, --version           Print version information and exit.
Commands:
  help           Displays help information about the specified command
  delete, rm     delete Object(s)
  download, get  download Object
  list, ls       list Bucket(Objects)
  upload, put    upload file
```
