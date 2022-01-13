# s3java
java s3cli

# Build
```shell
gradle jar
```
# Run
```shell
java -jar build/libs/s3java-1.0.jar 
```

# Usage
```shell
java -jar s3java-1.0.jar                       
Usage:       [-hV] [--path-style] [--tcp-keep-alive] [--v2-signer]
             [-a=<accessKey>]
             [--client-execution-timeout=<clientExecutionTimeout>]
             [--connection-max-idle-millis=<connectionMaxIdleMillis>]
             [--connection-timeout=<connectionTimeout>]
             [--connection-ttl=<connectionTTL>] [-e=<endpoint>]
             [--max-connections=<maxConnections>]
             [--max-error-retry=<maxErrorRetry>] [-r=<region>]
             [--request-timeout=<requestTimeout>] [-s=<secretKey>]
             [--socket-timeout=<socketTimeout>] [-H[=Key:Value...]]... [COMMAND]
S3 command line tool
  -a, --ak=<accessKey>    S3 access key
                            Default: root
      --client-execution-timeout=<clientExecutionTimeout>
                          S3 Client execution timeout in ms
                            Default: 0
      --connection-max-idle-millis=<connectionMaxIdleMillis>
                          S3 Client connection max idle millis
                            Default: 60000
      --connection-timeout=<connectionTimeout>
                          S3 Client connection timeout in ms
                            Default: 10000
      --connection-ttl=<connectionTTL>
                          S3 Client connection ttl
                            Default: -1
  -e, --endpoint=<endpoint>
                          S3 endpoint
                            Default: http://192.168.0.8:9000
  -h, --help              Show this help message and exit.
  -H, --header[=Key:Value...]
                          S3 Client request header
      --max-connections=<maxConnections>
                          S3 Client max connections
                            Default: 50
      --max-error-retry=<maxErrorRetry>
                          S3 Client max error retry
                            Default: -1
      --path-style        S3 Client path style
                            Default: true
  -r, --region=<region>   S3 endpoint
                            Default: cn-north-1
      --request-timeout=<requestTimeout>
                          S3 Client request timeout in ms
                            Default: 0
  -s, --sk=<secretKey>    S3 secret key
                            Default: ChangeMe
      --socket-timeout=<socketTimeout>
                          S3 Client socket timeout in ms
                            Default: 50000
      --tcp-keep-alive    S3 Client TCP keep alive
                            Default: false
  -V, --version           Print version information and exit.
      --v2sign            S3 Client v2 sign
                            Default: false
Commands:
  help           Displays help information about the specified command
  delete, rm     delete Object(s)
  download, get  download Object(s)
  head           head Bucket(Objects)
  list, ls       list Bucket(Objects)
  upload, put    upload file(s)
```
