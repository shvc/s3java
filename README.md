## s3java
s3java is a java edition [s3cli](https://github.com/shvc/s3cli)  
Inspired by [awssdk](https://github.com/awsdocs/aws-doc-sdk-examples)

#### Download prebuild [binary](https://github.com/shvc/s3java/releases)  

#### Or build fat jar from source
```
git clone https://github.com/shvc/s3java
cd s3java
gradle jar
```

## Usage
#### Create alias
```
s3java='java -jar /path/to/s3java-1.1.jar'
s3java -h
```
#### Bucket
```shell
# create Bucket
java -jar s3java-1.1.jar -e http://192.168.56.3:9000 -a root -s ChangeMe create-bucket bucket-name

# list(ls) all my Buckets
java -jar s3java-1.1.jar -e http://192.168.56.3:9000 -a root -s ChangeMe ls

# head Bucket
java -jar s3java-1.1.jar -e http://192.168.56.3:9000 -a root -s ChangeMe head bucket-name

# delete Bucket
java -jar s3java-1.1.jar -e http://192.168.56.3:9000 -a root -s ChangeMe delete bucket-name
```

#### Object
- upload(put) Objcet(s)  
```shell
# upload file(s)
java -jar s3java-1.1.jar upload bucket-name/k0 --data KKKK          # upload a Object(k0) with content KKKK
java -jar s3java-1.1.jar upload bucket-name/k1 /etc/hosts           # upload a file and specify Key(k1)
java -jar s3java-1.1.jar --v2sign upload bucket-name/k2 /etc/hosts  # upload(V2 sign) a file and specify Key(k2)
java -jar s3java-1.1.jar upload bucket-name /etc/hosts              # upload a file and use filename(hosts) as Key
java -jar s3java-1.1.jar upload bucket-name *.txt                   # upload files and use filename as Key
java -jar s3java-1.1.jar upload bucket-name/dir/ *.txt              # upload files and set Prefix(dir/) to all uploaded Object
java -jar s3java-1.1.jar --presign put bucket-name/k3 file          # presign(V4) a PUT Object URL
java -jar s3java-1.1.jar --presign --v2sign put bucket-name/k4 file # presign(V2) a PUT Object URL
```
- download(get) Object(s)  
```shell
# download Object(s)
java -jar s3java-1.1.jar download bucket-name/k0                    # download Object(k0) to current dir
java -jar s3java-1.1.jar --v2sign download bucket-name/k2           # download(V2 sign) Object(k2) to current dir
java -jar s3java-1.1.jar download bucket-name/k1 k2 k3              # download Objects(k1, k2 and k3) to current dir
java -jar s3java-1.1.jar --presign download bucket-name/k1          # presign(V4) a GET Object URL
java -jar s3java-1.1.jar --presign --v2sign download bucket-name/k2 # presign(V2) a GET Object URL
```

- list(ls) Objects  
```shell
# list Objects
java -jar s3java-1.1.jar list bucket-name           # list
java -jar s3java-1.1.jar list bucket-name/prefix    # list Objects with specified prefix
java -jar s3java-1.1.jar list-v2 bucket-name        # listObjects v2
java -jar s3java-1.1.jar list-v2 bucket-name/prefix # listObjects v2 with specified prefix
```

- delete(rm) Object(s)  
```shell
# delete Object(s)
java -jar s3java-1.1.jar delete bucket-name/k0                    # delete an Object
java -jar s3java-1.1.jar delete bucket-name/k1 k2 k3              # delete Objects
java -jar s3java-1.1.jar --presign delete bucket-name/hosts       # presign(V4) an DELETE Object URL
java -jar s3java-1.1.jar --presign --v2sign delete bucket-name/k4 # presign(V2) an DELETE Object URL
```
