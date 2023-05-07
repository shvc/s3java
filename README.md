## s3java
s3java is a java edition [s3cli](https://github.com/shvc/s3cli)  
Inspired by [awssdk](https://github.com/awsdocs/aws-doc-sdk-examples) examples

#### Download prebuilt [binary](https://github.com/shvc/s3java/releases)

#### Or build fat jar from source
```
# need gradle7 or above
git clone https://github.com/shvc/s3java
cd s3java
gradle jar
```

## Usage
#### Create alias
```
alias s3java="java -jar `pwd`/build/libs/s3java-1.2.jar"
s3java -h
```

#### Bucket
```shell
# create Bucket
s3java -e http://127.0.0.1:9000 -a root -s ChangeMe create-bucket bucket-name

# list(ls) all my Buckets
s3java -e http://127.0.0.1:9000 -a root -s ChangeMe ls

# head Bucket
s3java -e http://127.0.0.1:9000 -a root -s ChangeMe head bucket-name

# delete Bucket
s3java -e http://127.0.0.1:9000 -a root -s ChangeMe delete bucket-name
```

#### Object
- upload(put) Object(s)
```shell
# upload file(s)
s3java upload bucket-name/k0 --data KKKK          # upload a Object(k0) with content KKKK
s3java upload bucket-name/k1 /etc/hosts           # upload a file and specify Key(k1)
s3java --v2sign upload bucket-name/k2 /etc/hosts  # upload(V2 sign) a file and specify Key(k2)
s3java upload bucket-name /etc/hosts              # upload a file and use filename(hosts) as Key
s3java upload bucket-name *.txt                   # upload files and use filename as Key
s3java upload bucket-name/dir/ *.txt              # upload files and set Prefix(dir/) to all uploaded Object
s3java --presign put bucket-name/k3 file          # presign(V4) a PUT Object URL
s3java --presign --v2sign put bucket-name/k4 file # presign(V2) a PUT Object URL
```
- download(get) Object(s)
```shell
# download Object(s)
s3java download bucket-name/k0                    # download Object(k0) to current dir
s3java --v2sign download bucket-name/k2           # download(V2 sign) Object(k2) to current dir
s3java download bucket-name/k1 k2 k3              # download Objects(k1, k2 and k3) to current dir
s3java --presign download bucket-name/k1          # presign(V4) a GET Object URL
s3java --presign --v2sign download bucket-name/k2 # presign(V2) a GET Object URL
```

- list(ls) Objects
```shell
# list Objects
s3java list bucket-name           # list
s3java list bucket-name/prefix    # list Objects with specified prefix
s3java list-v2 bucket-name        # listObjects v2
s3java list-v2 bucket-name/prefix # listObjects v2 with specified prefix
```

- delete(rm) Object(s)
```shell
# delete Object(s)
s3java delete bucket-name/k0                    # delete an Object
s3java delete bucket-name/k1 k2 k3                 # delete Objects
s3java --presign delete bucket-name/hosts       # presign(V4) an DELETE Object URL
s3java --presign --v2sign delete bucket-name/k4 # presign(V2) an DELETE Object URL
```
