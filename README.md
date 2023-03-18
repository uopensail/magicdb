# magicdb
magicdb是基于[sqlite](https://www.sqlite.org/index.html), [etcd](https://etcd.io/), [S3](https://aws.amazon.com/s3/)/[OSS](https://help.aliyun.com/product/31815.html)以及本地SSD来实现的低成本数据存取工具。 这里没有什么技术创新点, 只是一些常用工具的组合。

## Quickstart
## magicdb-cli
magicdb-cli是管理的客户端, 可以通过该工具进行数据管理，包括机器/数据库/表/版本的管理。

## magicdb-engine
magicdb-engine是数据服务, 根据配置信息对外提供服务。

## Tutorials
[read this tutorials.md](tutorials.md)
## Install

## Design and Architecture
### 起因

在之前的工作工程中, 多次存在这样的需求, 即利用大数据工具hive/spark来处理用户历史数据, 然后生成用户的一些特征, 生成的用户特征又需要提供给线上使用, 同时这些特征的生成频次通常都是按小时或者按天。

在业务预算很多的时候, 通常都选择用redis这样的工具去存储。因为redis的读写性能都很高, 故而在写入的时候对线上也没有什么影响。但是当业务预算吃紧的时候, 高成本的redis方案就不被允许了。选择其他常见的数据库比如mysql, MongoDB，都会在写入的时候对线上产生冲击, 使用HBASE,Cassendra等又觉得太重了。于是我们自己打算开发一个适合业务的存储工具。

上述的业务数据有`数据量大`,`读写分离`等特点。于是我们在选择存储方案的时候, 在rocksdb和sqlite之间进行取舍。因为数据是读写分离的, 最终我们选择了用sqlite进行存储。原因是, sqlite利用的是B+树的索引,查询成本是相对固定的, 不存在读放大的问题。

### 涉及的工具
magicdb-engine/magicdb-cli用到了如下的一些技术:
1. sqlite: 用来存储结构化的数据
2. etcd: 用来做服务发现和事件监控
3. anltr: 用来解析magicdb-cli命令
4. oss/s3: 存储数据


### 数据流
1. 输入数据是事先准备好的hive表, 存放在S3/OSS等对象存储工具上，目前来说, 我们支持的数据格式只有parquet。
2. 利用magicdb-cli中的load命令从S3/OSS上把数据转换成sqlite格式, 然后再上传到S3/OSS上去, 以及生成一些meta文件。
   1. 我们的目标是中小型的企业数据, 我们在处理数据的时候, 把S3/OSS的原始拉回到本地, 利用本地计算资源进行数据处理, 然后再上传到S3/OSS上。后续会考虑支持集群的模型计算。
   2. 我们会利用[Murmurhash](https://en.wikipedia.org/wiki/MurmurHash)将主键分桶K个桶, 然后将数据写到K个sqlite文件中。
3. magicdb-engine服务会去wathch etcd的事件, 然后对数据进行更新, 提供对外访问。
   
### 数据库的操作
```json
//path: /magicdb/storage/machines/${ip}
{
    "database": "db1"
}

//path: /magicdb/storage/databases/${db}
{
    "machines":["ip1","ip2", "ip3"],
    "name": "db1",
    "bucket": "s3://bucket",
    "endpoint": "xxx.xxx.xxxx",
    "access_key": "xxxxx",
    "secret_key": "xxxxxx",
    "tables": ["table1", "table2", "table3"]
}


//path: /magicdb/storage/databases/${db}/${table}
{
    "name": "table1",
    "database":"db1",
    "data": "data_dir",
    "meta": "meta_dir",
    "current_version": "xxxxx",
    "versions": ["v1", "v2", "v3"],
    "partitions": 100,
    "key":"pk",
}
```