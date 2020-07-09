#  使用Tidb事务的一些问题  
根据Tidb官网，https://pingcap.com/docs-cn/dev/transaction-isolation-levels/  
```
TiDB 实现了快照隔离 (Snapshot Isolation, SI) 级别的一致性。为与 MySQL 保持一致，又称其为“可重复读”。该隔离级别不同于 ANSI 可重复读隔离级别和 MySQL 可重复读隔离级别。

注意：

在 TiDB v3.0 中，事务的自动重试功能默认为禁用状态。不建议开启自动重试功能，因为可能导致事务隔离级别遭到破坏。更多关于事务自动重试的文档说明，请参考事务重试。

从 TiDB v3.0.8 版本开始，新创建的 TiDB 集群会默认使用悲观事务模式，悲观事务中的当前读（for update 读）为不可重复读，关于悲观事务使用注意事项，请参考悲观事务模式

```
## 太长直接看结论  
按照以上说明，TiDB v3.0.8以后，默认使用悲观事务。  
之前版本，如TiDB v2.1.1，默认使用乐观事务，实现快照隔离级别的一致性。**并且，自动重试默认是开启的。这将导致“事务隔离级别遭到破坏”。**  
如果使用TiDB 2.x，一定要检查以下配置，保证事务隔离级别。  
```
set @@global.tidb_disable_txn_auto_retry=1;  
```  
# 实验
在tidb:v2.1.1做以下实验。(悲观事务下不会产生以下问题，后一个事务会被阻塞住直到前一个事务提交。)  
建一个表test，3列，并加一行id为1的数据。
```
mysql> show columns in test;   
+-------+--------------+------+------+---------+-------+   
| Field | Type         | Null | Key  | Default | Extra |   
+-------+--------------+------+------+---------+-------+   
| id    | varchar(255) | YES  | UNI  | NULL    |       |   
| size  | bigint(20)   | YES  |      | NULL    |       |   
| value | bigint(20)   | YES  |      | 0       |       |   
+-------+--------------+------+------+---------+-------+   
mysql> insert into test (id, value, size) values (1, 111, 1000);
Query OK, 1 row affected (0.12 sec)

mysql> select * from test;
+------+------+-------+
| id   | size | value |
+------+------+-------+
| 1    | 1000 |   111 |
+------+------+-------+
1 row in set (0.01 sec)
```  
开启两个终端，分别提交相同的以下事务，一个新加行id为2，一个新加行id为3，并且都更新了id为1的行。  
**先开始id为2的事务，提交id为3的事务，再提交id为2的事务，模拟高并发场景。**    
id为2的事务先开始后提交  
```  
mysql> start transaction;
Query OK, 0 rows affected (0.01 sec)

mysql> update test set size=10002000,value=111222 where id=1;
Query OK, 1 row affected (0.00 sec)

mysql> insert into test (id, value, size) values (2, 222, 2000);
Query OK, 1 row affected (0.00 sec)

mysql> commit;
Query OK, 0 rows affected (0.02 sec)

```  
id为3的事务先提交。  
```  
mysql> start transaction;
Query OK, 0 rows affected (0.00 sec)

mysql> update test set size=10003000,value=111333 where id=1;
Query OK, 1 row affected (0.01 sec)

mysql> insert into test (id, value, size) values (3, 333, 3000);
Query OK, 1 row affected (0.01 sec)

mysql> commit;
Query OK, 0 rows affected (0.01 sec)

```  
预期结果应该是后提交的事务失败返回，因为都更新id为1的记录产生了冲突。  
但实际两个事务都提交成功了，两个insert都成功了，分别加入了id为2和3的新纪录，并且id为1的记录被后提交的事务中的update覆盖。  
这应用于我们的业务会产生一些问题。  
主要是因为TiDB事务会自动重试。  
```
mysql> select * from test;
+------+----------+--------+
| id   | size     | value  |
+------+----------+--------+
| 1    | 10002000 | 111222 |
| 3    |     3000 |    333 |
| 2    |     2000 |    222 |
+------+----------+--------+
3 rows in set (0.00 sec)
```  
加入如下配置，我们才能得到期望的结果。
```
set @@global.tidb_disable_txn_auto_retry=1;  
```  
重新实验，后提交的事务报错，符合预期。  
```
mysql> set @@global.tidb_disable_txn_auto_retry=1;
Query OK, 0 rows affected (0.01 sec)

mysql> start transaction;
Query OK, 0 rows affected (0.01 sec)

mysql> update test set size=10002000,value=111222 where id=1;
Query OK, 1 row affected (0.01 sec)

mysql> insert into test (id, value, size) values (2, 222, 2000);
Query OK, 1 row affected (0.01 sec)

mysql> commit;
ERROR 1105 (HY000): [try again later]: tikv restarts txn: retryable: write conflict
```

