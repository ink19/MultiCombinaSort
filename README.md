## Achieved

1. 大文件排序
2. 懒惰排序
3. 服务端的单元测试
4. 部分脏数据处理
5. 开始的通信协议，解决了开始时的脏数据问题
6. 大数排序

## TODO

1. 通信协议
2. 断点重传
4. 大文件排序优化
5. 考虑使用通信协议进行终止

## 协议定义

### 通信方式

使用Redis的PubSub。

每个连接有两条通道，sCommit_#id，cCommit_#id，其中

sCommit_#id 为server端写入，client端读取
cCommit_#id 为client端写入，server端读取

### 编码规则

使用`,`分割，第一个字段为code，其后的字段为数据


### 准备阶段的协议

1. Hello, 0

在程序一开始时，s端和c端都会发出此信息，以邀请已经运行的端进行通信。

c端收到Hello之后，直接返回Hello

s端收到Hello之后，开始对Redis进行初始化，然后返回Ready

2. Ready, 1

c端收到Ready之后，进行初始化工作，然后返回Ready

s端收到Ready之后，判断自身是否已经完成初始化，若完成则返回Start，否则返回Hello，重新开始

3. Start, 2
  
c端收到Start之后，返回Start，进入工作状态

s端收到Start之后，进入工作状态

缺点：有可能出现重复Hello，导致工作不正常，譬如在s端启动发送Hello后，此时c端启动发送Hello，c端接受到s端的Hello又发送Hello，可能导致奇怪的情况。