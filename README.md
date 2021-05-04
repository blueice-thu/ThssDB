# ThssDB
A simple database based on ThssDB

# 核心模块

- [ ] 通信模块
- [ ] 异常处理模块
- [ ] 存储模块
- [ ] 元数据管理模块
- [ ] 查询模块
- [ ] 事务与恢复模块

# 时间节点

- [ ] 通信模块和存储模块：5 月 7 日（周五）
- [ ] 元数据管理模块：5 月 17 日（周一）
- [ ] 查询模块：5 月 28 日（周五）
- [ ] 事务与恢复模块：6 月 4 日（周五）
- [ ] 最终提交（展示准备）：6 月 7 日（周一）

提交内容：逐步完成系统的各个模块，逐次提交源代码和设计文档，最终提交需要提交设计文档、用户文档、展示 PPT 及源代码。

# 运行方法

在 Windows 平台下执行 `run_thrift.bat` 脚本，生成接口文件。linux 和 mac 平台参考脚本内容。

# 代码架构

```
client
|__client           客户端启动文件，接收命令
rpc.thrift          thrift 自动生成的抽象类
server
|__client           服务端启动文件，分配 handler
service
|__IServiceHandler  继承 thrift 生成的抽象类，业务的具体实现
utils
|__Global           默认常量
```