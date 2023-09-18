### gdfs

---
本项目用仅记录本人在学习`mit6824`分布式系统的一些心得体会，并且将课堂上涉及到的一些关键论文和随课论文进行代码上的一些实现，本项目主要在**GFS**论文描述的基础上，增加了**GFS**的元信息结点的高可用集群，并期望做到元信息结点的**自动容错**，**主从切换**，项目还不完整，后续会持续跟进`TODO`。

- #### TODO
    1. 增加更多测试用例
    2. 增加更多的配置项
    3. 完善部署环节，修复容器部署的bug
    4. 修复chunkServer在网络分区的bug

- ##### [配置文件](./config.xml)
- 项目目录
```
├─build 脚本构建
├─cmd 启动进程
├─config 配置管理
├─internal 
│  ├─chunkServer 实现数据服务    
│  ├─client 实现客户端
│  ├─common 通用lib
│  │  └─rpc
│  ├─master 实现主服务
│  ├─types 类型声明
│  └─wal 同步控制
│      └─raft  协议层    
└─test 测试文件
```


- `本地运行`
- 生成脚本文件
  ```sh
  cd build && go run build.go script
  ```
  - 运行脚本文件
  ```sh
  #windows
  .\run.ps1
  #linux
  chmod +x run.sh && ./run.sh
    ```

- `docker运行`
  - 生成配置文件
  ```sh
  cd build && go run build.go compose
  ```
  - 运行容器
  ```
  docker-compose up -d 
  ```

### 参考文献
- [Google File System](https://research.google/pubs/pub51/)
- [Raft](https://raft.github.io/raft.pdf)
- [mit6.824](http://nil.csail.mit.edu/6.824/2021/)