# Getting Started

### Kafka命令
1. 启动zookeeper

    ```bash
    # ./zkServer.sh start
    ```

2. 检测zookeeper是否启动成功

    ```bash
    # telnet localhost 2181
    ```

3. 启动kafka broker, 默认后台启动，使用配置文件

    ```bash
    # ./kafka-server-start.sh -daemon ../config/server.properties
    ```



