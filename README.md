### 摘要  

实时收集、解析Nginx日志（日志文件在不断地增加），需要较好的处理：  

* **断点续读**：agent意外挂掉，重启之后从上次处理过的位置继续处理  
* **日志滚动**：当rename Nginx日志文件，并reload Nginx的时候，需要在*读尽*旧的日志文件之后，从头开始处理新的日志文件  
* **并发计算**：解析日志的时候，需要消耗大量的CPU资源，因此使用进程池，降低因复杂计算而产生的*延迟*  
* **历史日志重放**：当需求方需要历史日志的时候，能够从原始的历史日志文件进行解析，输出解析结果给需求方
* **扩展性良好**：既利用了flume强大的拓扑结构，比如：扇入、扇出等功能。又利用了Python高效的开发效率。同时，可以很方便的自定义parser回调函数  

---

### 架构  

<img alt="架构图" src="http://timd.cn/content/images/pictures/log-parse.png"></img>  

---

### 环境安装  

* 安装Flume  
    * 安装JDK（省略）  
    * 去官网提供的[下载地址](http://flume.apache.org/download.html)下载flume的二进制包  
    * 解压缩：  
        * `tar zxvf apache-flume-1.7.0-bin.tar.gz`  
* 安装Python（2.6+即可，省略）  
* 安装python parser所用到的第三方模块  
    * 安装MySQLdb  
        * `sudo yum install -y mysql-devel MySQL-python`  
    * 安装ua-parser  
        * `sudo easy_install ua-parser`  
    * 安装concurrent futures
        * `sudo easy_install futures`  

---

### 部署  

* 将Flume 1.7.0安装到`/home/gadmin`  
* 从gitlab检出Flume的配置文件 和 相关的python脚本  
    * `cd /home/gadmin`  
    * `git clone http://code.ds.gome.com.cn/gitlab/zhoujingjiang/log-parser.git`  
* 创建相关临时文件和目录
    * Flume file channel相关目录  
        * `mkdir -p /home/gadmin/log-parser/flume_file_channel/checkpointdir`  
        * `mkdir -p /home/gadmin/log-parser/flume_file_channel/datadir`
    * python parser相关文件  
        * `touch /home/gadmin/log-parser/js_sdk.lock.file`  
        * `touch /home/gadmin/log-parser/js_sdk.position.file`  
* 启动Flume  
    * `cp log-parser/flume-conf.properties apache-flume-1.7.0-bin/conf/flume-conf.properties`  
    * `apache-flume-1.7.0-bin/bin/flume-ng agent --conf apache-flume-1.7.0-bin/conf/ --conf-file apache-flume-1.7.0-bin/conf/flume-conf.properties --name tier1 -Dflume.root.logger=INFO,console`  
    * 可以使用screen、nohup等命令将Flume放到后台执行  

---

### 采集机器  

* 10.125.143.121
* 10.125.143.122
* 10.125.143.123

单台单个日志文件200+RPS。

---

### Flume快速入门  

[请移步蒂米的博客](http://timd.cn/2017/09/20/flume/)  