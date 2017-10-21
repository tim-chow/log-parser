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

### 特性  

不仅支持实时日志收集，还支持断点续读、历史日志重放。  

---

### 采集机器  

* 10.125.143.121
* 10.125.143.122
* 10.125.143.123
