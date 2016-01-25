title: 利用 Cloud 9 制作 GitBook 电子书
date: 2016-01-22 13:00:27
tags:
- Cloud 9
- GitBook
categories:
- 技术
---

想用 GitBook 制作电子书而苦于没有环境？
<br> Windows 太坑？
<br> Linux 懒得装？
<br> Docker？这是什么能吃吗？

这里给大家提供一个利用 Cloud 9 IDE 生成 GitBook 电子书的教程。（其实也没有什么好教的，这货就是 Docker + Linux）

## 创建 Cloud 9 workspace

* template 记得选 Node.js。它自带 nvm，这样就不用装 node 了。
* 如果你使用 Git 的话也可以直接写上，它会自动帮你 Clone。

## 配置 GitBook 环境

### 安装 calibre

```text
sudo -v && wget -nv -O- https://raw.githubusercontent.com/kovidgoyal/calibre/master/setup/linux-installer.py | sudo python -c "import sys; main=lambda:sys.stderr.write('Download failed\n'); exec(sys.stdin.read()); main()"
```

### 安装中文字体

Cloud 9 的系统默认木有中文字体的样子，这里我们安装一个文泉驿微黑

> 实践发现不同字体对最终生成的 pdf 文件大小有影响，如果你发现生成的 pdf 过大，尝试换个字体看看。

```text
sudo apt-get update
sudo apt-get install ttf-wqy-microhei
```

### 安装 GitBook & 生成电子书

参考 [gitbook](https://github.com/GitbookIO/gitbook) 中的操作
