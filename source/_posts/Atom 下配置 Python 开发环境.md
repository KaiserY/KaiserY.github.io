title: Atom 下配置 Python 开发环境
date: 2016-04-05 22:14:34
tags:
- Atom
- Python
categories:
- 技术
---

## 自动补全

* [autocomplete-python](https://github.com/sadovnychyi/autocomplete-python)

```bash
apm install autocomplete-python
```

## 代码美化

* [apm install atom-beautify](https://github.com/Glavin001/atom-beautify)

```bash
apm install atom-beautify
```

另外你需要安装一个`Formatter`，推荐 [yapf](https://github.com/google/yapf)。

```bash
https://github.com/google/yapf
```

记得修改下`Atom`插件`atom-beautify`中`Python`的默认`Formatter`。

## 在编辑器中运行脚本

* [script](https://github.com/rgbkrk/atom-script)

> 这个插件还能运行很多其他脚本语言

```bash
apm install script
```

## Linter

* [linter](https://github.com/atom-community/linter)
* [linter-flake8](https://github.com/AtomLinter/linter-flake8)

首先用`pip`安装 [flake8](https://pypi.python.org/pypi/flake8)。

```bash
pip install flake8
pip install flake8-docstrings # 可选的 docstrings 支持
```

之后在`Atom`中安装`linter`插件：

```bash
apm install linter
apm install linter-flake8
```
