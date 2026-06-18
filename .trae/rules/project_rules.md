# 项目规则

## Git 提交
- git commit 信息必须使用中文撰写

## 上传到 GitHub 的流程
1. **暂存文件**：`git add <文件1> <文件2> ...`
2. **提交更改**：`git commit -m "提交信息（用中文）"`
3. **推送到远程**：`git push origin master`
4. 如果首次推送：`git push -u origin master`

### 完整示例
```powershell
git add pipeline/ runtime_core.py colab_loader.ipynb .gitignore
git commit -m "提交信息"
git push origin master
```

> 注意：当前远程地址为 `https://github.com/collinsgraciano/youtube-aduio-video-creator.git`

## 回复
- 所有回复必须使用中文撰写

## 代码规范
- 所有代码必须使用中文注释

