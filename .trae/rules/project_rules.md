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

## 自动上传规则
**每次代码更新后必须自动执行以下操作：**

1. `git add` 暂存所有变更过的文件（限 `pipeline/`、`colab_loader.ipynb`、`.gitignore`、`TODO.md`、`archive/` 等源码文件，不含 `.md` 使用教程等非代码文件）
2. `git commit -m "中文提交信息"` 提交更改
3. `git push origin master` 推送到远程仓库

### 例外情况
- 如果用户明确要求**不要上传**（如生成使用教程等本地文件），则不执行
- 如果用户说"先..."表示工作尚未完成，则在完成全部工作后再执行

## 回复
- 所有回复必须使用中文撰写

## 代码规范
- 所有代码必须使用中文注释

