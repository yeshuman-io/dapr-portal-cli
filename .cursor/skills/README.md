# Cursor project skills

Cursor loads project skills from **`.cursor/skills/<name>/`**, while OpenClaw loads **`./skills/<name>/`** at the workspace root.

**Avoid duplicating** [skills/dapr-portal](../../skills/dapr-portal): symlink from here to the repo skill:

```bash
ln -s ../../skills/dapr-portal dapr-portal
```

If symlinks are not available, copy the folder and re-sync manually when `skills/dapr-portal/SKILL.md` changes.
