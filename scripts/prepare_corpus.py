import os
import subprocess
from pathlib import Path

repo_path = r"C:\Users\ansh\Documents\Github\AlgoMLN"
out_dir = Path("corpus")
out_dir.mkdir(exist_ok=True)

def make_corpus(commit, out_name):
    out_path = out_dir / out_name
    cmd = ["git", "-C", repo_path, "ls-tree", "-r", "--name-only", commit]
    res = subprocess.run(cmd, capture_output=True, text=True, check=True)
    files = sorted([f.strip() for f in res.stdout.splitlines() if f.strip().endswith(".rs")])
    print(f"Commit {commit[:8]} has {len(files)} .rs files")
    
    total_bytes = 0
    with open(out_path, "wb") as out_f:
        for f in files:
            file_cmd = ["git", "-C", repo_path, "cat-file", "-p", f"{commit}:{f}"]
            file_res = subprocess.run(file_cmd, capture_output=True, check=True)
            total_bytes += len(file_res.stdout)
            out_f.write(file_res.stdout)
            
    print(f"Wrote {total_bytes} bytes to {out_path} (exact size: {out_path.stat().st_size})")

make_corpus("5d5624a97885a5fc99919b94c4e32a1855104f13", "algomln0.rs")
make_corpus("440b2ac947eb6998e417973165072c0499e29536", "algomln2.rs")
