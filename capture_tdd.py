"""Capture actual selected test subprocess output and exit code, never fabricate logs."""
import json
from pathlib import Path
import subprocess
import sys
root = Path(__file__).resolve().parent
phase, test = sys.argv[1:]
p = subprocess.run([sys.executable, 'run_tests.py', test], cwd=root, capture_output=True, text=True)
folder = root / 'tdd_logs'; folder.mkdir(exist_ok=True)
(folder / f'{test}-{phase}.log').write_text(p.stdout + p.stderr + f'\nEXIT_CODE={p.returncode}\n')
with (folder / 'cycles.jsonl').open('a') as f:
    f.write(json.dumps({'phase':phase, 'test':test, 'exit_code':p.returncode})+'\n')
print(p.stdout + p.stderr + f'EXIT_CODE={p.returncode}')
sys.exit(0 if (p.returncode != 0 if phase == 'RED' else p.returncode == 0) else 1)
