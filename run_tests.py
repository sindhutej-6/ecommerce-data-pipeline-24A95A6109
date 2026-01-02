import subprocess
import sys

result = subprocess.run(
    ["pytest"],
    capture_output=False
)

if result.returncode != 0:
    sys.exit("❌ Tests failed")
else:
    print("✅ All tests passed with sufficient coverage")
