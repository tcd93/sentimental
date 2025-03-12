# Create a temporary virtual environment
python -m venv venv
.\venv\Scripts\Activate.ps1

# Install dependencies into the python directory
pip install -r requirements.txt -t python/

# Clean up
deactivate
Remove-Item -Recurse -Force venv

# Remove unnecessary files to reduce layer size
Get-ChildItem -Path python -Recurse -Directory | Where-Object { $_.Name -eq "__pycache__" } | Remove-Item -Recurse -Force
Get-ChildItem -Path python -Recurse -Directory | Where-Object { $_.Name -like "*.dist-info" } | Remove-Item -Recurse -Force
Get-ChildItem -Path python -Recurse -Directory | Where-Object { $_.Name -like "*.egg-info" } | Remove-Item -Recurse -Force

# Create ZIP file for manual upload if needed
Compress-Archive -Path python/* -DestinationPath layer.zip -Force 