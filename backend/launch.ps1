$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$pythonExe = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
    throw "Copy these dashboard files into your existing C:\Projects\OrderShield project root. Its .venv is required."
}
$nativeRoot = Join-Path $PSScriptRoot "tools\hadoop-3.4.1"
if (Test-Path "$nativeRoot\bin\hadoop.dll") {
    $env:HADOOP_HOME = $nativeRoot
    $env:PATH = "$nativeRoot\bin;$env:PATH"
}
$env:PYSPARK_PYTHON = $pythonExe
$env:PYSPARK_DRIVER_PYTHON = $pythonExe
& $pythonExe -m streamlit run (Join-Path $PSScriptRoot "app.py")
exit $LASTEXITCODE
