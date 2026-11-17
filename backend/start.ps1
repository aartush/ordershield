param(
    [ValidateSet("api","consumer","producer","dashboard","kafka")]
    [string]$Service = "api",
    [int]$Limit = 30
)
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$python = "$PSScriptRoot\.venv\Scripts\python.exe"
$native = "$PSScriptRoot\tools\hadoop-3.4.1"
$env:PYSPARK_PYTHON = $python
$env:PYSPARK_DRIVER_PYTHON = $python
if (Test-Path "$native\bin\hadoop.dll") {
    $env:HADOOP_HOME = $native
    $env:PATH = "$native\bin;$env:PATH"
}
switch ($Service) {
    "api" {
        & $python -m uvicorn main:app --host 127.0.0.1 --port 8000 --workers 1
    }
    "consumer" { & $python src\kafka_consumer.py }
    "producer" { & $python src\kafka_producer.py --limit $Limit --interval 1 }
    "dashboard" { & $python -m streamlit run app.py }
    "kafka" {
        docker compose -p ordershield -f compose.kafka.yaml up -d --wait
    }
}
exit $LASTEXITCODE