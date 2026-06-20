# Self-contained PySpark environment setup for this project (Windows).
# Uses the conda env that bundles Python 3.11 + Java + PySpark 3.5.1,
# plus the winutils/hadoop binaries and spark-conf inside THIS repo.
#
# Usage (run from the project root, in PowerShell):
#   . .\setup-pyspark.ps1        # note the leading "dot space" so env vars persist
#   & $env:PYSPARK_PYTHON verify_spark.py

param(
    [string]$EnvPath = "C:\conda-envs\pyspark-3.5.1"
)

Write-Host "Configuring self-contained PySpark environment..." -ForegroundColor Cyan

$ProjRoot = if ($PSScriptRoot) { $PSScriptRoot } else { (Get-Location).Path }

$PythonExe   = Join-Path $EnvPath "python.exe"
$JavaHome    = Join-Path $EnvPath "Library"
$SparkHome   = Join-Path $EnvPath "Lib\site-packages\pyspark"

$HadoopHome  = Join-Path $ProjRoot "hadoop"
$SparkConf   = Join-Path $ProjRoot "spark-conf"
$SparkTmp    = Join-Path $ProjRoot ".spark-tmp"

foreach ($p in @($PythonExe, (Join-Path $JavaHome "bin\java.exe"), $SparkHome,
                 (Join-Path $HadoopHome "bin\winutils.exe"))) {
    if (-not (Test-Path $p)) { Write-Host "ERROR: missing $p" -ForegroundColor Red; return }
}

New-Item -ItemType Directory -Force $SparkTmp  | Out-Null
New-Item -ItemType Directory -Force $SparkConf | Out-Null

# Generate spark-defaults.conf with forward slashes (Windows-safe).
$pyC  = $PythonExe -replace "\\","/"
$tmpC = $SparkTmp  -replace "\\","/"
$hadC = $HadoopHome -replace "\\","/"
@"
spark.local.dir $tmpC
spark.driver.extraJavaOptions -Djava.io.tmpdir=$tmpC -Dhadoop.home.dir=$hadC
spark.executor.extraJavaOptions -Djava.io.tmpdir=$tmpC -Dhadoop.home.dir=$hadC
spark.pyspark.python $pyC
spark.pyspark.driver.python $pyC
"@ | Set-Content -Path (Join-Path $SparkConf "spark-defaults.conf") -Encoding ASCII

# Environment variables for this PowerShell session.
$env:JAVA_HOME            = $JavaHome
$env:SPARK_HOME           = $SparkHome
$env:SPARK_CONF_DIR       = $SparkConf
$env:HADOOP_HOME          = $HadoopHome
$env:PYSPARK_PYTHON       = $PythonExe
$env:PYSPARK_DRIVER_PYTHON= $PythonExe
$env:SPARK_LOCAL_DIRS     = $SparkTmp
$env:Path = "$SparkHome\bin;$JavaHome\bin;$HadoopHome\bin;$EnvPath;$EnvPath\Scripts;$env:Path"

Write-Host "PySpark environment ready." -ForegroundColor Green
Write-Host "  PYSPARK_PYTHON: $env:PYSPARK_PYTHON"
Write-Host "  JAVA_HOME:      $env:JAVA_HOME"
Write-Host "  HADOOP_HOME:    $env:HADOOP_HOME"
Write-Host ""
Write-Host "Next: & `$env:PYSPARK_PYTHON verify_spark.py"
