<#
.SYNOPSIS
  Spark Cluster Diagnostic Script for Bitnami containers

.DESCRIPTION
  - Validates Docker/container health
  - Checks port connectivity and network setup
  - Tests firewall rules
  - Verifies Java install and Ivy directory
  - Confirms Spark job execution
  - ETL script readiness (via spark_session.py and silver_transformer.py)

.NOTES
  Requires PowerShell 5.1+ (Admin privileges)
#>

# Configuration
$COMPOSE_PROJECT = "medallion_etl_project"
$MASTER = "$COMPOSE_PROJECT-spark-master-1"
$WORKER = "$COMPOSE_PROJECT-spark-worker-1-1"
$SPARK_MASTER_PORT = 7077
$SPARK_MASTER_UI = "http://localhost:8080"
$IVY_PATH = "/opt/bitnami/spark/.ivy2"
$SPARK_PI = "/opt/bitnami/spark/examples/jars/spark-examples_2.12-3.5.1.jar"

function Write-Header($msg) {
    Write-Host "`n===== $msg =====" -ForegroundColor Cyan
}

Write-Header "🧱 Docker Environment Checks"
docker version
docker ps --format "table {{.Names}}\t{{.Status}}"

Write-Header "📡 Network Connectivity"
curl -I $SPARK_MASTER_UI
docker exec $WORKER curl -s -o /dev/null -w "%{http_code}" http://spark-master:8080
docker exec $WORKER bash -c "echo >/dev/tcp/spark-master/$SPARK_MASTER_PORT && echo Port OPEN || echo Port CLOSED"

Write-Header "🔥 Firewall Rules Validation"
Get-NetFirewallRule | Where-Object {
    $_.DisplayName -like "Spark_*"
} | Format-Table DisplayName,Direction,Action,LocalPort,Enabled

Write-Header "☕ Java Version in Containers"
docker exec $MASTER java -version
docker exec $WORKER java -version

Write-Header "📦 Ivy Directory Validation"
docker exec $MASTER ls -l $IVY_PATH
docker exec $MASTER bash -c "test -w '$IVY_PATH' && echo 'Ivy is writable ✅' || echo 'Ivy is not writable ❌'"

Write-Header "🧪 Spark Job Submission Test"
docker exec $MASTER spark-submit --master spark://spark-master:$SPARK_MASTER_PORT `
  --conf spark.jars.ivy=$IVY_PATH `
  --class org.apache.spark.examples.SparkPi `
  $SPARK_PI 10

Write-Header "🔍 Spark Session Configuration Check"
docker exec $MASTER cat /opt/bitnami/spark/scripts/spark_session.py | Select-String -Pattern "spark.jars.ivy", "spark.jars.repositories"

Write-Header "🧬 Silver Transformer ETL Readiness"
docker exec $MASTER python3 /opt/bitnami/spark/scripts/silver_transformer.py

Write-Header "📊 Cluster Web UI Stats"
docker exec $MASTER curl -s http://localhost:8080 | Select-String -Pattern "Alive Workers", "Cores", "Memory"

Write-Host "`n✅ Diagnostics completed. Look for ❌ or warnings. If SparkPi succeeds and Ivy is writable, your ETL workflow is ready to launch!"
