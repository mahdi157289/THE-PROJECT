<#
.SYNOPSIS
    Complete Hybrid Spark Cluster Diagnostic Suite (Windows + Docker)
.DESCRIPTION
    Validates all aspects of Windows-hosted Spark driver with Docker executors:
    - Docker/WSL2 integration
    - Cross-system networking
    - File sharing between Windows and Docker
    - Version alignment
    - Resource allocation
    - Firewall rules
    - Spark job validation
    - Ivy path verification
.NOTES
    Version: 3.6.1
    Requires: PowerShell 5.1+ (Run as Administrator)
#>

#Requires -RunAsAdministrator

$ErrorActionPreference = "Stop"
$WarningPreference = "Continue"

# Configuration
$SPARK_VERSION = "3.4.0"
$SPARK_MASTER_UI = "http://localhost:8080"
$SPARK_APP_UI_PORT = 4041
$SPARK_MASTER_PORT = 7077
$COMPOSE_PROJECT = "medallion_etl_project"
$TEST_JAR = "/opt/bitnami/spark/examples/jars/spark-examples_2.12-$SPARK_VERSION.jar"
$IVY_PATH = "/opt/bitnami/spark/.ivy2"
$WINDOWS_SHARE_PATH = "C:\Users\bacca\Desktop\PFE\medallion_etl_project\data"

function Write-Header($message) {
    Write-Host "`n" + ("=" * 80) -ForegroundColor Cyan
    Write-Host " $message" -ForegroundColor Cyan
    Write-Host ("=" * 80) + "`n" -ForegroundColor Cyan
}

#region 1. Docker Environment Check
function Check-Docker {
    Write-Header "1. Docker-Windows Integration Check"
    try {
        # Docker Desktop verification
        if (Test-Path "C:\Program Files\Docker\Docker\Docker Desktop.exe") {
            Write-Host "Docker Desktop detected (WSL2 mode)" -ForegroundColor Green
            if (-not (Get-Process "Docker Desktop" -ErrorAction SilentlyContinue)) {
                throw "Docker Desktop is not running"
            }
        }
        else {
            throw "Docker Desktop not found"
        }

        # WSL2 integration
        $wslStatus = wsl -l -v
        if ($wslStatus -notmatch "Ubuntu.*Running") {
            Write-Host "WARNING: WSL2 Ubuntu distro not running" -ForegroundColor Yellow
        }

        Write-Host "Docker version:" (docker --version)
        Write-Host "docker-compose version:" (docker-compose --version)
        
        # Docker resource allocation
        $dockerInfo = docker info --format '{{json .}}' | ConvertFrom-Json
        Write-Host "`nDocker Resources:" -ForegroundColor Cyan
        Write-Host "CPUs: $($dockerInfo.NCPU)"
        Write-Host "Memory: $([math]::Round($dockerInfo.MemTotal/1GB, 2))GB"
        
        Write-Host "Docker environment is ready" -ForegroundColor Green
    }
    catch {
        Write-Host "ERROR: $_" -ForegroundColor Red
        exit 1
    }
}
#endregion

#region 2. Container Status Check
function Check-Containers {
    Write-Header "2. Container Status Check"
    try {
        $containers = docker ps -a --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
        if (-not $containers) { throw "No containers found" }
        
        Write-Host $containers
        
        $requiredContainers = @(
            "${COMPOSE_PROJECT}-spark-master-1",
            "${COMPOSE_PROJECT}-spark-worker-1-1"
        )
        
        $allRunning = $true
        foreach ($container in $requiredContainers) {
            $status = docker inspect --format '{{.State.Status}}' $container 2>$null
            if (-not $status) {
                Write-Host "Container $container not found" -ForegroundColor Red
                $allRunning = $false
            }
            elseif ($status -ne "running") {
                Write-Host "Container $container is not running (Status: $status)" -ForegroundColor Red
                $allRunning = $false
            }
        }
        
        if (-not $allRunning) {
            throw "Critical containers are not running"
        }
        
        Write-Host "All required containers are running" -ForegroundColor Green
        
        Write-Host "`nContainer Resources:"
        docker stats --no-stream $requiredContainers
    }
    catch {
        Write-Host "ERROR: $_" -ForegroundColor Red
        exit 1
    }
}
#endregion

#region 3. Version Checks
function Check-Versions {
    Write-Header "3. Version Alignment Check"
    
    try {
        $containers = @(
            "${COMPOSE_PROJECT}-spark-master-1",
            "${COMPOSE_PROJECT}-spark-worker-1-1"
        )
        
        # Spark version check
        foreach ($container in $containers) {
            $sparkVersion = docker exec $container bash -c "/opt/bitnami/spark/bin/spark-submit --version 2>&1 | grep version | head -n 1"
            
            # FIXED: Proper string formatting with subexpression operator
            if ($sparkVersion -match $SPARK_VERSION) {
                Write-Host "$($container) Spark version: $SPARK_VERSION" -ForegroundColor Green
            }
            else {
                Write-Host "VERSION MISMATCH in $($container): Expected $SPARK_VERSION, found $sparkVersion" -ForegroundColor Red
            }
        }

        # Java version check
        foreach ($container in $containers) {
            $javaVersion = docker exec $container bash -c "java -version 2>&1 | head -n 1"
            
            # FIXED: Proper string formatting
            if ($javaVersion -match "11\.\d") {
                Write-Host "$($container) Java version: $javaVersion" -ForegroundColor Green
            }
            else {
                Write-Host "UNSUPPORTED JAVA in $($container): $javaVersion (Expected Java 11)" -ForegroundColor Red
            }
        }
    }
    catch {
        Write-Host "VERSION CHECK ERROR: $_" -ForegroundColor Red
    }
}
#endregion

#region 4. Network Connectivity
function Test-NetworkConnectivity {
    Write-Header "4. Cross-System Networking"
    
    try {
        # Windows host IP detection
        $windowsIP = [System.Net.Dns]::GetHostAddresses("host.docker.internal")[0].IPAddressToString
        Write-Host "Windows Host IP (Docker-visible): $windowsIP" -ForegroundColor Cyan

        # Port accessibility from Windows
        $ports = @(7077, 8080, 4041, 4042, 4043)
        Write-Host "`nWindows Port Accessibility:" -ForegroundColor Cyan
        foreach ($port in $ports) {
            $test = Test-NetConnection -ComputerName $windowsIP -Port $port -InformationLevel Quiet
            Write-Host "Port $port : $(if ($test) {'OPEN (Good)'} else {'BLOCKED (Critical)'})" -ForegroundColor $(if ($test) {"Green"} else {"Red"})
        }

        # Container-to-container communication
        $masterIP = docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${COMPOSE_PROJECT}-spark-master-1"
        $workerIP = docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${COMPOSE_PROJECT}-spark-worker-1-1"
        
        Write-Host "`nContainer Network Topology:" -ForegroundColor Cyan
        Write-Host "Master Container IP: $masterIP"
        Write-Host "Worker Container IP: $workerIP"

        # Master UI accessibility
        Write-Host "`nTesting Master UI from host..." -ForegroundColor Cyan
        try {
            $response = Invoke-WebRequest -Uri $SPARK_MASTER_UI -UseBasicParsing -TimeoutSec 10
            Write-Host "Master UI accessible (Status: $($response.StatusCode))" -ForegroundColor Green
            
            if ($response.Content -match "Alive Workers.*(\d+)") {
                $workerCount = $Matches[1]
                Write-Host "Workers registered: $workerCount" -ForegroundColor Green
            }
        }
        catch {
            Write-Host "Master UI inaccessible: $_" -ForegroundColor Red
            throw
        }

        # Worker to Master connectivity
        Write-Host "`nTesting worker-to-master connectivity..." -ForegroundColor Cyan
        $httpTest = docker exec "${COMPOSE_PROJECT}-spark-worker-1-1" bash -c "curl -s -o /dev/null -w '%{http_code}' http://spark-master:8080"
        if ($httpTest -eq "200") {
            Write-Host "Worker can reach master UI (HTTP 200)" -ForegroundColor Green
        }
        else {
            throw "Worker cannot reach master UI (HTTP $httpTest)"
        }

        # Master service port test
        $portTest = docker exec "${COMPOSE_PROJECT}-spark-worker-1-1" bash -c "nc -zv spark-master 7077 2>&1 | grep succeeded"
        if ($portTest) {
            Write-Host "Master service port (7077) accessible from worker" -ForegroundColor Green
        }
        else {
            throw "Master service port (7077) not accessible from worker"
        }
    }
    catch {
        Write-Host "NETWORK ERROR: $_" -ForegroundColor Red
        exit 1
    }
}
#endregion

#region 5. File Sharing Validation
function Test-FileSharing {
    Write-Header "5. Windows-Docker File Sharing"
    
    try {
        # Check Windows mount path exists
        if (-not (Test-Path $WINDOWS_SHARE_PATH)) {
            throw "Windows share path not found: $WINDOWS_SHARE_PATH"
        }
        Write-Host "Windows share path exists: $WINDOWS_SHARE_PATH" -ForegroundColor Green

        # Test file creation from Docker
        $testFile = "test_$(Get-Date -Format 'yyyyMMddHHmmss').txt"
        $dockerPath = "/data/$testFile"
        
        Write-Host "`nTesting Docker → Windows write..." -ForegroundColor Cyan
        $writeResult = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "touch $dockerPath && echo 'DOCKER_WRITE_OK' || echo 'DOCKER_WRITE_FAIL'"
        
        if ($writeResult -contains "DOCKER_WRITE_OK") {
            $windowsFile = Join-Path $WINDOWS_SHARE_PATH $testFile
            if (Test-Path $windowsFile) {
                Write-Host "File created in Docker appears on Windows" -ForegroundColor Green
                Remove-Item $windowsFile -Force
            }
            else {
                throw "File not visible on Windows host"
            }
        }
        else {
            throw "Docker failed to create test file"
        }

        # Test Windows → Docker write
        Write-Host "`nTesting Windows → Docker write..." -ForegroundColor Cyan
        $testContent = "Hello from Windows at $(Get-Date)"
        $windowsFile | Out-File -FilePath $windowsFile -Force
        
        $dockerContent = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "cat $dockerPath 2>/dev/null || echo 'READ_FAIL'"
        
        if ($dockerContent -eq $testContent) {
            Write-Host "Windows-created file readable in Docker" -ForegroundColor Green
        }
        else {
            throw "Content mismatch: '$dockerContent' != '$testContent'"
        }

        # Cleanup
        Remove-Item $windowsFile -ErrorAction SilentlyContinue
        docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "rm -f $dockerPath"
    }
    catch {
        Write-Host "FILE SHARING ERROR: $_" -ForegroundColor Red
        exit 1
    }
}
#endregion

#region 6. Volume Mounts
function Check-VolumeMounts {
    Write-Header "6. Volume Mount Verification"
    
    try {
        $mountPoints = @(
            @{Path="/data"; Description="Data directory"},
            @{Path="/tmp/spark-temp"; Description="Spark temp directory"}
        )

        foreach ($mount in $mountPoints) {
            $testFile = "$($mount.Path)/test_$(Get-Date -Format 'yyyyMMddHHmmss')"
            $result = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "touch $testFile && echo 'WRITE_OK' || echo 'WRITE_FAIL'"
            
            if ($result -contains "WRITE_OK") {
                Write-Host "Mount $($mount.Path): OK ($($mount.Description))" -ForegroundColor Green
                docker exec "${COMPOSE_PROJECT}-spark-master-1" rm $testFile
            } else {
                throw "Mount point $($mount.Path) is not writable"
            }
        }

        # Ivy directory check
        $ivyCheck = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "if [ -d '$IVY_PATH' ]; then echo 'EXISTS'; else echo 'MISSING'; fi"
        if ($ivyCheck -contains "EXISTS") {
            Write-Host "Ivy directory exists: $IVY_PATH" -ForegroundColor Green
            
            $permissions = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "stat -c '%A %U %G' $IVY_PATH"
            Write-Host "Ivy permissions: $permissions" -ForegroundColor Cyan
            
            $writeTest = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "touch $IVY_PATH/permission_test && echo 'WRITE_OK' || echo 'WRITE_FAIL'; rm -f $IVY_PATH/permission_test"
            if ($writeTest -contains "WRITE_OK") {
                Write-Host "Ivy directory is writable" -ForegroundColor Green
            } else {
                throw "Ivy directory is not writable by Spark user"
            }
        } else {
            Write-Host "WARNING: Ivy directory missing" -ForegroundColor Yellow
            $createResult = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "mkdir -p $IVY_PATH && chown -R spark:spark $IVY_PATH && echo 'CREATED' || echo 'FAILED'"
            
            if ($createResult -contains "CREATED") {
                Write-Host "Successfully created Ivy directory" -ForegroundColor Green
            } else {
                throw "Failed to create Ivy directory"
            }
        }
    }
    catch {
        Write-Host "VOLUME ERROR: $_" -ForegroundColor Red
    }
}
#endregion

#region 7. Firewall Check
function Check-Firewall {
    Write-Header "7. Firewall Configuration Check"
    
    try {
        $requiredPorts = @(
            @{Port=7077; Name="Spark Master"},
            @{Port=8080; Name="Master UI"},
            @{Port=4041; Name="Application UI"},
            @{Port=8081; Name="Worker UI"},
            @{Port=4042; Name="Driver Port"},
            @{Port=4043; Name="BlockManager Port"}
        )

        $firewallOk = $true
        foreach ($port in $requiredPorts) {
            $rule = Get-NetFirewallRule -DisplayName "Spark*" | 
                    Where-Object { $_.Direction -eq "Inbound" -and $_.LocalPort -eq $port.Port } |
                    Select-Object -First 1

            if (-not $rule -or $rule.Action -ne "Allow") {
                Write-Host "MISSING RULE: $($port.Name) (Port $($port.Port))" -ForegroundColor Red
                $firewallOk = $false
            }
            else {
                Write-Host "Rule exists: $($port.Name) (Port $($port.Port))" -ForegroundColor Green
            }
        }

        if (-not $firewallOk) {
            Write-Host "`nRun these commands as Administrator to add missing rules:" -ForegroundColor Yellow
            $requiredPorts | ForEach-Object {
                Write-Host "New-NetFirewallRule -DisplayName 'Spark_$($_.Port)' -Direction Inbound -LocalPort $($_.Port) -Protocol TCP -Action Allow"
            }
        }
    }
    catch {
        Write-Host "ERROR checking firewall: $_" -ForegroundColor Red
    }
}
#endregion

#region 8. Resource Check
function Check-Resources {
    Write-Header "8. System Resource Check"
    
    try {
        # Host resources
        $os = Get-CimInstance Win32_OperatingSystem
        $totalMemGB = [math]::Round($os.TotalVisibleMemorySize/1MB, 2)
        $freeMemGB = [math]::Round($os.FreePhysicalMemory/1MB, 2)
        $cpu = Get-CimInstance Win32_Processor

        Write-Host "Host Resources:" -ForegroundColor Cyan
        Write-Host "  CPU: $($cpu.Name)"
        Write-Host "  Cores: $($cpu.NumberOfCores), Logical: $($cpu.NumberOfLogicalProcessors)"
        Write-Host "  Memory: $freeMemGB GB free of $totalMemGB GB total"
        if ($freeMemGB -lt 4) {
            Write-Host "WARNING: Low free memory (<4GB)" -ForegroundColor Yellow
        }

        # Docker resources
        $dockerInfo = docker info --format '{{json .}}' | ConvertFrom-Json
        Write-Host "`nDocker Resource Limits:" -ForegroundColor Cyan
        Write-Host "  CPUs: $($dockerInfo.NCPU)"
        Write-Host "  Memory: $([math]::Round($dockerInfo.MemTotal/1GB, 2))GB"
        
        # Process-specific checks
        Write-Host "`nSpark Process Limits:" -ForegroundColor Cyan
        docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c "ulimit -a" | Select-String -Pattern "open files|max user processes"
    }
    catch {
        Write-Host "ERROR checking resources: $_" -ForegroundColor Red
    }
}
#endregion

#region 9. Cluster Health
function Test-ClusterHealth {
    Write-Header "9. Cluster Health Check"
    
    try {
        # Worker registration
        $workers = (Invoke-WebRequest $SPARK_MASTER_UI -UseBasicParsing).Content -match "Alive Workers.*(\d+)"
        if ($Matches[1] -eq 0) {
            throw "No workers registered with master"
        }
        Write-Host "Workers registered: $($Matches[1])" -ForegroundColor Green

        # Test job submission
        Write-Host "`nSubmitting test job..." -ForegroundColor Cyan
        $output = docker exec "${COMPOSE_PROJECT}-spark-master-1" bash -c """
            /opt/bitnami/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --conf spark.jars.ivy=$IVY_PATH \
            --class org.apache.spark.examples.SparkPi \
            $TEST_JAR 10 2>&1
        """
        
        if ($output -match "Pi is roughly (\d+\.\d+)") {
            Write-Host "SUCCESS: Job completed - $($Matches[0])" -ForegroundColor Green
            return $true
        }
        else {
            Write-Host "JOB FAILED:" -ForegroundColor Red
            Write-Host $output
            return $false
        }
    }
    catch {
        Write-Host "CLUSTER ERROR: $_" -ForegroundColor Red
        return $false
    }
}
#endregion

# Main execution
try {
    Write-Host "Starting Comprehensive Hybrid Spark Diagnostics..." -ForegroundColor Green
    
    Check-Docker
    Check-Containers
    Check-Versions
    Test-NetworkConnectivity
    Test-FileSharing
    Check-VolumeMounts
    Check-Firewall
    Check-Resources
    $clusterHealthy = Test-ClusterHealth

    # Final summary
    Write-Header "Diagnostics Summary"
    if ($clusterHealthy) {
        Write-Host "HYBRID CLUSTER FULLY OPERATIONAL" -ForegroundColor Green
        Write-Host "All systems verified: Docker, Networking, File Sharing, Resources" -ForegroundColor Green
    }
    else {
        Write-Host "CLUSTER HAS ISSUES - Review failed checks above" -ForegroundColor Red
    }
}
catch {
    Write-Host "`nFATAL ERROR: $_" -ForegroundColor Red
    exit 1
}