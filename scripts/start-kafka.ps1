# Start Kafka Broker in KRaft mode (bypasses bat PATH length issue on Windows)
$env:KAFKA_HOME = "C:\Users\kpotf\kafka\kafka_2.13-3.7.0"
$env:JAVA_HOME = if (Test-Path "C:\Program Files\Eclipse Adoptium\jdk-17*") { (Get-Item "C:\Program Files\Eclipse Adoptium\jdk-17*").FullName } else { "C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot" }
$env:PATH = "$env:JAVA_HOME\bin;C:\Windows\system32;C:\Windows"

$classpath = "$env:KAFKA_HOME\libs\*"
$config = "$env:KAFKA_HOME\config\kraft\server.properties"

Write-Host "Starting Kafka Broker (KRaft mode)..."
Write-Host "JAVA_HOME: $env:JAVA_HOME"
java -cp $classpath kafka.Kafka $config
