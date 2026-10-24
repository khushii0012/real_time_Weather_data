# Create weather-data topic (bypasses bat PATH length issue)
$env:KAFKA_HOME = "C:\Users\kpotf\kafka\kafka_2.13-3.7.0"
$env:JAVA_HOME = if (Test-Path "C:\Program Files\Eclipse Adoptium\jdk-17*") { (Get-Item "C:\Program Files\Eclipse Adoptium\jdk-17*").FullName } else { "C:\Program Files\Microsoft\jdk-21.0.12.101-hotspot" }
$env:PATH = "$env:JAVA_HOME\bin;C:\Windows\system32;C:\Windows"

$classpath = "$env:KAFKA_HOME\libs\*"

Write-Host "Creating weather-data topic..."
java -cp $classpath kafka.admin.TopicCommand --create --topic weather-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
Write-Host "Topic created."
