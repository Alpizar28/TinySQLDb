param (
    [Parameter(Mandatory = $true)]
    [string]$IP,
    [Parameter(Mandatory = $true)]
    [int]$Port
)

$ipEndPoint = [System.Net.IPEndPoint]::new([System.Net.IPAddress]::Parse($IP), $Port)

function Send-Message {
    param (
        [Parameter(Mandatory=$true)]
        [pscustomobject]$message,
        [Parameter(Mandatory=$true)]
        [System.Net.Sockets.Socket]$client
    )

    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $writer = New-Object System.IO.StreamWriter($stream)
    try {
        $writer.WriteLine($message)
    }
    finally {
        $writer.Close()
        $stream.Close()
    }
}

function Receive-Message {
    param (
        [System.Net.Sockets.Socket]$client
    )
    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $reader = New-Object System.IO.StreamReader($stream)
    try {
            $line = $reader.ReadLine()
            if ($null -ne $line) {
                return $line
            } else {
                return ""
            }
    }
    finally {
        $reader.Close()
        $stream.Close()
    }
}

function Send-SQLCommand {
    param (
        [string]$command
    )
    $client = New-Object System.Net.Sockets.Socket([System.Net.IPAddress]::Parse($IP).AddressFamily, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)
    $client.Connect($IP, $Port)
    $requestObject = [PSCustomObject]@{
        RequestType = 0;
        RequestBody = $command
    }
    Write-Host -ForegroundColor Green "Sending command: $command"

    $jsonMessage = ConvertTo-Json -InputObject $requestObject -Compress
    Send-Message -client $client -message $jsonMessage
    $response = Receive-Message -client $client

    Write-Host -ForegroundColor Green "Response received: $response"
    
    $responseObject = ConvertFrom-Json -InputObject $response
    Write-Output $responseObject
    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}

function Execute-MyQuery {
    param (
        [string]$QueryFile,
        [int]$Port,
        [string]$IP
    )

    $queries = Get-Content $QueryFile -Raw -ErrorAction Stop | ForEach-Object { $_ -split ";" }

    foreach ($query in $queries) {
        if ($query -ne "") {
            $query = $query.Trim()
            $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()

            $client = New-Object System.Net.Sockets.TcpClient
            $client.Connect($IP, $Port)

            $stream = $client.GetStream()
            $writer = New-Object System.IO.StreamWriter($stream)
            $reader = New-Object System.IO.StreamReader($stream)

            $writer.WriteLine($query)
            $writer.Flush()

            $response = $reader.ReadLine()

            $stopwatch.Stop()
            Write-Host "Consulta: $query"
            Write-Host "Resultado: $response"
            Write-Host "Tiempo: $($stopwatch.ElapsedMilliseconds) ms"

            $writer.Close()
            $reader.Close()
            $client.Close()
        }
    }
}

Execute-MyQuery -QueryFile "C:\Users\Pablo\Downloads\TinySQLDb-main\TinySQLDb-main\script.tinysql" -Port 11000 -IP "127.0.0.1"


# Llamada de prueba
Send-SQLCommand -command "CREATE TABLE ESTUDIANTE"
Send-SQLCommand -command "SELECT * FROM ESTUDIANTE"
