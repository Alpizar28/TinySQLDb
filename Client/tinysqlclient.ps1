param (
    [Parameter(Mandatory = $true)]
    [string]$QueryFile,  # Ruta al archivo con las sentencias SQL
    [Parameter(Mandatory = $false)]
    [string]$IP = "127.0.0.1",  # IP por defecto
    [Parameter(Mandatory = $false)]
    [int]$Port = 11000  # Puerto por defecto
)

function Send-Message {
    param (
        [Parameter(Mandatory=$true)]
        [string]$message,
        [Parameter(Mandatory=$true)]
        [System.Net.Sockets.Socket]$client
    )

    $stream = New-Object System.Net.Sockets.NetworkStream($client)
    $writer = New-Object System.IO.StreamWriter($stream)
    try {
        $writer.WriteLine($message)
        $writer.Flush()
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

    $client = New-Object System.Net.Sockets.Socket([System.Net.Sockets.AddressFamily]::InterNetwork, [System.Net.Sockets.SocketType]::Stream, [System.Net.Sockets.ProtocolType]::Tcp)
    $client.Connect($IP, $Port)

    $requestObject = [PSCustomObject]@{
        RequestType = 0;
        RequestBody = $command
    }

    $jsonMessage = $requestObject | ConvertTo-Json -Compress

    # Medir el tiempo de ejecución de la consulta
    $executionTime = Measure-Command {
        Send-Message -client $client -message $jsonMessage
        $response = Receive-Message -client $client
    }

    Write-Host -ForegroundColor Green "Response received: $response"
    Write-Host -ForegroundColor Cyan "Tiempo de ejecución: $($executionTime.TotalSeconds) segundos."

    $responseObject = $response | ConvertFrom-Json
    if ($responseObject.ResponseBody -and $responseObject.ResponseBody -ne "") {
        $jsonObject = $responseObject.ResponseBody | ConvertFrom-Json
        $jsonObject | Format-Table -AutoSize
    } else {
        Write-Host "No se encontraron registros o la consulta no produjo resultados." -ForegroundColor Yellow
    }

    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}


function Execute-MyQuery {
    param (
        [string]$QueryFile
    )

    # Lee el contenido completo del archivo de consultas
    $scriptContent = Get-Content -Path $QueryFile -Raw

    Write-Host "Ejecutando script completo." -ForegroundColor Yellow

    # Medir tiempo total de ejecución del script
    $totalExecutionTime = Measure-Command {
        Send-SQLCommand -command $scriptContent
    }

    Write-Host -ForegroundColor Cyan "Tiempo total de ejecución del script: $($totalExecutionTime.TotalSeconds) segundos."
}

# Ejecutar el archivo que contiene las sentencias SQL
Execute-MyQuery -QueryFile $QueryFile
