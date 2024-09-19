param (
    [Parameter(Mandatory = $false)]
    [string]$IP = "127.0.0.1",  # IP por defecto
    [Parameter(Mandatory = $false)]
    [int]$Port = 11000  # Puerto por defecto
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
        RequestType = 0;  # SQLSentence
        RequestBody = $command
    }

    $jsonMessage = ConvertTo-Json -InputObject $requestObject -Compress
    Send-Message -client $client -message $jsonMessage
    $response = Receive-Message -client $client

    Write-Host -ForegroundColor Green "Response received: $response"
    
    if ($responseObject.ResponseBody -and $responseObject.ResponseBody -ne "") {
        # Convertir el JSON a un objeto de PowerShell
        $jsonObject = $responseObject.ResponseBody | ConvertFrom-Json

        # Mostrar los resultados en formato de tabla
        $jsonObject | Format-Table -AutoSize
    } else {
        Write-Host "No se encontraron registros o la consulta no produjo resultados." -ForegroundColor Yellow
    }


    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}

function Execute-MyQuery {
    param (
        [string]$QueryFile  # Archivo con las sentencias SQL
    )

    # Lee el contenido del archivo de consultas
    $queries = Get-Content -Path $QueryFile

    $combinedQuery = ""
    $insideCreateTable = $false

    foreach ($query in $queries) {
        if (-not [string]::IsNullOrWhiteSpace($query)) {
            # Detecta si empieza una sentencia CREATE TABLE
            if ($query.Trim().StartsWith("CREATE TABLE", [StringComparison]::OrdinalIgnoreCase)) {
                $insideCreateTable = $true
            }

            if ($insideCreateTable) {
                $combinedQuery += $query + " "

                # Detecta si termina la sentencia CREATE TABLE
                if ($query.Trim().EndsWith(");")) {
                    # Ejecuta toda la sentencia CREATE TABLE como una sola
                    Write-Host "Ejecutando consulta: $combinedQuery" -ForegroundColor Yellow
                    Send-SQLCommand -command $combinedQuery
                    $combinedQuery = ""
                    $insideCreateTable = $false
                }
            } else {
                # Ejecuta las consultas normales (que no son CREATE TABLE)
                Write-Host "Ejecutando consulta: $query" -ForegroundColor Yellow
                Send-SQLCommand -command $query
            }
        }
    }
}

# Ejecutar el archivo que contiene las sentencias SQL
Execute-MyQuery -QueryFile "C:\ruta\al\archivo\script.tinysql"
