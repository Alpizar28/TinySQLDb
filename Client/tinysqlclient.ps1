param (
    [Parameter(Mandatory = $false)]
    [string]$IP = "127.0.0.1",  # Asignar IP por defecto
    
    [Parameter(Mandatory = $false)]
    [int]$Port = 11000  # Asignar Puerto por defecto
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
    
    $responseObject = ConvertFrom-Json -InputObject $response
    Write-Output $responseObject
    $client.Shutdown([System.Net.Sockets.SocketShutdown]::Both)
    $client.Close()
}

# Bucle para continuar o salir
do {
    $consulta = Read-Host "Ingresa tu consulta SQL o escribe 'salir' para terminar"

    if ($consulta -eq "salir") {
        Write-Host "Sesión finalizada."
        break
    }

    Send-SQLCommand -command $consulta

} while ($consulta -ne "salir")

Write-Host "Sesión finalizada."

