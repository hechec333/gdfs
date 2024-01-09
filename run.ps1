function utime {
    Write-Host -NoNewline ("[" + (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + "]")
}

Write-Host ">>> start gdfs clusters configuration"
utime

Write-Host ">>> start running Master-m1"
Start-Process -NoNewWindow "go" -ArgumentList "run cmd/main.go -r m -u 1"

Write-Host ">>> start running Master-m2"
Start-Process -NoNewWindow "go" -ArgumentList "run cmd/main.go -r m -u 2"

Write-Host ">>> start running Master-m3"
Start-Process -NoNewWindow "go" -ArgumentList "run cmd/main.go -r m -u 3"


utime
Start-Sleep -Seconds 1



Write-Host ">>> start running Master-ck1"
Start-Process -NoNewWindow "go" -ArgumentList "run cmd/main.go -r m -u 1"

Write-Host ">>> start running Master-ck2"
Start-Process -NoNewWindow "go" -ArgumentList "run cmd/main.go -r m -u 2"

Write-Host ">>> start running Master-ck3"
Start-Process -NoNewWindow "go" -ArgumentList "run cmd/main.go -r m -u 3"


Write-Host ">>> finish running gdfs clusters"
