param(
    [string]$RemoteHost = "andrei@100.96.0.3",
    [string]$RemoteRoot = "C:\Users\Andrei\ci-runners",
    [ValidateSet("Auto", "BuildKit", "Legacy")]
    [string]$BuilderMode = "Auto",
    [string]$ImageTagPrefix = "investintell-railway-ci",
    [switch]$AllowDirty,
    [int]$DockerReadyTimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

function Invoke-Git {
    param([Parameter(ValueFromRemainingArguments = $true)][string[]]$Args)
    $output = & git @Args
    if ($LASTEXITCODE -ne 0) {
        throw "git $($Args -join ' ') failed with exit code $LASTEXITCODE"
    }
    return $output
}

function Convert-ToRemoteScpPath {
    param([string]$Path)
    return ($Path -replace "\\", "/")
}

$repoRoot = (Invoke-Git rev-parse --show-toplevel).Trim()
Set-Location $repoRoot

$sha = (Invoke-Git rev-parse HEAD).Trim()
$shortSha = $sha.Substring(0, 7)
$branch = (Invoke-Git rev-parse --abbrev-ref HEAD).Trim()

if (-not $AllowDirty) {
    $dirty = & git status --porcelain
    if ($dirty) {
        throw "Working tree is dirty. Commit or stash changes before remote CI, or pass -AllowDirty to bypass this check."
    }
}

$remoteArchiveDir = Join-Path $RemoteRoot "archives"
$remoteWorkDir = Join-Path $RemoteRoot ("investintell-datalake-workers-" + $shortSha)
$remoteZip = Join-Path $remoteArchiveDir ("investintell-datalake-workers-" + $shortSha + ".zip")
$localZip = Join-Path ([System.IO.Path]::GetTempPath()) ("investintell-datalake-workers-" + $shortSha + ".zip")

if (Test-Path $localZip) {
    Remove-Item -LiteralPath $localZip -Force
}

Invoke-Git archive "--format=zip" "-o" $localZip $sha | Out-Null

$prepareRemote = @"
`$ErrorActionPreference = 'Stop'
New-Item -ItemType Directory -Force -Path '$remoteArchiveDir' | Out-Null
New-Item -ItemType Directory -Force -Path '$RemoteRoot' | Out-Null
"@
$prepareEncoded = [Convert]::ToBase64String([Text.Encoding]::Unicode.GetBytes($prepareRemote))
& ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new $RemoteHost "powershell -NoProfile -EncodedCommand $prepareEncoded"
if ($LASTEXITCODE -ne 0) {
    throw "Remote preparation failed on $RemoteHost"
}

$scpTarget = $RemoteHost + ":" + (Convert-ToRemoteScpPath $remoteZip)
& scp -q -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new $localZip $scpTarget
if ($LASTEXITCODE -ne 0) {
    throw "Failed to copy archive to $RemoteHost"
}

$remoteScript = @"
`$ErrorActionPreference = 'Stop'
`$ProgressPreference = 'SilentlyContinue'
`$sha = '$sha'
`$shortSha = '$shortSha'
`$branch = '$branch'
`$remoteZip = '$remoteZip'
`$workDir = '$remoteWorkDir'
`$builderMode = '$BuilderMode'
`$imageTag = '$ImageTagPrefix' + ':' + `$shortSha
`$dockerReadyTimeoutSeconds = $DockerReadyTimeoutSeconds

function Wait-DockerReady {
    param([int]`$TimeoutSeconds)
    `$deadline = [DateTime]::UtcNow.AddSeconds(`$TimeoutSeconds)
    while ([DateTime]::UtcNow -lt `$deadline) {
        `$info = docker info --format 'Server={{.ServerVersion}} CPUs={{.NCPU}} Mem={{.MemTotal}}' 2>`$null
        if (`$LASTEXITCODE -eq 0 -and `$info) {
            Write-Output ('REMOTE_DOCKER_INFO ' + `$info)
            return
        }
        `$dockerDesktop = 'C:\Program Files\Docker\Docker\Docker Desktop.exe'
        if (Test-Path `$dockerDesktop) {
            Start-Process -FilePath `$dockerDesktop -WindowStyle Hidden | Out-Null
        }
        Start-Sleep -Seconds 5
    }
    throw 'Docker did not become ready before timeout.'
}

function Invoke-DockerBuild {
    param(
        [string]`$Mode,
        [string]`$LogPath
    )
    if (`$Mode -eq 'Legacy') {
        `$env:DOCKER_BUILDKIT = '0'
    } else {
        Remove-Item Env:\DOCKER_BUILDKIT -ErrorAction SilentlyContinue
    }
    `$previousErrorActionPreference = `$ErrorActionPreference
    `$ErrorActionPreference = 'Continue'
    try {
        docker build -f docker/railway-ci/Dockerfile -t `$imageTag . 2>&1 |
            Tee-Object -FilePath `$LogPath |
            Out-Null
        `$dockerExit = `$LASTEXITCODE
        return [int]`$dockerExit
    } finally {
        `$ErrorActionPreference = `$previousErrorActionPreference
    }
}

if (Test-Path `$workDir) {
    Remove-Item -LiteralPath `$workDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path `$workDir | Out-Null
Expand-Archive -LiteralPath `$remoteZip -DestinationPath `$workDir -Force
Set-Location `$workDir

`$actualFiles = (Get-ChildItem -Recurse -File | Measure-Object).Count
Write-Output "REMOTE_CI_HOST=`$env:COMPUTERNAME"
Write-Output "REMOTE_CI_SHA=`$sha"
Write-Output "REMOTE_CI_BRANCH=`$branch"
Write-Output "REMOTE_CI_WORKDIR=`$workDir"
Write-Output "REMOTE_CI_FILES=`$actualFiles"

Wait-DockerReady -TimeoutSeconds `$dockerReadyTimeoutSeconds

`$logDir = Join-Path `$workDir 'ci-logs'
New-Item -ItemType Directory -Force -Path `$logDir | Out-Null
`$sw = [Diagnostics.Stopwatch]::StartNew()

`$selectedMode = `$builderMode
`$selectedLog = `$null
`$exitCode = 1
if (`$builderMode -eq 'Auto' -or `$builderMode -eq 'BuildKit') {
    `$buildKitLog = Join-Path `$logDir ('docker-build-buildkit-' + `$shortSha + '.log')
    `$exitCode = Invoke-DockerBuild -Mode 'BuildKit' -LogPath `$buildKitLog
    `$selectedMode = 'BuildKit'
    `$selectedLog = `$buildKitLog
    if (`$exitCode -ne 0 -and `$builderMode -eq 'Auto') {
        `$logText = Get-Content -Raw -LiteralPath `$buildKitLog -ErrorAction SilentlyContinue
        if (`$logText -match 'error getting credentials|specified logon session|credsStore') {
            Write-Output 'REMOTE_CI_BUILDKIT_CREDENTIAL_HELPER_FALLBACK=true'
            `$legacyLog = Join-Path `$logDir ('docker-build-legacy-' + `$shortSha + '.log')
            `$exitCode = Invoke-DockerBuild -Mode 'Legacy' -LogPath `$legacyLog
            `$selectedMode = 'Legacy'
            `$selectedLog = `$legacyLog
        }
    }
} else {
    `$legacyLog = Join-Path `$logDir ('docker-build-legacy-' + `$shortSha + '.log')
    `$exitCode = Invoke-DockerBuild -Mode 'Legacy' -LogPath `$legacyLog
    `$selectedMode = 'Legacy'
    `$selectedLog = `$legacyLog
}

`$sw.Stop()
Write-Output "REMOTE_CI_BUILDER=`$selectedMode"
Write-Output "REMOTE_CI_IMAGE=`$imageTag"
Write-Output "REMOTE_CI_SECONDS=`$([math]::Round(`$sw.Elapsed.TotalSeconds, 2))"
Write-Output "REMOTE_CI_EXIT=`$exitCode"
if (`$exitCode -ne 0) {
    Write-Output "REMOTE_CI_LOG=`$selectedLog"
    Get-Content -LiteralPath `$selectedLog -Tail 120 -ErrorAction SilentlyContinue |
        ForEach-Object { Write-Output "REMOTE_CI_LOG_TAIL: `$_" }
    exit `$exitCode
}
"@

$remoteCommand = "powershell -NoLogo -NoProfile -NonInteractive -ExecutionPolicy Bypass -OutputFormat Text -Command -"
$sshOutput = $remoteScript | & ssh -o BatchMode=yes -o ConnectTimeout=10 -o StrictHostKeyChecking=accept-new $RemoteHost $remoteCommand 2>&1
$sshExit = $LASTEXITCODE
$sshText = ($sshOutput | ForEach-Object { [string]$_ }) -join [Environment]::NewLine
$sshText -split "`r?`n" |
    Where-Object {
        $line = [string]$_
        $line -notmatch '^#< CLIXML' -and $line -notmatch '^<Objs Version='
    } |
    ForEach-Object { Write-Output $_ }
if ($sshExit -ne 0) {
    throw "Remote Railway CI failed on $RemoteHost for $sha"
}
Write-Output "REMOTE_CI_STATUS=PASS"
