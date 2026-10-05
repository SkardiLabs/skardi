# Tests for install.ps1. Each case installs a local release-shaped zip into a
# fresh temp directory with -NoModifyPath, so nothing is downloaded and the
# user PATH in the registry is never touched.
#
#   pwsh ./scripts/install_test.ps1
#
# Uses target\debug\skardi.exe as the payload when it exists (CI builds it
# first), so the installed binary is also run; otherwise a stand-in file.

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = Split-Path -Parent $PSScriptRoot
$install = Join-Path $root 'install.ps1'
$work = Join-Path ([IO.Path]::GetTempPath()) ("skardi-install-test-" + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work | Out-Null

$script:fails = 0
function Pass($name) { Write-Host "ok   $name" }
function Fail($name, $why) { Write-Host "FAIL $name $why"; $script:fails++ }

# A zip shaped like a release asset: skardi.exe at the root, and a .sha256
# next to it in shasum's "<hash>  <file>" format.
function New-ReleaseZip {
    param([string]$Name, [string]$Payload, [string]$EntryName = 'skardi.exe', [string]$Hash)
    $stage = Join-Path $work "stage-$Name"
    New-Item -ItemType Directory -Path $stage | Out-Null
    Copy-Item -LiteralPath $Payload -Destination (Join-Path $stage $EntryName)
    $zip = Join-Path $work "$Name.zip"
    Compress-Archive -Path (Join-Path $stage $EntryName) -DestinationPath $zip
    if (-not $Hash) { $Hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $zip).Hash.ToLower() }
    [IO.File]::WriteAllText("$zip.sha256", "$Hash  skardi-x86_64-pc-windows-msvc.zip`n")
    return $zip
}

# Run install.ps1 against ZIP into DIR; returns the error message, or $null.
function Invoke-Install {
    param([string]$Zip, [string]$Dir)
    $env:SKARDI_INSTALL_ZIP = $Zip
    try {
        & $install -InstallDir $Dir -NoModifyPath *> $null
        return $null
    } catch {
        return $_.Exception.Message
    } finally {
        Remove-Item Env:\SKARDI_INSTALL_ZIP -ErrorAction SilentlyContinue
    }
}

try {
    $real = Join-Path $root 'target\debug\skardi.exe'
    if (Test-Path -LiteralPath $real) {
        $payload = $real
    } else {
        $payload = Join-Path $work 'fake-skardi.exe'
        [IO.File]::WriteAllText($payload, 'not a real binary')
    }
    $payloadHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $payload).Hash

    # Installs, and the file is the one in the zip.
    $good = New-ReleaseZip -Name 'good' -Payload $payload
    $dir = Join-Path $work 'bin-good'
    $err = Invoke-Install -Zip $good -Dir $dir
    $dest = Join-Path $dir 'skardi.exe'
    if ($err) { Fail 'installs from a release zip' $err }
    elseif (-not (Test-Path -LiteralPath $dest)) { Fail 'installs from a release zip' 'no skardi.exe' }
    elseif ((Get-FileHash -Algorithm SHA256 -LiteralPath $dest).Hash -ne $payloadHash) { Fail 'installs from a release zip' 'wrong bytes' }
    else { Pass 'installs from a release zip' }

    if ($payload -eq $real) {
        $version = & $dest --version
        if ($LASTEXITCODE -eq 0 -and $version -match '^skardi ') { Pass 'the installed skardi.exe runs' }
        else { Fail 'the installed skardi.exe runs' "exit $LASTEXITCODE, output: $version" }
    }

    # Re-running over an existing install replaces it.
    $err = Invoke-Install -Zip $good -Dir $dir
    if ($err) { Fail 're-running upgrades in place' $err } else { Pass 're-running upgrades in place' }

    # A checksum that does not match is refused and nothing is installed.
    $bad = New-ReleaseZip -Name 'badsum' -Payload $payload -Hash ('0' * 64)
    $dir = Join-Path $work 'bin-badsum'
    $err = Invoke-Install -Zip $bad -Dir $dir
    if ($err -notmatch 'checksum mismatch') { Fail 'refuses a checksum mismatch' "got: $err" }
    elseif (Test-Path -LiteralPath (Join-Path $dir 'skardi.exe')) { Fail 'refuses a checksum mismatch' 'installed anyway' }
    else { Pass 'refuses a checksum mismatch' }

    # An archive without skardi.exe is refused by name.
    $empty = New-ReleaseZip -Name 'noexe' -Payload $payload -EntryName 'other.exe'
    $err = Invoke-Install -Zip $empty -Dir (Join-Path $work 'bin-noexe')
    if ($err -match 'no skardi.exe') { Pass 'refuses an archive without skardi.exe' }
    else { Fail 'refuses an archive without skardi.exe' "got: $err" }
} finally {
    Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:fails -gt 0) {
    Write-Host "$($script:fails) failed"
    exit 1
}
Write-Host 'all passed'
