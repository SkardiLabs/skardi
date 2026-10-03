# Install the skardi CLI on Windows.
#
#   irm https://raw.githubusercontent.com/SkardiLabs/skardi/main/install.ps1 | iex
#
# Downloads the latest release's skardi.exe, checks its SHA-256, puts it in
# %LOCALAPPDATA%\Programs\skardi\bin and adds that directory to your user
# PATH. No administrator rights needed. Re-running it upgrades in place.
#
# To pass options, run the script as a script block:
#
#   & ([scriptblock]::Create((irm https://raw.githubusercontent.com/SkardiLabs/skardi/main/install.ps1))) -NoModifyPath
#
# Options:
#   -InstallDir DIR   where skardi.exe goes
#                     (default %LOCALAPPDATA%\Programs\skardi\bin)
#   -NoModifyPath     do not add the install directory to the user PATH
#
# Environment:
#   SKARDI_INSTALL_DIR   same as -InstallDir
#   SKARDI_INSTALL_ZIP   install from a local release zip instead of
#                        downloading one (used by the tests); its checksum is
#                        read from the .sha256 next to it when there is one
#
# This sets up the CLI only. The agent setup install.sh does (skills and MCP
# config for Claude Code, Codex and Cursor) is described by hand in
# docs/mcp.md; on Windows the MCP command is `skardi mcp` as on any platform.

[CmdletBinding()]
param(
    [string]$InstallDir,
    [switch]$NoModifyPath
)

# Everything runs inside a function: `irm | iex` evaluates this script in the
# CALLER's scope, so a top-level $ErrorActionPreference would leak into their
# session, and `exit` would close their PowerShell window.
function Install-Skardi {
    param([string]$InstallDir, [bool]$ModifyPath)

    $ErrorActionPreference = 'Stop'
    # Invoke-WebRequest's progress bar slows downloads severalfold on 5.1.
    $ProgressPreference = 'SilentlyContinue'

    $repo = 'SkardiLabs/skardi'
    $target = 'x86_64-pc-windows-msvc'

    if (-not $InstallDir) {
        if ($env:SKARDI_INSTALL_DIR) {
            $InstallDir = $env:SKARDI_INSTALL_DIR
        } else {
            $InstallDir = Join-Path $env:LOCALAPPDATA 'Programs\skardi\bin'
        }
    }

    # 32-bit PowerShell on 64-bit Windows reports x86 here and the real
    # architecture in PROCESSOR_ARCHITEW6432.
    $arch = $env:PROCESSOR_ARCHITEW6432
    if (-not $arch) { $arch = $env:PROCESSOR_ARCHITECTURE }
    switch ($arch) {
        'AMD64' { }
        'ARM64' { Write-Host 'No native ARM64 build yet; installing the x64 skardi, which Windows runs under emulation.' }
        default { throw "no pre-built skardi for $arch Windows; build from source: https://github.com/$repo#install" }
    }

    $tmp = Join-Path ([IO.Path]::GetTempPath()) ("skardi-install-" + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $tmp | Out-Null
    try {
        $zip = Join-Path $tmp "skardi-$target.zip"
        $sumFile = "$zip.sha256"
        $haveSum = $false

        if ($env:SKARDI_INSTALL_ZIP) {
            Copy-Item -LiteralPath $env:SKARDI_INSTALL_ZIP -Destination $zip
            if (Test-Path -LiteralPath "$($env:SKARDI_INSTALL_ZIP).sha256") {
                Copy-Item -LiteralPath "$($env:SKARDI_INSTALL_ZIP).sha256" -Destination $sumFile
                $haveSum = $true
            }
        } else {
            # Windows PowerShell 5.1 may still default to TLS 1.0, which
            # GitHub refuses.
            [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
            $url = "https://github.com/$repo/releases/latest/download/skardi-$target.zip"
            Write-Host "Downloading skardi for $target"
            try {
                Invoke-WebRequest -Uri $url -OutFile $zip -UseBasicParsing
            } catch {
                throw "download failed: $url ($($_.Exception.Message))"
            }
            # Same rule as install.sh: verify whenever the release publishes a
            # checksum, which every release does.
            try {
                Invoke-WebRequest -Uri "$url.sha256" -OutFile $sumFile -UseBasicParsing
                $haveSum = $true
            } catch { }
        }

        if ($haveSum) {
            $want = ((Get-Content -LiteralPath $sumFile -Raw).Trim() -split '\s+')[0].ToLower()
            $got = (Get-FileHash -Algorithm SHA256 -LiteralPath $zip).Hash.ToLower()
            if ($want -ne $got) { throw "checksum mismatch for skardi-$target.zip" }
        }

        $unpacked = Join-Path $tmp 'unpacked'
        Expand-Archive -LiteralPath $zip -DestinationPath $unpacked
        $exe = Join-Path $unpacked 'skardi.exe'
        if (-not (Test-Path -LiteralPath $exe)) { throw 'the archive has no skardi.exe' }

        New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
        $dest = Join-Path $InstallDir 'skardi.exe'
        try {
            Move-Item -LiteralPath $exe -Destination $dest -Force
        } catch {
            # Windows will not replace a running executable, and an agent that
            # spawned `skardi mcp` keeps it running.
            throw "could not replace $dest; if skardi is running (an agent's MCP server, say), stop it and run this again. ($($_.Exception.Message))"
        }
        Write-Host "Installed $dest"
    } finally {
        Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
    }

    if ($ModifyPath) {
        Add-ToUserPath $InstallDir
    }
    $onPath = ($env:Path -split ';' | Where-Object { $_.TrimEnd('\') -ieq $InstallDir.TrimEnd('\') }).Count -gt 0
    if (-not $onPath) {
        Write-Warning "$InstallDir is not on your PATH"
    }
}

# Append DIR to the user PATH in the registry, and to this session's PATH so
# skardi runs without opening a new terminal.
function Add-ToUserPath {
    param([string]$Dir)

    $key = Get-Item -Path 'HKCU:\Environment'
    # Read unexpanded and write back as REG_EXPAND_SZ: going through
    # [Environment]::SetEnvironmentVariable would expand %USERPROFILE%-style
    # entries the user already has into fixed paths.
    $current = $key.GetValue('Path', '', 'DoNotExpandEnvironmentNames')
    $entries = @($current -split ';' | Where-Object { $_ })
    $present = $entries | Where-Object {
        [Environment]::ExpandEnvironmentVariables($_).TrimEnd('\') -ieq $Dir.TrimEnd('\')
    }
    if (-not $present) {
        $updated = (@($entries) + $Dir) -join ';'
        Set-ItemProperty -Path 'HKCU:\Environment' -Name 'Path' -Value $updated -Type ExpandString
        # Tell running programs (Explorer, new terminals) the environment
        # changed: setting any user variable through .NET broadcasts
        # WM_SETTINGCHANGE.
        [Environment]::SetEnvironmentVariable('SKARDI_INSTALL_PATH_REFRESH', '1', 'User')
        [Environment]::SetEnvironmentVariable('SKARDI_INSTALL_PATH_REFRESH', $null, 'User')
        Write-Host "Added $Dir to your user PATH (new terminals pick it up)"
    }
    if (-not (($env:Path -split ';') | Where-Object { $_.TrimEnd('\') -ieq $Dir.TrimEnd('\') })) {
        $env:Path = "$env:Path;$Dir"
    }
}

Install-Skardi -InstallDir $InstallDir -ModifyPath (-not $NoModifyPath)
