# Tests for install.ps1. The CLI cases install a local release-shaped zip
# into a fresh temp directory with -NoModifyPath, so nothing is downloaded
# and the user PATH in the registry is never touched.
#
# The agent cases run install.ps1 -AgentsOnly in a child PowerShell with
# USERPROFILE and HOME pointed at a fresh fake home and a local skills
# tarball, so nothing is downloaded and no real agent config is touched. PATH
# is reduced to the Windows directories so a developer's own `claude` or
# `skardi` cannot leak into a case.
#
#   pwsh ./scripts/install_test.ps1
#
# Uses target\debug\skardi.exe as the payload when it exists (CI builds it
# first), so the installed binary is also run; otherwise a stand-in file.

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$root = Split-Path -Parent $PSScriptRoot
$install = Join-Path $root 'install.ps1'
$shell = (Get-Process -Id $PID).Path
$tar = Join-Path $env:SystemRoot 'System32\tar.exe'
$work = Join-Path ([IO.Path]::GetTempPath()) ("skardi-install-test-" + [Guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work | Out-Null

$script:fails = 0
function Pass($name) { Write-Host "ok   $name" }
function Fail($name, $why) { Write-Host "FAIL $name $why"; $script:fails++ }
function Check($name, [scriptblock]$Test) {
    $ok = $false
    try { $ok = [bool](& $Test) } catch { Fail $name $_.Exception.Message; return }
    if ($ok) { Pass $name } else { Fail $name '' }
}

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

# Run install.ps1 against ZIP into DIR, CLI only; returns the error message,
# or $null.
function Invoke-Install {
    param([string]$Zip, [string]$Dir)
    $env:SKARDI_INSTALL_ZIP = $Zip
    try {
        & $install -InstallDir $Dir -NoModifyPath -NoAgents *> $null
        return $null
    } catch {
        return $_.Exception.Message
    } finally {
        Remove-Item Env:\SKARDI_INSTALL_ZIP -ErrorAction SilentlyContinue
    }
}

# A fresh fake home with the given agent config directories.
function New-Home {
    param([string]$Name, [string[]]$Dirs = @())
    $script:H = Join-Path $work "home-$Name"
    New-Item -ItemType Directory -Path $script:H | Out-Null
    foreach ($d in $Dirs) { New-Item -ItemType Directory -Force -Path (Join-Path $script:H $d) | Out-Null }
}

function Write-Text([string]$Path, [string]$Text) {
    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Path) | Out-Null
    [IO.File]::WriteAllText($Path, $Text)
}

function Read-Json([string]$Path) { return [IO.File]::ReadAllText($Path) | ConvertFrom-Json }
# Windows PowerShell wraps warnings at the console width, so whitespace is
# collapsed before a message is matched.
function Read-Log { return [IO.File]::ReadAllText((Join-Path $script:H 'out.log')) -replace '\s+', ' ' }

# Run install.ps1 in a child PowerShell against the current fake home, with
# stdin redirected and -NonInteractive, so there is never a console to ask.
# Returns the exit code; the output goes to out.log in the home.
function Invoke-Setup {
    param([string[]]$Arguments = @(), [string]$ExtraPath, [hashtable]$Env = @{}, [switch]$WithCli)
    $windows = "$env:SystemRoot\System32;$env:SystemRoot;$env:SystemRoot\System32\WindowsPowerShell\v1.0"
    $vars = @{
        USERPROFILE = $script:H
        HOME = $script:H
        PATH = if ($ExtraPath) { "$ExtraPath;$windows" } else { $windows }
        SKARDI_SKILLS_TARBALL = $skillsTarball
        SKARDI_INSTALL_ZIP = $null
        SKARDI_INSTALL_NO_AGENTS = $null
        SKARDI_INSTALL_DIR = $null
        SKARDI_SKILLS_REF = $null
        SKARDI_CLOUD_MCP_URL = $null
        CLAUDE_CONFIG_DIR = $null
        CODEX_HOME = $null
    }
    foreach ($k in $Env.Keys) { $vars[$k] = $Env[$k] }
    $saved = @{}
    foreach ($k in $vars.Keys) {
        $saved[$k] = [Environment]::GetEnvironmentVariable($k)
        [Environment]::SetEnvironmentVariable($k, $vars[$k])
    }
    $argv = @('-NoProfile', '-NonInteractive', '-ExecutionPolicy', 'Bypass', '-File', $install,
        '-InstallDir', (Join-Path $script:H 'skardi-bin'), '-NoModifyPath')
    if (-not $WithCli) { $argv += '-AgentsOnly' }
    try {
        $ErrorActionPreference = 'Continue'
        $out = '' | & $shell @argv @Arguments 2>&1 | Out-String
        $code = $LASTEXITCODE
    } finally {
        foreach ($k in $saved.Keys) { [Environment]::SetEnvironmentVariable($k, $saved[$k]) }
    }
    [IO.File]::WriteAllText((Join-Path $script:H 'out.log'), $out)
    return $code
}

# A stand-in skardi on PATH: prints VERSION for --version and exits CODE
# for anything else.
function New-FakeSkardi {
    param([string]$Version = '0.7.0', [int]$Code = 0)
    $bin = Join-Path $script:H 'bin'
    Write-Text (Join-Path $bin 'skardi.cmd') "@echo off`r`nif `"%1`"==`"--version`" (echo skardi $Version& exit /b 0)`r`nexit /b $Code`r`n"
    return $bin
}

try {
    # ---------------------------------------------------------------------
    # CLI
    # ---------------------------------------------------------------------

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

    # ---------------------------------------------------------------------
    # Agents
    # ---------------------------------------------------------------------

    # A skills tarball shaped like codeload's: one top-level directory,
    # skills/ inside. alpha runs on 0.5.0, beta needs main, gamma states
    # nothing.
    $s = Join-Path $work 'src\skardi-skills-main\skills'
    Write-Text "$s\alpha\SKILL.md" "---`nname: alpha`nmetadata:`n  skardi-min-version: `"0.5.0`"`n---`nbody`n"
    Write-Text "$s\beta\SKILL.md" "---`nname: beta`nmetadata:`n  skardi-min-version: `"main`"`n---`nbody`n"
    Write-Text "$s\gamma\SKILL.md" "---`nname: gamma`n---`nbody`n"
    $skillsTarball = Join-Path $work 'skills.tar.gz'
    & $tar -czf $skillsTarball -C (Join-Path $work 'src') 'skardi-skills-main'
    if ($LASTEXITCODE -ne 0) { throw 'could not build the skills tarball' }

    $cloud = 'https://gateway.skardi.ai/mcp'

    # 1. Cloud MCP for all three agents, from config directories alone.
    New-Home 'all' @('.claude', '.codex', '.cursor')
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
    Check 'exits 0' { $code -eq 0 }
    Check 'claude skills copied' { (Test-Path "$H\.claude\skills\alpha\SKILL.md") -and (Test-Path "$H\.claude\skills\gamma\SKILL.md") }
    Check 'main-only skill held back' { -not (Test-Path "$H\.claude\skills\beta") -and (Read-Log) -match 'Not installed.* beta' }
    Check 'shared skills copied once' { Test-Path "$H\.agents\skills\alpha\SKILL.md" }
    Check 'install marker written' { Test-Path "$H\.claude\skills\alpha\.skardi-install" }
    Check 'claude MCP is URL-only' {
        $e = (Read-Json "$H\.claude.json").mcpServers.skardi
        $e.type -eq 'http' -and $e.url -eq $cloud -and @($e.PSObject.Properties).Count -eq 2
    }
    Check 'cursor MCP is URL-only' {
        $e = (Read-Json "$H\.cursor\mcp.json").mcpServers.skardi
        $e.url -eq $cloud -and @($e.PSObject.Properties).Count -eq 1
    }
    Check 'codex MCP written' { Select-String -LiteralPath "$H\.codex\config.toml" -Pattern "^url = `"$cloud`"" -Quiet }
    Check 'configs written without a BOM' { [IO.File]::ReadAllBytes("$H\.cursor\mcp.json")[0] -eq [byte][char]'{' }
    Check 'sign-in hint printed' { (Read-Log) -match 'codex mcp login skardi' }

    # 2. Re-running changes nothing that is already there.
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
    Check 'rerun exits 0' { $code -eq 0 }
    Check 'codex entry not duplicated' { @(Select-String -LiteralPath "$H\.codex\config.toml" -Pattern '^\[mcp_servers\.skardi\]').Count -eq 1 }
    Check 'rerun reports existing entry' { (Read-Log) -match 'already has a skardi MCP server' }

    # 3. Existing configs are merged into, not replaced, and an existing
    # skardi entry is never overwritten.
    New-Home 'merge' @('.cursor', '.claude')
    Write-Text "$H\.cursor\mcp.json" '{"mcpServers":{"other":{"url":"https://example.com/mcp","args":["one"]}},"keep":1,"empty":[]}'
    Write-Text "$H\.claude.json" '{"numStartups":3,"firstStartTime":"2026-10-16T10:14:33.264Z","mcpServers":{"skardi":{"type":"http","url":"https://mine.example/mcp"}},"projects":{"C:\\work":{"allowedTools":["Bash"]}}}'
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
    Check 'merge exits 0' { $code -eq 0 }
    Check 'other servers kept' {
        $d = Read-Json "$H\.cursor\mcp.json"
        $d.keep -eq 1 -and $d.mcpServers.other.url -eq 'https://example.com/mcp' -and $d.mcpServers.skardi.url -eq $cloud
    }
    Check 'one-element and empty arrays stay arrays' {
        $text = [IO.File]::ReadAllText("$H\.cursor\mcp.json")
        $text -match '"args":\s*\[\s*"one"\s*\]' -and $text -match '"empty":\s*\[\s*\]'
    }
    Check 'existing skardi entry not overwritten' {
        [IO.File]::ReadAllText("$H\.claude.json") -match 'https://mine.example/mcp' -and (Read-Log) -match 'already has a skardi MCP server'
    }
    Check 'only detected agents touched' { -not (Test-Path "$H\.codex") }

    # A config this script cannot rewrite faithfully is left alone.
    New-Home 'unsafe' @('.cursor')
    $bigNumber = '{"mcpServers":{},"id":12345678901234567890}'
    Write-Text "$H\.cursor\mcp.json" $bigNumber
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
    Check 'number past int64 left alone' {
        $code -eq 0 -and [IO.File]::ReadAllText("$H\.cursor\mcp.json") -eq $bigNumber -and (Read-Log) -match 'too large to rewrite safely'
    }
    Write-Text "$H\.cursor\mcp.json" '{"mcpServers": '
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
    Check 'invalid JSON left alone' {
        $code -eq 0 -and [IO.File]::ReadAllText("$H\.cursor\mcp.json") -eq '{"mcpServers": ' -and (Read-Log) -match 'is not valid JSON'
    }
    $shapes = @(
        '[{"mcpServers":{}}]', '42', '"just a string"',
        '{"mcpServers":[{"name":"x"}]}', '{"mcpServers":"oops"}'
    )
    foreach ($shape in $shapes) {
        Write-Text "$H\.cursor\mcp.json" $shape
        $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
        Check "non-object config left alone: $shape" {
            $code -eq 0 -and [IO.File]::ReadAllText("$H\.cursor\mcp.json") -eq $shape -and (Read-Log) -match 'not a JSON object|not hold a JSON object'
        }
    }
    $readOnly = '{"mcpServers":{}}'
    Write-Text "$H\.cursor\mcp.json" $readOnly
    Set-ItemProperty -LiteralPath "$H\.cursor\mcp.json" -Name IsReadOnly -Value $true
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
    Check 'read-only config left alone' {
        $code -eq 0 -and [IO.File]::ReadAllText("$H\.cursor\mcp.json") -eq $readOnly -and
            (Get-Item -LiteralPath "$H\.cursor\mcp.json").IsReadOnly -and (Read-Log) -match 'could not write'
    }
    Set-ItemProperty -LiteralPath "$H\.cursor\mcp.json" -Name IsReadOnly -Value $false

    # A real .claude.json rewrite (no claude CLI on PATH) keeps everything
    # else in the file. A PowerShell that would rewrite the dates refuses.
    New-Home 'rewrite' @('.claude')
    $claudeState = '{"numStartups":3,"firstStartTime":"2026-10-16T18:14:33.264+05:00","projects":{"C:\\work":{"allowedTools":["Bash"],"history":[]}},"mcpServers":{"other":{"type":"stdio","command":"x","args":["y"],"env":{}}}}'
    Write-Text "$H\.claude.json" $claudeState
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud')
    $keepsDates = $PSVersionTable.PSVersion.Major -lt 6 -or (Get-Command ConvertFrom-Json).Parameters.ContainsKey('DateKind')
    if ($keepsDates) {
        Check 'rewrite keeps the rest of .claude.json' {
            $text = [IO.File]::ReadAllText("$H\.claude.json")
            $d = $text | ConvertFrom-Json
            $code -eq 0 -and $d.mcpServers.skardi.url -eq $cloud -and $d.numStartups -eq 3 -and
                $text -match '"firstStartTime":\s*"2026-10-16T18:14:33.264\+05:00"' -and
                $text -match '"allowedTools":\s*\[\s*"Bash"\s*\]' -and $text -match '"history":\s*\[\s*\]' -and
                $d.mcpServers.other.args[0] -eq 'y'
        }
    } else {
        Check 'dates this PowerShell would rewrite are left alone' {
            $code -eq 0 -and [IO.File]::ReadAllText("$H\.claude.json") -eq $claudeState -and (Read-Log) -match 'holds dates'
        }
    }

    # CLAUDE_CONFIG_DIR and CODEX_HOME move the agents' config.
    New-Home 'envdirs' @('cc', 'cx')
    $null = Invoke-Setup @('-Yes', '-Mcp', 'cloud') -Env @{ CLAUDE_CONFIG_DIR = "$H\cc"; CODEX_HOME = "$H\cx" }
    Check 'CLAUDE_CONFIG_DIR is used' {
        (Test-Path "$H\cc\skills\alpha\SKILL.md") -and (Read-Json "$H\cc\.claude.json").mcpServers.skardi.url -eq $cloud -and
            -not (Test-Path "$H\.claude.json")
    }
    Check 'CODEX_HOME is used' { Select-String -LiteralPath "$H\cx\config.toml" -Pattern '^\[mcp_servers\.skardi\]' -Quiet }

    # 4. A skill directory someone else put there is left alone.
    New-Home 'foreign' @('.claude')
    Write-Text "$H\.claude\skills\alpha\SKILL.md" 'mine'
    $null = Invoke-Setup @('-Yes', '-Mcp', 'none')
    Check 'foreign skill untouched' { [IO.File]::ReadAllText("$H\.claude\skills\alpha\SKILL.md") -eq 'mine' }
    Check 'other skills still installed' { Test-Path "$H\.claude\skills\gamma\SKILL.md" }
    Check 'MCP none writes no config' { -not (Test-Path "$H\.claude.json") }

    # 5. The Claude Code plugin already carries the skills.
    New-Home 'plugin' @('.claude\plugins')
    Write-Text "$H\.claude\plugins\installed_plugins.json" '{"plugins":{"skardi@skardi-skills":[{}]}}'
    $null = Invoke-Setup @('-Yes', '-Mcp', 'none')
    Check 'no copy next to the plugin' { -not (Test-Path "$H\.claude\skills") }

    # 6. -Yes without -Mcp installs the skills but does not pick an MCP
    # target on the user's behalf.
    New-Home 'noflag' @('.claude', '.cursor')
    $null = Invoke-Setup @('-Yes')
    Check '-Yes installs the skills' { (Test-Path "$H\.claude\skills\alpha\SKILL.md") -and (Test-Path "$H\.agents\skills\alpha\SKILL.md") }
    Check 'MCP skipped without -Mcp' {
        -not (Test-Path "$H\.cursor\mcp.json") -and -not (Test-Path "$H\.claude.json") -and (Read-Log) -match 'MCP: skipped'
    }

    # 7. No console and no -Yes: every answer is no, nothing beyond the CLI.
    New-Home 'notty' @('.claude', '.cursor')
    $code = Invoke-Setup @('-Mcp', 'cloud')
    Check 'no console exits 0' { $code -eq 0 }
    Check 'no console means no changes' {
        -not (Test-Path "$H\.claude\skills") -and -not (Test-Path "$H\.agents") -and
            -not (Test-Path "$H\.claude.json") -and -not (Test-Path "$H\.cursor\mcp.json")
    }
    Check 'no console says how to rerun' { (Read-Log) -match 'no console to ask on.* Re-run with -AgentsOnly -Yes' }

    # 8. Local MCP needs a skardi that has the mcp command.
    New-Home 'local' @('.cursor', '.claude')
    $bin = New-FakeSkardi -Code 2
    $null = Invoke-Setup @('-Yes', '-Mcp', 'local') -ExtraPath $bin
    Check 'local MCP skipped on old CLI' { -not (Test-Path "$H\.cursor\mcp.json") -and (Read-Log) -match "no 'mcp' command" }
    $bin = New-FakeSkardi -Code 0
    $null = Invoke-Setup @('-Yes', '-Mcp', 'local') -ExtraPath $bin
    Check 'local MCP runs skardi mcp' {
        $e = (Read-Json "$H\.cursor\mcp.json").mcpServers.skardi
        $e.command -eq 'skardi' -and @($e.args).Count -eq 1 -and $e.args[0] -eq 'mcp' -and
            [IO.File]::ReadAllText("$H\.cursor\mcp.json") -match '"args":\s*\[\s*"mcp"\s*\]'
    }
    Check 'claude local entry is stdio' {
        $e = (Read-Json "$H\.claude.json").mcpServers.skardi
        $e.type -eq 'stdio' -and $e.command -eq 'skardi' -and $e.args[0] -eq 'mcp'
    }

    # 9. With the claude CLI on PATH, it is used instead of editing
    # .claude.json, and an entry it already has is left alone.
    New-Home 'claudecli' @('.claude')
    $bin = Join-Path $H 'bin'
    $calls = Join-Path $H 'claude-calls.txt'
    Write-Text "$bin\claude.cmd" "@echo off`r`nif `"%2`"==`"get`" (if exist `"%~dp0has-skardi`" (exit /b 0) else (exit /b 1))`r`necho %*>>`"$calls`"`r`nexit /b 0`r`n"
    $null = Invoke-Setup @('-Yes', '-Mcp', 'cloud') -ExtraPath $bin
    Check 'claude mcp add used for cloud' {
        (Test-Path $calls) -and [IO.File]::ReadAllText($calls).Trim() -eq "mcp add --scope user --transport http skardi $cloud"
    }
    Check 'claude CLI path leaves .claude.json alone' { -not (Test-Path "$H\.claude.json") }
    Remove-Item -LiteralPath $calls
    $null = Invoke-Setup @('-Yes', '-Mcp', 'local') -ExtraPath "$bin;$(New-FakeSkardi)"
    Check 'claude local add passes -- through' {
        [IO.File]::ReadAllText($calls).Trim() -eq 'mcp add --scope user skardi -- skardi mcp'
    }
    Remove-Item -LiteralPath $calls
    Write-Text "$bin\has-skardi" ''
    $null = Invoke-Setup @('-Yes', '-Mcp', 'cloud') -ExtraPath $bin
    Check 'claude CLI entry not overwritten' { -not (Test-Path $calls) -and (Read-Log) -match 'already has a skardi MCP server' }

    # 10. Versions: -WithUnreleased, and a CLI older than a skill needs.
    New-Home 'unreleased' @('.cursor')
    $null = Invoke-Setup @('-Yes', '-WithUnreleased')
    Check '-WithUnreleased installs main-only' { Test-Path "$H\.agents\skills\beta\SKILL.md" }
    New-Home 'oldcli' @('.cursor')
    $null = Invoke-Setup @('-Yes') -ExtraPath (New-FakeSkardi -Version '0.4.2')
    Check 'too-old CLI skips 0.5.0 skill' { -not (Test-Path "$H\.agents\skills\alpha") -and (Test-Path "$H\.agents\skills\gamma\SKILL.md") }
    $null = Invoke-Setup @('-Yes') -ExtraPath (New-FakeSkardi -Version '0.10.0')
    Check '0.10.0 counts as newer than 0.5.0' { Test-Path "$H\.agents\skills\alpha\SKILL.md" }

    # 11. No agent installed.
    New-Home 'none'
    $code = Invoke-Setup @('-Yes')
    Check 'no agent is not an error' { $code -eq 0 -and (Read-Log) -match 'No supported AI coding agent found' }

    # 12. SKARDI_INSTALL_NO_AGENTS installs the CLI and nothing else.
    New-Home 'noagents' @('.cursor')
    $code = Invoke-Setup @('-Yes', '-Mcp', 'cloud') -WithCli -Env @{ SKARDI_INSTALL_ZIP = $good; SKARDI_INSTALL_NO_AGENTS = '1' }
    Check 'SKARDI_INSTALL_NO_AGENTS skips agents' {
        $code -eq 0 -and (Test-Path "$H\skardi-bin\skardi.exe") -and -not (Test-Path "$H\.agents") -and -not (Test-Path "$H\.cursor\mcp.json")
    }

    # 13. Bad input fails fast, before anything is installed.
    New-Home 'bad' @('.cursor')
    $code = Invoke-Setup @('-Mcp', 'nowhere') -WithCli -Env @{ SKARDI_INSTALL_ZIP = $good }
    Check 'bad -Mcp rejected' { $code -ne 0 -and (Read-Log) -match '-Mcp must be cloud, local or none' -and -not (Test-Path "$H\skardi-bin") }
} finally {
    Remove-Item -LiteralPath $work -Recurse -Force -ErrorAction SilentlyContinue
}

if ($script:fails -gt 0) {
    Write-Host "$($script:fails) failed"
    exit 1
}
Write-Host 'all passed'
# Some cases run the installer to a failing exit on purpose, and the CI
# runner exits with whatever $LASTEXITCODE is left, so say 0 out loud.
exit 0
