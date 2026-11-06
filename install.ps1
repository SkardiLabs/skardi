# Install the skardi CLI on Windows, then set up the AI coding agents on this
# machine to use Skardi: copy the Agent Skills into each agent's skills
# directory and register Skardi's MCP server in each agent's config.
#
#   irm https://raw.githubusercontent.com/SkardiLabs/skardi/main/install.ps1 | iex
#
# Downloads the latest release's skardi.exe, checks its SHA-256, puts it in
# %LOCALAPPDATA%\Programs\skardi\bin and adds that directory to your user
# PATH. No administrator rights needed. Re-running it upgrades in place.
#
# The agent setup is the one install.sh does on macOS and Linux. Re-run with
# -AgentsOnly to refresh the skills or to set up an agent you installed
# later; it never reinstalls the CLI. Every step is idempotent: an MCP entry
# named `skardi` that already exists is left alone.
#
# To pass options, run the script as a script block:
#
#   & ([scriptblock]::Create((irm https://raw.githubusercontent.com/SkardiLabs/skardi/main/install.ps1))) -AgentsOnly
#
# Options:
#   -InstallDir DIR   where skardi.exe goes
#                     (default %LOCALAPPDATA%\Programs\skardi\bin)
#   -NoModifyPath     do not add the install directory to the user PATH
#   -Yes              answer yes to every prompt (MCP is still skipped unless
#                     -Mcp is given, since there is no safe default target)
#   -AgentsOnly       skip installing the CLI
#   -NoAgents         install the CLI only
#   -Mcp MODE         cloud | local | none
#   -WithUnreleased   also install skills that need Skardi main (a main build
#                     reports the last release's version, so it cannot be told
#                     apart from that release automatically)
#
# Environment:
#   SKARDI_INSTALL_DIR           same as -InstallDir
#   SKARDI_INSTALL_ZIP           install from a local release zip instead of
#                                downloading one (used by the tests); its
#                                checksum is read from the .sha256 next to it
#                                when there is one
#   SKARDI_INSTALL_NO_AGENTS=1   same as -NoAgents
#   SKARDI_SKILLS_REF            skardi-skills branch or tag (default main)
#   SKARDI_SKILLS_TARBALL        use a local skardi-skills tarball instead of
#                                downloading one (used by the tests)
#   SKARDI_CLOUD_MCP_URL         Skardi Cloud MCP endpoint
#
# Run it as yourself: skills and agent configs belong in your user profile.

[CmdletBinding()]
param(
    [string]$InstallDir,
    [switch]$NoModifyPath,
    [switch]$Yes,
    [switch]$AgentsOnly,
    [switch]$NoAgents,
    # Checked by hand, not with [ValidateSet]: under `irm | iex` the parameter
    # becomes a variable in the caller's session and would keep the attribute.
    [string]$Mcp,
    [switch]$WithUnreleased
)

# Everything runs inside a function: `irm | iex` evaluates this script in the
# CALLER's scope, so a top-level $ErrorActionPreference would leak into their
# session, and `exit` would close their PowerShell window.
# The -InstallDir the user gave, else SKARDI_INSTALL_DIR, else the default.
function Resolve-SkardiInstallDir {
    param([string]$InstallDir)

    if ($InstallDir) { return $InstallDir }
    if ($env:SKARDI_INSTALL_DIR) { return $env:SKARDI_INSTALL_DIR }
    return Join-Path $env:LOCALAPPDATA 'Programs\skardi\bin'
}

function Install-Skardi {
    param([string]$InstallDir, [bool]$ModifyPath)

    $ErrorActionPreference = 'Stop'
    # Invoke-WebRequest's progress bar slows downloads severalfold on 5.1.
    $ProgressPreference = 'SilentlyContinue'

    $repo = 'SkardiLabs/skardi'
    $target = 'x86_64-pc-windows-msvc'

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

# Set up Claude Code, Codex and Cursor the way install.sh does: the Skardi
# skills in each agent's skills directory, and Skardi's MCP server in each
# agent's config. The helpers are nested, so they live only while this runs.
function Install-SkardiAgents {
    param([string]$InstallDir, [string]$McpMode, [bool]$AssumeYes, [bool]$WithUnreleased)

    $ErrorActionPreference = 'Stop'
    $ProgressPreference = 'SilentlyContinue'

    $repo = 'SkardiLabs/skardi'
    $skillsRepo = 'SkardiLabs/skardi-skills'
    $skillsRef = if ($env:SKARDI_SKILLS_REF) { $env:SKARDI_SKILLS_REF } else { 'main' }
    $cloudUrl = if ($env:SKARDI_CLOUD_MCP_URL) { $env:SKARDI_CLOUD_MCP_URL } else { 'https://gateway.skardi.ai/mcp' }
    $docsUrl = "https://github.com/$repo/blob/main/docs/mcp.md"
    $utf8 = New-Object System.Text.UTF8Encoding $false

    # An agent counts as installed when its config directory exists. That is
    # what each agent creates on first run, and it is present even when the
    # agent's CLI is not on PATH (a desktop-only install), which is why the
    # MCP steps write config files when the CLI is missing. Agents resolve
    # the home directory from USERPROFILE on Windows.
    $userHome = if ($env:USERPROFILE) { $env:USERPROFILE } else { $HOME }
    $claudeDir = if ($env:CLAUDE_CONFIG_DIR) { $env:CLAUDE_CONFIG_DIR } else { Join-Path $userHome '.claude' }
    # User-scoped MCP servers live in .claude.json: in the home directory by
    # default, inside CLAUDE_CONFIG_DIR when that is set.
    $claudeJson = if ($env:CLAUDE_CONFIG_DIR) { Join-Path $env:CLAUDE_CONFIG_DIR '.claude.json' } else { Join-Path $userHome '.claude.json' }
    $codexDir = if ($env:CODEX_HOME) { $env:CODEX_HOME } else { Join-Path $userHome '.codex' }
    $cursorDir = Join-Path $userHome '.cursor'
    $sharedSkills = Join-Path $userHome '.agents\skills'
    $labels = @{ claude = 'Claude Code'; codex = 'Codex'; cursor = 'Cursor' }

    # Prompts need someone at a console. With none (CI, a scheduled task,
    # input piped in) and no -Yes, every answer is no: nothing outside the
    # CLI install happens unless someone agreed to it. The console is read
    # directly rather than through Read-Host, which a host started with
    # -NonInteractive (the VS Code terminal is one) refuses.
    $hasConsole = [Environment]::UserInteractive -and -not [Console]::IsInputRedirected

    function Read-YesNo {
        param([string]$Question, [bool]$Default)
        if ($AssumeYes) { return $true }
        if (-not $hasConsole) { return $false }
        Write-Host -NoNewline "$Question "
        $reply = [Console]::ReadLine()
        if ($reply) { $reply = $reply.Trim() }
        if (-not $reply) { return $Default }
        return $reply -match '^(y|yes)$'
    }

    # Run a native command for its exit code and output. Under Windows
    # PowerShell 5.1 a native command's stderr becomes error records, which
    # 'Stop' would turn into a throw, so this runs with 'Continue' and turns
    # them back into plain lines; -NoStderr drops them instead.
    function Invoke-Native {
        param([string]$Exe, [string[]]$Arguments, [switch]$NoStderr)
        $ErrorActionPreference = 'Continue'
        try {
            $out = if ($NoStderr) {
                & $Exe @Arguments 2>$null | Out-String
            } else {
                & $Exe @Arguments 2>&1 | ForEach-Object { "$_" } | Out-String
            }
            return [pscustomobject]@{ ExitCode = $LASTEXITCODE; Output = $out.Trim() }
        } catch {
            return [pscustomobject]@{ ExitCode = -1; Output = $_.Exception.Message }
        }
    }

    function Get-SkardiExe {
        $installed = Join-Path $InstallDir 'skardi.exe'
        if (Test-Path -LiteralPath $installed) { return $installed }
        $cmd = Get-Command skardi -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($cmd) { return $cmd.Source }
        return $null
    }

    # x.y.z as three numbers; a pre-release suffix on a part is dropped.
    function ConvertTo-VersionParts {
        param([string]$Version)
        $parts = $Version.Split('.')
        foreach ($i in 0..2) {
            $digits = if ($i -lt $parts.Count) { [regex]::Match($parts[$i], '^\d+').Value } else { '' }
            if ($digits) { [int64]$digits } else { [int64]0 }
        }
    }

    function Test-VersionAtLeast {
        param([string]$Have, [string]$Need)
        $a = @(ConvertTo-VersionParts $Have)
        $b = @(ConvertTo-VersionParts $Need)
        foreach ($i in 0..2) {
            if ($a[$i] -ne $b[$i]) { return $a[$i] -gt $b[$i] }
        }
        return $true
    }

    # metadata.skardi-min-version from a SKILL.md front matter, or $null.
    function Get-SkillMinVersion {
        param([string]$SkillMd)
        $lines = [IO.File]::ReadAllLines($SkillMd)
        if ($lines.Count -eq 0 -or $lines[0] -notmatch '^---') { return $null }
        foreach ($line in ($lines | Select-Object -Skip 1)) {
            if ($line -match '^---') { break }
            if ($line -match '^  skardi-min-version:\s*"?([^"\s]+)"?') { return $Matches[1] }
        }
        return $null
    }

    # Download (or copy) the skardi-skills tarball and unpack it into TMP;
    # returns its skills directory. tar.exe ships with Windows 10 and later.
    function Get-SkillsDir {
        param([string]$Tmp)
        $tarball = Join-Path $Tmp 'skills.tar.gz'
        if ($env:SKARDI_SKILLS_TARBALL) {
            Copy-Item -LiteralPath $env:SKARDI_SKILLS_TARBALL -Destination $tarball
        } else {
            [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
            $url = "https://codeload.github.com/$skillsRepo/tar.gz/$skillsRef"
            try {
                Invoke-WebRequest -Uri $url -OutFile $tarball -UseBasicParsing
            } catch {
                throw "could not download $skillsRepo@$skillsRef ($($_.Exception.Message))"
            }
        }
        $src = Join-Path $Tmp 'skills-src'
        New-Item -ItemType Directory -Path $src | Out-Null
        # The system tar, not a GNU tar from Git for Windows on PATH, which
        # reads C:\... as a remote host.
        $tar = Join-Path $env:SystemRoot 'System32\tar.exe'
        if (-not (Test-Path -LiteralPath $tar)) { $tar = 'tar' }
        $result = Invoke-Native $tar @('-xzf', $tarball, '-C', $src)
        if ($result.ExitCode -ne 0) { throw "could not unpack $skillsRepo@${skillsRef}: $($result.Output)" }
        $dir = Get-ChildItem -LiteralPath $src -Directory |
            ForEach-Object { Join-Path $_.FullName 'skills' } |
            Where-Object { Test-Path -LiteralPath $_ -PathType Container } |
            Select-Object -First 1
        if (-not $dir) { throw "$skillsRepo@$skillsRef has no skills/ directory" }
        return $dir
    }

    # Each SKILL.md states the oldest Skardi it runs on, as
    # metadata.skardi-min-version: a release number, or "main" while no
    # release has what it needs. Skills the installed CLI is too old for are
    # left out, so the agent is never taught a capability the user's Skardi
    # lacks. With no skardi found the version is unknown and only "main"
    # skills are held back.
    function Select-Skills {
        param([string]$SkillsDir)
        $version = $null
        $exe = Get-SkardiExe
        if ($exe) {
            $result = Invoke-Native $exe @('--version') -NoStderr
            if ($result.ExitCode -eq 0 -and $result.Output -match '^\S+\s+(\S+)') { $version = $Matches[1] }
        }
        $picked = @()
        $skipped = @()
        foreach ($skill in (Get-ChildItem -LiteralPath $SkillsDir -Directory | Sort-Object Name)) {
            $skillMd = Join-Path $skill.FullName 'SKILL.md'
            if (-not (Test-Path -LiteralPath $skillMd)) { continue }
            $min = Get-SkillMinVersion $skillMd
            if ($min -eq 'main' -and -not $WithUnreleased) { $skipped += $skill.Name; continue }
            if ($min -and $min -ne 'main' -and $version -and -not (Test-VersionAtLeast $version $min)) {
                $skipped += $skill.Name; continue
            }
            $picked += $skill.Name
        }
        if ($skipped.Count -gt 0) {
            $against = if ($version) { $version } else { 'the latest release' }
            Write-Host "Not installed, they need a newer Skardi than ${against}: $($skipped -join ' ')"
            Write-Host '  (on a build of Skardi main, re-run with -AgentsOnly -WithUnreleased)'
        }
        return , $picked
    }

    # Copy the picked skills into DEST. A directory this script did not
    # install (no marker) is someone else's and is left untouched.
    function Copy-Skills {
        param([string]$SkillsDir, [string[]]$Names, [string]$Dest)
        New-Item -ItemType Directory -Force -Path $Dest | Out-Null
        foreach ($name in $Names) {
            $target = Join-Path $Dest $name
            if (Test-Path -LiteralPath $target) {
                if (-not (Test-Path -LiteralPath (Join-Path $target '.skardi-install'))) {
                    Write-Warning "$target exists and was not installed by this script; left as is"
                    continue
                }
                Remove-Item -LiteralPath $target -Recurse -Force
            }
            Copy-Item -LiteralPath (Join-Path $SkillsDir $name) -Destination $target -Recurse
            [IO.File]::WriteAllText((Join-Path $target '.skardi-install'), "source=$skillsRepo@$skillsRef`n", $utf8)
        }
    }

    function Test-ClaudePlugin {
        $file = Join-Path $claudeDir 'plugins\installed_plugins.json'
        return (Test-Path -LiteralPath $file) -and
            (Select-String -LiteralPath $file -SimpleMatch '"skardi@skardi-skills"' -Quiet)
    }

    function Install-Skills {
        param([string]$SkillsDir, [string[]]$Names, [string[]]$Agents)
        $sharedDone = $false
        foreach ($agent in $Agents) {
            if ($agent -eq 'claude') {
                if (Test-ClaudePlugin) {
                    Write-Host '  Claude Code: the skardi plugin is already installed; skills left to it'
                } else {
                    $dest = Join-Path $claudeDir 'skills'
                    Copy-Skills $SkillsDir $Names $dest
                    Write-Host "  Claude Code: skills in $dest"
                }
            } else {
                # Codex and Cursor both read the cross-tool ~/.agents/skills,
                # so one copy serves both.
                if (-not $sharedDone) {
                    Copy-Skills $SkillsDir $Names $sharedSkills
                    $sharedDone = $true
                }
                Write-Host "  $($labels[$agent]): skills in $sharedSkills"
            }
        }
    }

    $canKeepDates = (Get-Command ConvertFrom-Json).Parameters.ContainsKey('DateKind')

    # Why TEXT cannot go through ConvertFrom-Json and back unchanged, or
    # $null when it can. PowerShell 7 reads an integer past int64 as a
    # BigInteger, which comes back out as an object, and before 7.5 it turns
    # date strings into DateTime, which come back in another format or time
    # zone. Windows PowerShell 5.1 does neither.
    function Get-JsonRewriteRisk {
        param([string]$Text)
        if ($Text -match '[:\[,]\s*-?\d{19,}') { return 'holds a number too large to rewrite safely' }
        if ($PSVersionTable.PSVersion.Major -ge 6 -and -not $canKeepDates -and
            $Text -match '"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}') {
            return 'holds dates this PowerShell would rewrite (PowerShell 7.5 or Windows PowerShell 5.1 can do it)'
        }
        return $null
    }

    function Test-JsonObject {
        param($Value)
        return $Value -is [System.Management.Automation.PSCustomObject]
    }

    # Add mcpServers.skardi to a JSON config unless it is already there. A
    # file that is not a JSON object with an object for mcpServers, or that
    # would not survive the rewrite, is left to the user. The new content is
    # written to a temp file and swapped in, so an agent reading it never
    # sees half a file.
    function Add-JsonMcp {
        param([string]$Path, $Entry)
        $byHand = "the skardi MCP server was not added. Add it by hand: $docsUrl"
        $data = [pscustomobject]@{}
        $exists = Test-Path -LiteralPath $Path
        if ($exists) {
            $text = [IO.File]::ReadAllText($Path)
        }
        if ($exists -and $text.Trim()) {
            # The check on the first character also keeps PowerShell 7 from
            # unrolling a top-level array into its first element.
            if ($text.Trim()[0] -ne '{') { Write-Warning "$Path does not hold a JSON object; $byHand"; return }
            $options = @{}
            if ($canKeepDates) { $options.DateKind = 'String' }
            try {
                $data = $text | ConvertFrom-Json @options
            } catch {
                Write-Warning "$Path is not valid JSON; $byHand"
                return
            }
            if (-not (Test-JsonObject $data)) { Write-Warning "$Path does not hold a JSON object; $byHand"; return }
        }
        if (-not $data.PSObject.Properties['mcpServers']) {
            $data | Add-Member -NotePropertyName mcpServers -NotePropertyValue ([pscustomobject]@{})
        } elseif ($null -eq $data.mcpServers) {
            $data.mcpServers = [pscustomobject]@{}
        } elseif (-not (Test-JsonObject $data.mcpServers)) {
            Write-Warning "$Path has an mcpServers that is not a JSON object; $byHand"
            return
        }
        # Property names match without regard to case, so an existing
        # "Skardi" also counts as present and is left alone.
        if ($data.mcpServers.PSObject.Properties['skardi']) {
            Write-Host '    already has a skardi MCP server; left as is'
            return
        }
        if ($exists) {
            $risk = Get-JsonRewriteRisk $text
            if ($risk) { Write-Warning "$Path $risk; $byHand"; return }
        }
        $data.mcpServers | Add-Member -NotePropertyName skardi -NotePropertyValue $Entry
        $dir = Split-Path -Parent $Path
        if ($dir) { New-Item -ItemType Directory -Force -Path $dir | Out-Null }
        $tmp = "$Path.skardi-tmp"
        [IO.File]::WriteAllText($tmp, (ConvertTo-Json -InputObject $data -Depth 100) + "`n", $utf8)
        try {
            # File.Replace keeps the file's attributes and ACL, and refuses a
            # read-only file instead of overriding it. [NullString] because
            # PowerShell passes a plain $null to a string parameter as "".
            if ($exists) { [IO.File]::Replace($tmp, $Path, [NullString]::Value) } else { [IO.File]::Move($tmp, $Path) }
        } catch {
            Remove-Item -LiteralPath $tmp -Force -ErrorAction SilentlyContinue
            Write-Warning "could not write $Path ($($_.Exception.Message)); $byHand"
        }
    }

    function Add-CodexMcp {
        param([string]$Mode)
        $file = Join-Path $codexDir 'config.toml'
        if ((Test-Path -LiteralPath $file) -and
            (Select-String -LiteralPath $file -Pattern '^\[mcp_servers\.skardi\]' -CaseSensitive -Quiet)) {
            Write-Host '    already has a skardi MCP server; left as is'
            return
        }
        New-Item -ItemType Directory -Force -Path $codexDir | Out-Null
        $body = if ($Mode -eq 'cloud') { "url = `"$cloudUrl`"`n" } else { "command = `"skardi`"`nargs = [`"mcp`"]`n" }
        [IO.File]::AppendAllText($file, "`n[mcp_servers.skardi]`n$body", $utf8)
    }

    # User scope, so every project sees it. A running Claude Code rewrites
    # .claude.json often, so its own CLI is used when it is on PATH; editing
    # the file directly is the fallback for a desktop-only install.
    function Add-ClaudeMcp {
        param([string]$Mode)
        $claude = Get-Command claude -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
        if ($claude) {
            if ((Invoke-Native $claude.Source @('mcp', 'get', 'skardi')).ExitCode -eq 0) {
                Write-Host '    already has a skardi MCP server; left as is'
                return
            }
            $add = if ($Mode -eq 'cloud') {
                @('mcp', 'add', '--scope', 'user', '--transport', 'http', 'skardi', $cloudUrl)
            } else {
                @('mcp', 'add', '--scope', 'user', 'skardi', '--', 'skardi', 'mcp')
            }
            $result = Invoke-Native $claude.Source $add
            if ($result.ExitCode -ne 0) {
                Write-Warning "claude mcp add failed ($($result.Output)); add the skardi MCP server by hand: $docsUrl"
            }
        } elseif ($Mode -eq 'cloud') {
            Add-JsonMcp $claudeJson ([pscustomobject][ordered]@{ type = 'http'; url = $cloudUrl })
        } else {
            Add-JsonMcp $claudeJson ([pscustomobject][ordered]@{ type = 'stdio'; command = 'skardi'; args = @('mcp'); env = [pscustomobject]@{} })
        }
    }

    function Select-McpMode {
        if ($McpMode) { return $McpMode }
        if ($AssumeYes) { return 'none' }
        if (Read-YesNo 'Connect your agents to Skardi Cloud over MCP? [y/N]' $false) { return 'cloud' }
        if (Read-YesNo 'Connect them to a local skardi-server instead? [y/N]' $false) { return 'local' }
        return 'none'
    }

    function Install-Mcp {
        param([string[]]$Agents)
        $mode = Select-McpMode
        if ($mode -eq 'none') { Write-Host 'MCP: skipped'; return }

        if ($mode -eq 'local') {
            $exe = Get-SkardiExe
            if (-not $exe -or (Invoke-Native $exe @('mcp', '--help')).ExitCode -ne 0) {
                Write-Warning "this skardi has no 'mcp' command (added in v0.6.0; upgrade the CLI and re-run with -AgentsOnly); local MCP skipped"
                return
            }
        }

        Write-Host "MCP (${mode}):"
        foreach ($agent in $Agents) {
            Write-Host "  $($labels[$agent])"
            switch ($agent) {
                'claude' { Add-ClaudeMcp $mode }
                'codex' { Add-CodexMcp $mode }
                'cursor' {
                    $entry = if ($mode -eq 'cloud') {
                        [pscustomobject]@{ url = $cloudUrl }
                    } else {
                        [pscustomobject][ordered]@{ command = 'skardi'; args = @('mcp') }
                    }
                    Add-JsonMcp (Join-Path $cursorDir 'mcp.json') $entry
                }
            }
        }
        Write-Host ''
        if ($mode -eq 'cloud') {
            Write-Host 'Each agent asks you to sign in to Skardi Cloud in the browser the first time it connects:'
            if ($Agents -contains 'claude') { Write-Host '  Claude Code: run  claude mcp login skardi  (or /mcp inside a session)' }
            if ($Agents -contains 'codex') { Write-Host '  Codex:       run  codex mcp login skardi' }
            if ($Agents -contains 'cursor') { Write-Host '  Cursor:      open Settings > MCP and sign in to skardi' }
        } else {
            Write-Host 'skardi mcp talks to the skardi-server your config points at (default http://localhost:8080).'
        }
    }

    $agents = @()
    if (Test-Path -LiteralPath $claudeDir -PathType Container) { $agents += 'claude' }
    if (Test-Path -LiteralPath $codexDir -PathType Container) { $agents += 'codex' }
    if (Test-Path -LiteralPath $cursorDir -PathType Container) { $agents += 'cursor' }
    if ($agents.Count -eq 0) {
        Write-Host 'No supported AI coding agent found (Claude Code, Codex, Cursor). Re-run with -AgentsOnly after installing one.'
        return
    }
    $names = ($agents | ForEach-Object { $labels[$_] }) -join ', '
    if (-not $AssumeYes -and -not $hasConsole) {
        Write-Host "Found $names, but there is no console to ask on, so they were not set up. Re-run with -AgentsOnly -Yes (add -Mcp cloud or -Mcp local for MCP)."
        return
    }
    if (-not (Read-YesNo "Set up Skardi skills and MCP for ${names}? [Y/n]" $true)) {
        Write-Host 'Skipped. Run again with -AgentsOnly whenever you want it.'
        return
    }

    $tmp = Join-Path ([IO.Path]::GetTempPath()) ("skardi-agents-" + [Guid]::NewGuid().ToString('N'))
    New-Item -ItemType Directory -Path $tmp | Out-Null
    try {
        $skillsDir = Get-SkillsDir $tmp
        $picked = Select-Skills $skillsDir
        Write-Host "Skills ($skillsRepo@${skillsRef}):"
        Install-Skills $skillsDir $picked $agents
        Install-Mcp $agents
    } finally {
        Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue
    }
}

# The entry point. -Mcp is checked before anything is installed, so a typo
# fails fast instead of after the download.
function Invoke-SkardiInstall {
    param([string]$InstallDir, [bool]$ModifyPath, [bool]$AssumeYes, [bool]$InstallCli, [bool]$SetupAgents, [string]$McpMode, [bool]$WithUnreleased)

    if ($McpMode -and @('cloud', 'local', 'none') -notcontains $McpMode) {
        throw "-Mcp must be cloud, local or none, got '$McpMode'"
    }
    $dir = Resolve-SkardiInstallDir $InstallDir
    if ($InstallCli) {
        Install-Skardi -InstallDir $dir -ModifyPath $ModifyPath
    }
    if ($SetupAgents) {
        Install-SkardiAgents -InstallDir $dir -McpMode $McpMode.ToLower() -AssumeYes $AssumeYes -WithUnreleased $WithUnreleased
    }
}

# Under `irm | iex` the functions above are defined in the caller's session;
# they are removed again once the install is done.
try {
    Invoke-SkardiInstall -InstallDir $InstallDir -ModifyPath (-not $NoModifyPath) -AssumeYes ([bool]$Yes) `
        -InstallCli (-not $AgentsOnly) -SetupAgents ((-not $NoAgents) -and $env:SKARDI_INSTALL_NO_AGENTS -ne '1') `
        -McpMode $Mcp -WithUnreleased ([bool]$WithUnreleased)
} finally {
    'Resolve-SkardiInstallDir', 'Install-Skardi', 'Add-ToUserPath', 'Install-SkardiAgents', 'Invoke-SkardiInstall' |
        ForEach-Object { Remove-Item -LiteralPath "function:$_" -ErrorAction SilentlyContinue }
}
