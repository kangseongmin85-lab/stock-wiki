# auto-backup.ps1 -- weekly local-asset backup
# Snapshots the entire working tree to the GitHub branch 'backup/auto'.
# Never touches the current branch (main), the real index, or files on disk
# (uses a temporary index via plumbing).
# Secret blocking (triple): (1) .gitignore excludes .env/config.json/_archive/_news-guide
#                           (2) pre-push token-pattern scan
#                           (3) GitHub push protection
# Log: _cache\auto_backup.log  (ASCII-only to avoid PS 5.1 encoding issues)

# repo path = this script's own directory (avoids hardcoding a non-ASCII path)
$repo = $PSScriptRoot
if (-not $repo) { $repo = Split-Path -Parent $MyInvocation.MyCommand.Definition }
$logDir = Join-Path $repo '_cache'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
$log = Join-Path $logDir 'auto_backup.log'

function Log($m) {
    $ts = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    Add-Content -Path $log -Value "[$ts] $m" -Encoding utf8
    Write-Host "[$ts] $m"
}

try {
    Set-Location $repo

    # 1) Snapshot working tree into a temp index (real index/HEAD untouched, .gitignore applied)
    $idx = Join-Path $repo '.git\backup_index.tmp'
    if (Test-Path $idx) { Remove-Item $idx -Force }
    $env:GIT_INDEX_FILE = $idx
    git read-tree HEAD
    git add -A
    $tree = (git write-tree).Trim()
    Remove-Item Env:\GIT_INDEX_FILE
    if (Test-Path $idx) { Remove-Item $idx -Force }
    if (-not $tree) { Log 'FAIL: write-tree returned nothing'; exit 1 }

    # 2) Pre-push secret-pattern scan over the whole tree (tracked + untracked)
    $patterns = @(
        'ntn_[A-Za-z0-9]{20,}',
        'secret_[A-Za-z0-9]{30,}',
        '[0-9]{8,10}:[A-Za-z0-9_-]{35}',
        'sk-ant-[A-Za-z0-9_-]{20,}',
        '1[A-Za-z0-9+/=]{200,}'
    )
    foreach ($p in $patterns) {
        $hit = git grep -nI -E $p $tree
        if ($hit) {
            Log "ABORT: secret pattern matched [$p] -- not pushing. Manual review needed."
            exit 1
        }
    }

    # 3) Create snapshot commit on top of backup/auto (parent = HEAD on first run)
    $prev = git rev-parse --verify --quiet refs/heads/backup/auto
    if (-not $prev) { $prev = git rev-parse HEAD }
    $prev = "$prev".Trim()
    $stamp = Get-Date -Format 'yyyy-MM-dd HH:mm'
    $commit = ("auto-backup: $stamp" | git commit-tree $tree -p $prev).Trim()
    if (-not $commit) { Log 'FAIL: commit-tree returned nothing'; exit 1 }
    git update-ref refs/heads/backup/auto $commit

    # 4) Push to GitHub
    git push -q origin backup/auto
    if ($LASTEXITCODE -ne 0) {
        Log "FAIL: push rejected (exit $LASTEXITCODE) -- push protection may have caught a secret. Check log/remote message."
        exit 1
    }

    $n = (git ls-tree -r --name-only $tree | Measure-Object -Line).Lines
    Log "OK: backup/auto -> $($commit.Substring(0,10)) ($n files)"
}
catch {
    Log "FAIL (exception): $_"
    exit 1
}
