# ==============================
# Script: GAL-LDAPS-Powershell.ps1
# Purpose: Pull all active users by dynamically discovered ministry OUs (paged)
# Author: David Rattray
# ==============================

Add-Type -AssemblyName System.DirectoryServices.Protocols

$LdapHost   = "keel.idir.bcgov"
$Port       = 636
$OutputCSV  = "E:\Projects\rpd-server-operations\input\Powershell\gal_users.csv"
$LdapFilter = "(&(objectCategory=person)(objectClass=user)(!(userAccountControl:1.2.840.113556.1.4.803:=2)))"

$AttributesOfInterest = @(
    "distinguishedname",
    "samaccountname",
    "displayname",
    "givenname",
    "sn",
    "mail",
    "title",
    "department",
    "company",
    "telephonenumber",
    "physicaldeliveryofficename",
    "l",
    "st",
    "streetaddress",
    "postalcode",
    "userprincipalname",
    "employeeid",
    "bcgovaccountstatus",
    "bcgovaccounttype",
    "bcgovemploymenttype",
    "bcgovhrcity",
    "bcgovhrdepartmentid",
    "bcgovhrpositionnumber",
    "bcgovhrstatus",
    "bcgovhrcompany",
    "bcgovhrbusinessunit",
    "bcgovhrlocationcode",
    "mailboxorgcode",
    "whencreated",
    "whenchanged",
    "lastlogontimestamp"
)

$timestampFields = @("lastlogontimestamp", "whencreated", "whenchanged")

# ----------------------------
# Helper functions
# ----------------------------

function Convert-LdapValue {
    param($val)
    if ($val -is [byte[]]) {
        try {
            $str = [System.Text.Encoding]::UTF8.GetString($val).Trim()
            if ($str -match '^[\x20-\x7E\xA0-\xFF]+$') { return $str }
        } catch {}
        return "0x" + [BitConverter]::ToString($val).Replace("-", "")
    }
    return $val.ToString()
}

function Convert-FileTime {
    param([string]$raw)
    try {
        $ticks = [long]$raw
        if ($ticks -gt 0 -and $ticks -ne 9223372036854775807) {
            return [DateTime]::FromFileTimeUtc($ticks).ToString("yyyy-MM-dd HH:mm:ss")
        }
    } catch {}
    return ""
}

# ----------------------------
# Discover ministry OUs dynamically
# ----------------------------

function Get-MinistryOUs {
    param(
        [System.DirectoryServices.Protocols.LdapConnection]$Connection
    )

    $ous = New-Object System.Collections.Generic.List[string]

    # Ministries under OU=BCGOV
    $bcgovBase = "OU=BCGOV,DC=idir,DC=BCGOV"
    $req1 = New-Object System.DirectoryServices.Protocols.SearchRequest(
        $bcgovBase,
        "(objectClass=organizationalUnit)",
        [System.DirectoryServices.Protocols.SearchScope]::OneLevel,
        "distinguishedName"
    )

    $resp1 = $Connection.SendRequest($req1)
    foreach ($entry in $resp1.Entries) {
        $ous.Add($entry.DistinguishedName)
    }

    # Top-level ministries under the domain
    $rootBase = "DC=idir,DC=BCGOV"
    $req2 = New-Object System.DirectoryServices.Protocols.SearchRequest(
        $rootBase,
        "(objectClass=organizationalUnit)",
        [System.DirectoryServices.Protocols.SearchScope]::OneLevel,
        "distinguishedName"
    )

    $resp2 = $Connection.SendRequest($req2)
    foreach ($entry in $resp2.Entries) {
        if ($entry.DistinguishedName -notlike "OU=BCGOV,*") {
            $ous.Add($entry.DistinguishedName)
        }
    }

    return $ous | Sort-Object -Unique
}

# ----------------------------
# Paged user search (size‑limit safe)
# ----------------------------

function Search-OU-Paged {
    param(
        [System.DirectoryServices.Protocols.LdapConnection]$Connection,
        [string]$BaseDN,
        [string]$Filter,
        [string[]]$Attributes,
        [int]$PageSize = 2000
    )

    $pageRequest = New-Object System.DirectoryServices.Protocols.PageResultRequestControl($PageSize)
    $cookie = $null

    do {
        $pageRequest.Cookie = $cookie

        $searchRequest = New-Object System.DirectoryServices.Protocols.SearchRequest(
            $BaseDN,
            $Filter,
            [System.DirectoryServices.Protocols.SearchScope]::Subtree,
            $Attributes
        )

        $searchRequest.Controls.Add($pageRequest)
        $response = $Connection.SendRequest($searchRequest)

        foreach ($entry in $response.Entries) {
            $entry
        }

        $pageResponse = $response.Controls |
            Where-Object { $_ -is [System.DirectoryServices.Protocols.PageResultResponseControl] }

        $cookie = if ($pageResponse) { $pageResponse.Cookie } else { $null }

    } while ($cookie -and $cookie.Length -gt 0)
}

# ============================
# Connect
# ============================

# $Cred     = Get-Credential -Message "Enter your IDIR credentials"
# $netCred  = New-Object System.Net.NetworkCredential($Cred.UserName, $Cred.Password)
# auto connect method
$netCred = [System.Net.CredentialCache]::DefaultNetworkCredentials
$identifier = New-Object System.DirectoryServices.Protocols.LdapDirectoryIdentifier($LdapHost, $Port, $false, $false)

$conn = New-Object System.DirectoryServices.Protocols.LdapConnection(
    $identifier,
    $netCred,
    [System.DirectoryServices.Protocols.AuthType]::Negotiate
)

$conn.SessionOptions.SecureSocketLayer = $true
$conn.Timeout = [TimeSpan]::FromSeconds(60)
$conn.Bind()

Write-Host "Bind OK" -ForegroundColor Green

# ============================
# Discover ministries
# ============================

$MinistryOUs = Get-MinistryOUs -Connection $conn
Write-Host "Discovered $($MinistryOUs.Count) ministry OUs" -ForegroundColor Green

# ============================
# Export users
# ============================

$results = [System.Collections.Generic.List[PSObject]]::new()
$seenDNs = [System.Collections.Generic.HashSet[string]]::new()
$ouIndex = 0

foreach ($ou in $MinistryOUs) {
    $ouIndex++
    Write-Host "[$ouIndex/$($MinistryOUs.Count)] Searching: $ou" -ForegroundColor Cyan

    $ouCount = 0
    $entries = Search-OU-Paged -Connection $conn -BaseDN $ou -Filter $LdapFilter -Attributes $AttributesOfInterest

    foreach ($entry in $entries) {
        if (-not $seenDNs.Add($entry.DistinguishedName)) { continue }

        $row = [ordered]@{}
        if (-not $entry.Attributes) {
            continue   # skip objects with no readable attributes
            }

        foreach ($attr in $AttributesOfInterest) {
            if ($entry.Attributes.Contains($attr)) {
                $vals = @($entry.Attributes[$attr]) | ForEach-Object { Convert-LdapValue $_ }

                if ($attr -in $timestampFields) {
                    $row[$attr] = Convert-FileTime $vals[0]
                } else {
                    $row[$attr] = $vals -join "|"
                }
            } else {
                $row[$attr] = ""
            }
        }

        $results.Add([PSCustomObject]$row)
        $ouCount++
    }

    Write-Host "  -> $ouCount records added (running total: $($results.Count))" -ForegroundColor Gray
}

$conn.Dispose()

Write-Host "Writing $($results.Count) total records to CSV..." -ForegroundColor Cyan
$results | Export-Csv -Path $OutputCSV -NoTypeInformation -Encoding UTF8
Write-Host "Done: $OutputCSV" -ForegroundColor Green
