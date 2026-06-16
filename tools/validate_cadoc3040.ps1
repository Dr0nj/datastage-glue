# =============================================================================
# validate_cadoc3040.ps1 - Validacao local (Windows/PowerShell 5.1) dos XMLs
# CADOC 3040 gerados pelo job Glue, ANTES de rodar o validador oficial Bacen.
#
# Checa:
#   [S81] CNPJ do header == 8 primeiros digitos do IPOC de toda <Op>
#   [B01] Ordem dos filhos de <Op>: Venc -> Gar -> Inf -> ContInstFinRes4966
#   [C83] Op com EstInstFin sem <Estagio Motivo= DtAlocacao=> e sem Inf 03xx (contagem)
#   [I13] Cliente com soma de vencimentos < R$200 sem Inf 03xx (contagem estimada)
#   [RUNOFF]  conta (pos 592='S') do downdircontas vs arquivo certo (BASE x PPBANK)
#   [CTA_DIA] conta com status B,H,I,J,L,M,Q,S,U,Y,X (pos 83/84) ou atraso>5
#             (pos 171,5) + Mod=1904 NAO pode existir em nenhum XML
#   [ESTRUTURA] arquivo termina em </Doc3040>
#
# Uso: edite os 3 caminhos abaixo e rode:
#   powershell -ExecutionPolicy Bypass -File .\validate_cadoc3040.ps1
# Deixe '' nos arquivos que nao quiser validar.
# AVISO: o XML BASE de ~19GB demora bastante em PowerShell (linha a linha);
#        o PPBANK valida rapido. Para o arquivo gigante, prefira o log de
#        contagem dentro do proprio job (sugestao no README).
# =============================================================================

# ============ CONFIG - EDITE AQUI ============
$XmlPpbank = 'C:\Users\TC163068\Downloads\CADOC3040_xml_PPBANK_TESTAR.xml'
$XmlBase   = ''   # ex.: 'C:\...\CADOC3040_xml_20260531.xml' (19GB = demorado)
$Downdir   = ''   # ex.: 'C:\...\downdircontas_20260531.txt'
# =============================================

$ErrorActionPreference = 'Stop'
$ci = [System.Globalization.CultureInfo]::InvariantCulture

function Get-XmlAttr([string]$line, [string]$name) {
    $k = $name + '="'
    $i = $line.IndexOf($k)
    if ($i -lt 0) { return $null }
    $i += $k.Length
    $j = $line.IndexOf('"', $i)
    if ($j -lt 0) { return $null }
    return $line.Substring($i, $j - $i)
}

# -----------------------------------------------------------------------------
# 1) downdircontas -> conjuntos RunOff / exclusao CTA_DIA (regra do analista)
# -----------------------------------------------------------------------------
$runoffSet  = New-Object 'System.Collections.Generic.HashSet[string]'
$exclSet    = New-Object 'System.Collections.Generic.HashSet[string]'
$statusList = @('B','H','I','J','L','M','Q','S','U','Y','X')

if ($Downdir -ne '' -and (Test-Path $Downdir)) {
    Write-Host "`n== [1/2] Lendo downdircontas: $Downdir" -ForegroundColor Cyan
    $ddSamples = @()
    $sr = New-Object System.IO.StreamReader($Downdir)
    $n = 0; $skipped = 0
    while ($null -ne ($line = $sr.ReadLine())) {
        $n++
        if ($line.Length -lt 175) { $skipped++; continue }
        $org = $line.Substring(0, 3)
        if ($org -eq '000' -or $org -eq '999') { $skipped++; continue }   # header/trailer
        $contaRaw = $line.Substring(6, 19)        # DIA_CONTA pos 7, tam 19 (1-based)
        $conta = $contaRaw.Trim()
        if ($conta.Length -eq 0) { continue }
        if ($ddSamples.Count -lt 3) { $ddSamples += ('[' + $contaRaw + ']') }
        if ($line.Length -ge 592) {               # RunOff: pos 592, tam 1 == 'S'
            if ($line.Substring(591, 1).ToUpper() -eq 'S') { [void]$runoffSet.Add($conta) }
        }
        $s1 = $line.Substring(82, 1).ToUpper()    # DLDIA_STATUS    pos 83
        $s2 = $line.Substring(83, 1).ToUpper()    # DLDIA_STATUS_2  pos 84
        $atrRaw = ($line.Substring(170, 5) -replace '[^0-9-]', '')   # DLDIA_DIAS_ATRASO pos 171,5
        $atr = 0
        [void][int]::TryParse($atrRaw, [ref]$atr)
        if (($statusList -contains $s1) -or ($statusList -contains $s2) -or ($atr -gt 5)) {
            [void]$exclSet.Add($conta)
        }
    }
    $sr.Close()
    Write-Host ("   linhas lidas: {0:N0} | ignoradas (curta/header/trailer): {1:N0}" -f $n, $skipped)
    Write-Host ("   contas RunOff (pos592='S')............: {0:N0}" -f $runoffSet.Count)
    Write-Host ("   contas exclusao CTA_DIA (status/atraso): {0:N0}" -f $exclSet.Count)
    Write-Host ("   amostra do campo conta CRU (pos 7-25, entre []): {0}" -f ($ddSamples -join '  '))
    Write-Host  "   ^ compare o formato (zeros a esquerda? espacos?) com a amostra de Contrt do XML abaixo"
} else {
    Write-Host "`n== [1/2] downdircontas nao informado - checks RUNOFF/CTA_DIA serao pulados" -ForegroundColor Yellow
}

# -----------------------------------------------------------------------------
# 2) Scanner de XML CADOC 3040 (streaming, linha a linha)
# -----------------------------------------------------------------------------
function Scan-Cadoc([string]$Path, [string]$Kind) {   # Kind: 'PPBANK' | 'BASE'
    Write-Host "`n== [2/2] Escaneando $Kind : $Path" -ForegroundColor Cyan
    $sr = New-Object System.IO.StreamReader($Path)
    $headerCnpj = $null
    $cli = 0; $op = 0; $lineNo = 0
    $ipocMism = 0;  $ipocEx  = @()
    $ordViol  = 0;  $ordEx   = @()
    $c83 = 0;       $c83Ex   = @()
    $i13 = 0;       $i13Ex   = @()
    $runWrong = 0;  $runWrongEx = @()
    $runMatch = 0
    $ctaViol  = 0;  $ctaEx   = @()
    $contaSamples = @()
    $lastLine = ''
    # estado por Op / Cli
    $rank = 0
    $opHasEst = $false; $opEstagioOK = $false; $opInf03 = $false; $opLine = 0
    $cliSum = [decimal]0; $cliInf03 = $false; $cliCd = ''; $cliLine = 0

    while ($null -ne ($line = $sr.ReadLine())) {
        $lineNo++
        if (($lineNo % 1000000) -eq 0) { Write-Host ("   ... {0:N0} linhas" -f $lineNo) }
        $t = $line.TrimStart()
        if ($t.Length -eq 0) { continue }
        $lastLine = $t

        if ($null -eq $headerCnpj -and $t.StartsWith('<Doc3040')) {
            $headerCnpj = Get-XmlAttr $t 'CNPJ'
            Write-Host ("   header CNPJ = {0}" -f $headerCnpj)
            continue
        }
        if ($t.StartsWith('<Cli')) {
            $cli++
            $cliSum = [decimal]0; $cliInf03 = $false; $cliLine = $lineNo
            $cliCd = Get-XmlAttr $t 'Cd'
            continue
        }
        if ($t.StartsWith('<Op')) {
            $op++
            $rank = 0; $opHasEst = $false; $opEstagioOK = $false; $opInf03 = $false; $opLine = $lineNo
            # ---- S81: prefixo do IPOC == CNPJ do header
            $ipoc = Get-XmlAttr $t 'IPOC'
            if ($null -ne $headerCnpj -and $null -ne $ipoc -and $ipoc.Length -ge 8) {
                if ($ipoc.Substring(0, 8) -ne $headerCnpj) {
                    $ipocMism++
                    if ($ipocEx.Count -lt 5) { $ipocEx += ("linha {0}: IPOC inicia '{1}'" -f $lineNo, $ipoc.Substring(0, 8)) }
                }
            }
            # ---- conta (espelha _clean_account do job: trim + 19 primeiros chars)
            $contrt = Get-XmlAttr $t 'Contrt'
            $mod    = Get-XmlAttr $t 'Mod'
            $conta  = $null
            if ($null -ne $contrt) {
                $c = $contrt.Trim()
                if ($c.Length -gt 19) { $c = $c.Substring(0, 19) }
                $conta = $c
                if ($contaSamples.Count -lt 3) { $contaSamples += ('[' + $c + ']') }
            }
            if ($null -ne $conta) {
                # ---- RUNOFF: conta com 'S' deve estar APENAS no arquivo BASE
                if ($runoffSet.Count -gt 0) {
                    $inRun = $runoffSet.Contains($conta)
                    if ($inRun) { $runMatch++ }
                    $wrong = $false
                    if ($Kind -eq 'PPBANK' -and $inRun) { $wrong = $true }
                    if ($Kind -eq 'BASE' -and -not $inRun) { $wrong = $true }
                    if ($wrong) {
                        $runWrong++
                        if ($runWrongEx.Count -lt 5) { $runWrongEx += ("linha {0}: conta={1}" -f $lineNo, $conta) }
                    }
                }
                # ---- CTA_DIA: conta excluida + Mod 1904 nao pode existir
                if ($exclSet.Count -gt 0 -and $mod -eq '1904' -and $exclSet.Contains($conta)) {
                    $ctaViol++
                    if ($ctaEx.Count -lt 5) { $ctaEx += ("linha {0}: conta={1}" -f $lineNo, $conta) }
                }
            }
            continue   # <Op ...></Op> sem filhos tambem cai aqui (sem checks de filho)
        }

        # ---- filhos de Op: ordem oficial Venc(1) Gar(2) Inf(3) 4966(4)
        $childRank = 0
        if ($t.StartsWith('<Venc')) {
            $childRank = 1
            foreach ($m in [regex]::Matches($t, 'v\d+="([0-9.\-]+)"')) {
                $v = [decimal]0
                if ([decimal]::TryParse($m.Groups[1].Value, [System.Globalization.NumberStyles]::Number, $ci, [ref]$v)) { $cliSum += $v }
            }
        }
        elseif ($t.StartsWith('<Gar')) { $childRank = 2 }
        elseif ($t.StartsWith('<Inf')) {
            $childRank = 3
            $tp = Get-XmlAttr $t 'Tp'
            if ($null -ne $tp -and $tp.StartsWith('03')) { $opInf03 = $true; $cliInf03 = $true }
        }
        elseif ($t.StartsWith('<ContInstFinRes4966')) {
            $childRank = 4
            if ($null -ne (Get-XmlAttr $t 'EstInstFin')) { $opHasEst = $true }
        }
        elseif ($t.StartsWith('<Estagio')) {
            $mo = Get-XmlAttr $t 'Motivo'
            $dt = Get-XmlAttr $t 'DtAlocacao'
            if ($null -ne $mo -and $null -ne $dt) { $opEstagioOK = $true }
            continue
        }
        elseif ($t.StartsWith('</Op>')) {
            # ---- C83
            if ($opHasEst -and -not $opEstagioOK -and -not $opInf03) {
                $c83++
                if ($c83Ex.Count -lt 5) { $c83Ex += ("Op da linha {0}" -f $opLine) }
            }
            continue
        }
        elseif ($t.StartsWith('</Cli>')) {
            # ---- I13 (estimativa: soma de TODOS os vNNN do cliente)
            if (-not $cliInf03 -and $cliSum -lt 200) {
                $i13++
                if ($i13Ex.Count -lt 5) { $i13Ex += ("Cli Cd={0} soma={1} (linha {2})" -f $cliCd, $cliSum, $cliLine) }
            }
            continue
        }
        else { continue }

        if ($childRank -gt 0) {
            if ($childRank -lt $rank) {
                $ordViol++
                $frag = $t.Substring(0, [Math]::Min(25, $t.Length))
                if ($ordEx.Count -lt 5) { $ordEx += ("linha {0}: '{1}' depois de elemento de ordem maior" -f $lineNo, $frag) }
            }
            if ($childRank -gt $rank) { $rank = $childRank }
        }
    }
    $sr.Close()

    # ------------------------- RESUMO -------------------------
    Write-Host ("`n   ===== RESUMO {0} =====" -f $Kind) -ForegroundColor White
    Write-Host ("   Cli: {0:N0} | Op: {1:N0} | linhas: {2:N0}" -f $cli, $op, $lineNo)

    $endOk = $lastLine.Contains('</Doc3040>')
    if ($endOk) { Write-Host '   [ESTRUTURA] termina em </Doc3040> ............ PASS' -ForegroundColor Green }
    else        { Write-Host ("   [ESTRUTURA] NAO termina em </Doc3040> (ultima linha: '{0}') .. FAIL (arquivo truncado?)" -f $lastLine) -ForegroundColor Red }

    if ($ipocMism -eq 0) { Write-Host '   [S81] IPOC x CNPJ header ..................... PASS (0 divergencias)' -ForegroundColor Green }
    else { Write-Host ("   [S81] IPOC x CNPJ header ..................... FAIL ({0:N0} Ops divergentes)" -f $ipocMism) -ForegroundColor Red
           $ipocEx | ForEach-Object { Write-Host ("         ex: {0}" -f $_) -ForegroundColor Red } }

    if ($ordViol -eq 0) { Write-Host '   [B01] ordem Venc,Gar,Inf,4966 ................ PASS (0 violacoes)' -ForegroundColor Green }
    else { Write-Host ("   [B01] ordem Venc,Gar,Inf,4966 ................ FAIL ({0:N0} violacoes)" -f $ordViol) -ForegroundColor Red
           $ordEx | ForEach-Object { Write-Host ("         ex: {0}" -f $_) -ForegroundColor Red } }

    if ($runoffSet.Count -gt 0) {
        if ($runWrong -eq 0) { Write-Host ("   [RUNOFF] split correto ({0:N0} Ops casaram com contas RunOff) ... PASS" -f $runMatch) -ForegroundColor Green }
        else { Write-Host ("   [RUNOFF] {0:N0} Ops no arquivo ERRADO ........ FAIL" -f $runWrong) -ForegroundColor Red
               $runWrongEx | ForEach-Object { Write-Host ("         ex: {0}" -f $_) -ForegroundColor Red } }
        if ($Kind -eq 'BASE' -and $runMatch -eq 0 -and $op -gt 0) {
            Write-Host '   [RUNOFF] ATENCAO: NENHUMA conta casou - provavel problema de normalizacao (zeros a esquerda?)' -ForegroundColor Yellow
        }
        if ($Kind -eq 'PPBANK' -and $runoffSet.Count -gt 0 -and $runMatch -eq 0) {
            Write-Host '   [RUNOFF] info: nenhuma conta RunOff apareceu neste arquivo (esperado para PPBANK)' -ForegroundColor DarkGray
        }
    } else { Write-Host '   [RUNOFF] pulado (sem downdircontas)' -ForegroundColor Yellow }

    if ($exclSet.Count -gt 0) {
        if ($ctaViol -eq 0) { Write-Host '   [CTA_DIA] exclusao conta+Mod1904 ............. PASS (0 sobreviventes)' -ForegroundColor Green }
        else { Write-Host ("   [CTA_DIA] exclusao conta+Mod1904 ............. FAIL ({0:N0} Ops deveriam ter sido excluidas)" -f $ctaViol) -ForegroundColor Red
               $ctaEx | ForEach-Object { Write-Host ("         ex: {0}" -f $_) -ForegroundColor Red } }
    } else { Write-Host '   [CTA_DIA] pulado (sem downdircontas)' -ForegroundColor Yellow }

    Write-Host ("   [C83] Ops com EstInstFin sem Estagio (sem Inf 03xx): {0:N0}  <- pendencia de negocio" -f $c83) -ForegroundColor Yellow
    if ($c83 -gt 0) { $c83Ex | ForEach-Object { Write-Host ("         ex: {0}" -f $_) -ForegroundColor Yellow } }
    Write-Host ("   [I13] clientes com soma vencimentos < 200 (sem Inf 03xx): {0:N0}  <- pendencia de negocio (estimativa)" -f $i13) -ForegroundColor Yellow
    if ($i13 -gt 0) { $i13Ex | ForEach-Object { Write-Host ("         ex: {0}" -f $_) -ForegroundColor Yellow } }

    Write-Host ("   amostra Contrt do XML (19 chars, entre []): {0}" -f ($contaSamples -join '  '))
}

if ($XmlPpbank -ne '' -and (Test-Path $XmlPpbank)) { Scan-Cadoc $XmlPpbank 'PPBANK' }
elseif ($XmlPpbank -ne '') { Write-Host "ARQUIVO NAO ENCONTRADO: $XmlPpbank" -ForegroundColor Red }

if ($XmlBase -ne '' -and (Test-Path $XmlBase)) { Scan-Cadoc $XmlBase 'BASE' }
elseif ($XmlBase -ne '') { Write-Host "ARQUIVO NAO ENCONTRADO: $XmlBase" -ForegroundColor Red }

Write-Host "`nFIM." -ForegroundColor Cyan
