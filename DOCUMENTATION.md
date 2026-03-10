# Payment Validation Tool - Documentation

## Overview

Aplicacao local para conciliacao de pagamentos em afiliacao. O fluxo continua em pt-BR e preserva o motor de matching v2 onde ele segue valido:

- `click_id` primario
- `txn_id` secundario/fallback/disambiguador
- aliases customizados de status do anunciante
- inputs financeiros antes da execucao
- `match_audit` e export ajustado

O ponto central desta versao e o gating correto pelo status operacional do MASTER.

Tambem existem dois utilitarios operacionais separados dos fluxos de validacao:

- `Modo Postback` para preencher URLs de postback por upload ou grade manual
- `Modo Comissao` para gerar listas de valores a partir de total + quantidade

## MASTER Status Gating

### Candidatos ativos

Somente linhas do MASTER em:

- `pending`
- `declined`

entram como candidatas ativas para matching com o anunciante.

### Carry-forward

Linhas do MASTER ja em:

- `approved`

nao precisam de novo match. Elas entram no export final consolidado como carry-forward e ficam depois das novas aprovacoes.

### Excluidas desta validacao

Linhas do MASTER em:

- `paid`

ficam fora desta validacao:

- nao participam do matching ativo
- nao entram no `validated_export_<ts>.csv`
- nao entram no `validated_export_balanced_<ts>.csv`
- sao contadas nas metricas como transparência operacional

## Final Export Composition

O export final consolidado e:

1. novas aprovacoes por match vindas de `pending/declined`
2. carry-forward ja `approved` no MASTER

Nunca entram:

3. linhas `paid`

### Ordering

Ordem final obrigatoria:

1. novas aprovacoes por match
2. carry-forward ja aprovado no MASTER

## Export Sanitization

`affiliate_id` / `publisher_id` continuam disponiveis internamente quando necessario para:

- prioridade segura por publisher
- reconciliacao e balanced export
- ordenacao e agrupamentos internos
- auditoria e calculos internos

Mas existe agora uma camada central de sanitizacao antes da escrita dos arquivos de validacao entregues ao operador.

Arquivos finais publicos:

- `validated_export_<ts>.csv`
- `validated_export_balanced_<ts>.csv`
- `validated_export_payout_adjusted_<ts>.csv`

Esses arquivos nao expoem `affiliate_id` nem `publisher_id`.

Os previews desses exports na UI tambem usam a versao sanitizada.

Os arquivos auxiliares de operador:

- `match_audit_<ts>.csv`
- `needs_review_<ts>.csv`
- `diff_<ts>.csv`

tambem saem sem identificadores de affiliate/publisher nesta versao.

## Matching Rules

O motor v2 foi preservado, mas aplicado apenas aos candidatos ativos (`pending/declined`):

- `click_id` continua primario
- `txn_id` continua secundario/fallback
- status do anunciante continua obrigatorio por padrao
- aprovacao implicita continua opcional
- aliases customizados de status continuam suportados

## Complemento Manual na Validacao

### Objetivo

A aba de `Validacao` agora possui a secao `Complemento manual (pos-validacao)`.

Ela existe para anexar linhas diretamente aos arquivos finais de export sem interferir no motor de matching.

### Colunas predefinidas

- `click_id`
- `offer_id`
- `txn_id`
- `sub1`
- `sub2`
- `sub3`
- `sub4`
- `sale_amount`
- `revenue`
- `payout`
- `sale_currency`
- `status`
- `created`
- `conversion_id`

### Regra de negocio

As linhas do `Complemento manual`:

- nao participam do matching
- nao contam como evidencia do anunciante
- nao criam matches artificiais
- nao entram na base da reconciliacao
- nao sao alteradas pelo balanceamento
- nao sao alteradas pelo ajuste de payout

### Regra de export

Depois que o dataset normal de validacao e produzido, o app anexa as linhas manuais ao final de:

- `validated_export_manual_append_<ts>.csv`
- `validated_export_balanced_manual_append_<ts>.csv`
- `validated_export_payout_adjusted_manual_append_<ts>.csv`

O export base validado continua preservado em seu arquivo original.

Essas linhas entram sempre no final das variantes derivadas e permanecem inalteradas.

### UI do complemento

- a secao so aparece depois da validacao principal
- `Modo de entrada`: `Editar/colar manualmente` ou `Subir arquivo`
- grade editavel com colunas predefinidas
- upload de CSV/XLSX com o mesmo schema esperado
- preview da quantidade de linhas manuais prontas para anexacao
- botao para anexar o complemento aos exports derivados
- botao para limpar/remover o complemento aplicado

## Ajuste de Payout na Validacao

### Fluxo correto

O `Ajuste de payout` agora acontece apenas em `pos-validacao`.

O operador primeiro:

1. roda a validacao
2. ve metricas e diferencas
3. decide se quer aplicar ajuste

### Regra operacional

O ajuste:

- usa o snapshot ja validado
- nao reexecuta o matching
- nao altera o export base original
- gera uma variante explicita de export ajustado
- pode gerar tambem a variante `ajustado + complemento manual` se houver linhas manuais aplicadas

## MASTER Operational Outcomes in Audit

Se uma linha do anunciante apontar para uma linha do MASTER ja `approved`, o audit registra:

- `ALREADY_APPROVED_IN_MASTER`
- `MASTER_STATUS_NOT_ELIGIBLE_FOR_MATCH`

e a linha segue apenas como carry-forward no export final.

Se uma linha do anunciante apontar para uma linha do MASTER ja `paid`, o audit registra:

- `MASTER_ALREADY_PAID_EXCLUDED`
- `MASTER_STATUS_NOT_ELIGIBLE_FOR_MATCH`

e ela fica fora do export final e do balanced export.

Se uma linha ativa (`pending/declined`) for aprovada por match, o audit registra:

- `NEWLY_APPROVED_FROM_PENDING_OR_DECLINED`

## Financial Reconciliation Base

A validacao sempre gera primeiro o export final consolidado, sem equilibrio:

- `validated_export_<ts>.csv`

Depois disso, a reconciliacao financeira fica disponivel como acao opcional do operador.
Somente quando o operador clica em `Gerar export equilibrado` o app gera:

- `validated_export_balanced_<ts>.csv`

O arquivo original consolidado e preservado e nunca e sobrescrito.

Se o operador ja tiver aplicado `Complemento manual`, o app pode gerar tambem:

- `validated_export_balanced_manual_append_<ts>.csv`

Nesse caso, as linhas manuais entram no final do arquivo equilibrado sem sofrer alteracao.

A reconciliacao financeira e o balanced export usam somente a populacao correta:

- novas aprovacoes por match
- carry-forward ja aprovado
- exclusao total de `paid`

Isso evita contaminar a base financeira com linhas do MASTER que ja estavam pagas.

## Balanced Export Rules

O balanced export continua usando redistribuicao segura:

- 2 casas decimais
- nenhum valor negativo
- nenhum valor zerado
- piso minimo por linha, `1.00` por padrao
- reducao priorizando os maiores payouts quando o alvo cai
- redistribuicao deterministica quando o alvo sobe
- warning claro se o alvo exato for impossivel sob o piso

## Modo Postback

### Objetivo

O `Modo Postback` preenche templates de postback linha a linha usando os valores da propria linha, seja por planilha enviada pelo operador ou por grade manual editavel.

### Formatos aceitos

- CSV
- XLSX
- XLSM
- XLS
- grade editavel na propria UI

### Estrutura esperada

A planilha deve ter uma coluna por variavel, por exemplo:

- `VALOR`
- `CLICK`
- `TRANSACTION`
- `POSTBACK` opcional

### Fluxos suportados

#### Template por linha

Cada linha usa sua propria coluna `POSTBACK` como template.

Exemplo:

- `click_id=CLICK`
- `rate=VALOR`
- `txn_id=TRANSACTION`

Cada token e preenchido com o valor daquela mesma linha.

#### Template unico

O operador cola um template unico na UI.

Esse mesmo template e aplicado a todas as linhas, sempre usando os valores da linha corrente.

#### Montagem manual / grade editavel

O operador pode alternar o `Modo de entrada` para `Editar/colar dados manualmente`.

A UI usa uma grade editavel com suporte a colar linhas diretamente da area de transferencia.

Colunas base da grade:

- `VALOR`
- `CLICK`
- `TRANSACTION`
- `POSTBACK`

As linhas totalmente vazias sao ignoradas na geracao.

### Refino de UX da grade

Para reduzir a disputa entre a barra de rolagem e a alca de preenchimento no canto inferior direito:

- a area visivel da grade foi ampliada antes do scroll vertical
- a coluna `POSTBACK` ficou mais larga
- existe o atalho `Duplicar valor para baixo` como fallback operacional

Esse atalho replica o valor de uma linha para as linhas abaixo dentro da coluna escolhida, inclusive criando linhas vazias adicionais quando necessario.

### Regra de substituicao

O preenchimento e deterministico e case-insensitive no matching dos nomes de colunas.

Suportes implementados:

- placeholders embrulhados, como `{CLICK}` e `{{CLICK}}`
- valores de parametros de query que sejam exatamente iguais ao nome da coluna, como `click_id=CLICK`

O app nao tenta substituir substrings arbitrarias no meio de outros textos. Isso reduz o risco de corromper partes nao relacionadas da URL.

Se um placeholder nao tiver coluna correspondente:

- o token original e mantido
- a linha recebe status `PLACEHOLDER_SEM_COLUNA`
- o detalhe vai para `POSTBACK_WARNINGS`

### Colunas de saida

- `POSTBACK_FINAL`
- `POSTBACK_STATUS`
- `POSTBACK_WARNINGS`

Status previstos:

- `OK`
- `PLACEHOLDER_SEM_COLUNA`
- `TEMPLATE_VAZIO`
- `ERRO_DE_PROCESSAMENTO`

### UI do modo

Secoes principais:

1. `Modo de entrada`
2. `Upload de planilha` ou `Montagem manual`
3. `Modo de template`
4. `Gerar postbacks`
5. `Pre-visualizacao`
6. `Copiar coluna POSTBACK_FINAL`
7. `Baixar arquivo processado`

### Copia operacional de POSTBACK_FINAL

Depois da geracao, o app monta um bloco com todos os valores da coluna `POSTBACK_FINAL`, um por linha.

Padrao da UI:

- botao `Copiar coluna POSTBACK_FINAL`
- area de texto preenchida com os mesmos valores para fallback manual via `Ctrl+C`

Isso facilita colar o resultado em outra planilha ou ferramenta operacional.

## Modo ID

### Objetivo

O `Modo ID` continua focado em gerar IDs novos a partir de exemplos, mas agora com fidelidade estrutural maior.

### Refinos de inferencia

O gerador passou a:

- tratar caracteres estaveis como literais, inclusive quando sao letras ou numeros
- preservar separadores e substrings fixas compartilhadas entre os exemplos
- respeitar rigidamente a classe por posicao sempre que o padrao indicar isso
- manter blocos repetidos, como zeros em serie, quando eles aparecem como parte estavel do molde
- priorizar primeiro os caracteres realmente observados em cada posicao antes de ampliar para o pool completo da mesma classe

### Resultado pratico

Isso reduz casos em que:

- uma letra virava numero sem justificativa
- um prefixo fixo passava a variar demais
- blocos como `000000` eram perdidos
- os IDs gerados pareciam aleatorios demais em comparacao com os exemplos

## Modo Comissao

### Objetivo

O `Modo Comissao` gera uma lista de valores de comissao a partir de:

- `Valor total`
- `Quantidade de linhas`
- `Modo de geracao`

O resultado tenta manter soma exata do alvo com 2 casas decimais sempre que isso for matematicamente possivel sob o piso minimo configurado.

### Inputs

- `Valor total`
- `Quantidade de linhas`
- `Modo de geracao`: `Exato` ou `Media`
- `Valor minimo por linha`
- `Seed da variacao`

Piso sugerido por padrao:

- `1.00` por linha

Esse piso foi mantido como default por ser mais aderente ao uso operacional de comissao, mas pode ser reduzido quando necessario.

### Regra do modo Exato

1. o total e convertido para centavos
2. o valor base por linha e calculado por divisao inteira
3. o resto em centavos e distribuido das primeiras linhas para as ultimas, 1 centavo por vez
4. isso garante fechamento deterministico da soma com 2 casas decimais

Se o total nao comportar o piso minimo por linha na quantidade pedida, o app avisa e gera o menor total valido possivel respeitando o piso.

### Regra do modo Media

1. o app reserva primeiro o piso minimo para todas as linhas
2. o saldo restante em centavos e distribuido com pesos deterministas gerados a partir da seed
3. esses pesos ficam limitados a uma faixa moderada para evitar outliers absurdos
4. apos o arredondamento para baixo, os centavos restantes sao redistribuidos pelas maiores fracoes ate fechar a soma
5. o resultado final fica visualmente variado, positivo e com 2 casas decimais

Se o total nao comportar o piso minimo por linha, o app avisa e gera o menor total valido possivel.

### Saida

- preview da tabela com `indice` e `valor_gerado`
- `Total gerado`
- `Quantidade gerada`
- `Diferenca vs alvo`
- bloco copiavel com um valor por linha
- `Baixar CSV`

### Copia operacional de valores

O app aplica o mesmo padrao ergonomico do `Modo Postback`:

- botao `Copiar valores`
- area de texto preenchida para copia manual se necessario

## Safe Publisher Priority

Campos pre-run:

- `Publisher prioritario`
- `Percentual de prioridade`

Escopo permitido:

- ordenar casos em revisao
- desempatar entre linhas ja elegiveis
- enviesar apenas a redistribuicao positiva do balanced export quando houver folga real

Escopo proibido:

- aprovar linhas sem match valido
- fabricar aprovacoes
- aumentar payout fora da base real de reconciliacao

## Metrics in UI

As metricas principais agora separam explicitamente:

- `Linhas do MASTER`
- `Pendentes no MASTER`
- `Declinadas no MASTER`
- `Ja aprovadas no MASTER`
- `Ja pagas no MASTER (fora desta validacao)`
- `Novamente aprovadas por match`
- `Linhas exportadas finais`
- `Linhas em revisao`
- `Excluidas por status paid no MASTER`

## Preview vs Full Download

As tabelas exibidas na tela sao apenas pre-visualizacoes parciais.

Mensagem exibida:

`Visualizacao parcial para conferencia. O arquivo baixado contem todas as linhas.`

O arquivo baixado continua contendo todas as linhas.

Na area de downloads, a distincao principal e:

- `Export final consolidado (sem equilibrio) (CSV)`
- `Export equilibrado (CSV)` apenas depois da acao explicita

## Output Files

| Arquivo | Descricao |
|---|---|
| `validated_export_<ts>.csv` | Export base validado sem equilibrio: novas aprovacoes + carry-forward aprovado |
| `validated_export_manual_append_<ts>.csv` | Export base validado + `Complemento manual` anexado ao final |
| `validated_export_balanced_<ts>.csv` | Segundo arquivo opcional, gerado apenas apos `Gerar export equilibrado` |
| `validated_export_balanced_manual_append_<ts>.csv` | Export equilibrado + `Complemento manual` anexado ao final sem alteracao |
| `validated_export_payout_adjusted_<ts>.csv` | Export com ajuste de payout em pos-validacao |
| `validated_export_payout_adjusted_manual_append_<ts>.csv` | Export com ajuste de payout + `Complemento manual` anexado ao final sem alteracao |
| `match_audit_<ts>.csv` | Auditoria de match e elegibilidade do MASTER, sem identificadores de affiliate/publisher |
| `needs_review_<ts>.csv` | Casos que realmente exigem revisao, sem identificadores de affiliate/publisher |
| `diff_<ts>.csv` | Mudancas de status nas linhas ativas do MASTER, sem identificadores de affiliate/publisher |
| `postback_preenchido_<ts>.csv` | Arquivo baixado no `Modo Postback`, com colunas originais mais `POSTBACK_FINAL`, `POSTBACK_STATUS` e `POSTBACK_WARNINGS` |
| `comissao_gerada_<ts>.csv` | Arquivo baixado no `Modo Comissao`, com `indice` e `valor_gerado` |
| `log_<ts>.txt` | Log legivel |
| `log_<ts>.json` | Log estruturado |

### match_audit columns

`advertiser_row_index`, `extracted_click_id`, `extracted_txn_id`, `all_found_click_ids`, `all_found_txn_ids`, `non_empty_cell_count`, `matched_master_index`, `matched_by`, `raw_status_detected`, `normalized_status`, `confidence`, `issue_codes`, `advertiser_commission_value`, `master_revenue`, `decision`, `diagnostic_hint`, `master_status_before`

## Setup

```bash
python -m venv .venv
source .venv/bin/activate      # macOS/Linux
.venv\Scripts\activate         # Windows
pip install -r requirements.txt
streamlit run app.py
```

## Manual Validation Checklist

1. Monte um MASTER com ao menos uma linha `pending`, uma `declined`, uma `approved` e uma `paid`.
2. Rode um anunciante que aprove a linha `pending` ou `declined`.
3. Confirme que ela entra como `NEWLY_APPROVED_FROM_PENDING_OR_DECLINED`.
4. Confirme que a linha `approved` entra no export final mesmo sem depender do match.
5. Confirme que a linha `paid` nao entra no `validated_export` nem no `validated_export_balanced`.
6. Confirme que a ordem final do export e: novas aprovacoes primeiro, carry-forward depois.
7. Rode um caso com `Publisher prioritario` e confirme que ele so afeta revisao/desempate/redistribuicao positiva, sem aprovar linhas invalidas.
8. No `Modo Postback`, valide o fluxo por upload e o fluxo manual com a grade editavel.
9. Confirme que `POSTBACK_FINAL` pode ser copiado pelo botao e tambem pela area de texto.
10. No `Modo Comissao`, gere um caso `Exato` e confirme soma exata.
11. No `Modo Comissao`, gere um caso `Media` e confirme variacao positiva com soma final fechando o total.
12. Na `Validacao`, execute primeiro a validacao principal e confirme que `Ajuste de payout (pos-validacao)` e `Complemento manual (pos-validacao)` so aparecem depois disso.
13. Em `Complemento manual (pos-validacao)`, teste `Editar/colar manualmente` e confirme que o app gera `validated_export_manual_append_<ts>.csv`.
14. Em `Complemento manual (pos-validacao)`, teste `Subir arquivo` com o mesmo schema e confirme o mesmo comportamento de anexacao.
15. Gere tambem o export equilibrado e confirme que as linhas do `Complemento manual` continuam no final sem alteracao.
16. Gere um ajuste de payout e confirme que o export base permanece preservado e que a variante ajustada sai em arquivo separado.
17. Com ajuste + complemento manual aplicados, confirme a existencia da variante `validated_export_payout_adjusted_manual_append_<ts>.csv`.
