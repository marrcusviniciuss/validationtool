# Payment Validation Tool

Aplicacao local para conciliacao de pagamentos em marketing de afiliacao. Ela cruza o arquivo do anunciante com um MASTER, preservando a UI em pt-BR e o motor de matching v2 onde ele continua valido.

Tambem inclui:

- `Modo Postback` para preencher URLs de postback por upload de planilha ou grade manual editavel
- `Modo Comissao` para gerar listas de valores a partir de um total e de uma quantidade de linhas

## Quick Start

**Requisitos:** Python 3.11+

```bash
python -m venv .venv
source .venv/bin/activate      # macOS/Linux
.venv\Scripts\activate         # Windows

pip install -r requirements.txt
python -m streamlit run streamlit_app.py
```

Abre em `http://localhost:8501`.

## Execucao Local

Para rodar localmente:

```bash
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Se preferir, `app.py` continua sendo o app real e `streamlit_app.py` funciona como entrypoint fino para o deploy.

## Deploy no Streamlit Community Cloud

Configuracao recomendada deste repositorio:

- repositorio: `marrcusviniciuss/validationtool`
- branch: `main`
- main file path: `streamlit_app.py`

Passos:

1. conecte o GitHub ao Streamlit Community Cloud
2. selecione o repositorio `marrcusviniciuss/validationtool`
3. escolha a branch `main`
4. informe `streamlit_app.py` em `Main file path`
5. confirme o deploy

O projeto continua sendo um app nativo em Streamlit. O arquivo `streamlit_app.py` apenas chama o `main()` definido em `app.py`.

## Gating de Status do MASTER

O fluxo final agora separa o MASTER em tres grupos:

- `pending` e `declined`: candidatos ativos para matching com o anunciante.
- `approved`: carry-forward. Nao precisam de novo match, mas entram no export final.
- `paid`: excluidos desta validacao. Nao entram no export final nem no balanced export.

## Composicao Final do Export

O `validated_export_<ts>.csv` e composto por:

1. linhas `pending/declined` que foram novamente aprovadas por match valido
2. linhas ja `approved` no MASTER, carregadas como carry-forward

Nunca entram:

- linhas `paid`

Ordem final:

1. novas aprovacoes por match
2. carry-forward ja aprovado no MASTER

## Sanitizacao de Export

`affiliate_id` / `publisher_id` podem continuar sendo usados internamente para:

- prioridade segura por publisher
- reconciliacao e balanceamento
- ordenacao interna
- suporte de auditoria e calculos

Mas os CSVs finais do modulo de validacao nao expoem esses identificadores.

Arquivos finais sanitizados:

- `validated_export_<ts>.csv`
- `validated_export_balanced_<ts>.csv`
- `validated_export_payout_adjusted_<ts>.csv`

Os previews desses exports na UI tambem seguem a versao sanitizada.

Os arquivos auxiliares entregues ao operador (`match_audit`, `needs_review` e `diff`) tambem saem sem `publisher_id` / `affiliate_id`.

## Reconciliacao Financeira

Primeiro a validacao gera sempre o `validated_export_<ts>.csv`, sem equilibrio.

Depois, se o operador quiser, ele pode usar a reconciliacao financeira para gerar um segundo arquivo:

- `validated_export_balanced_<ts>.csv`

Esse balanced export e uma acao separada no pos-validacao, via botao `Gerar export equilibrado`.

O balanceamento usa somente a base correta do export final:

- novas aprovacoes por match
- carry-forward ja aprovado
- exclusao total de `paid`

Todos os valores monetarios visiveis e exportados saem com 2 casas decimais.

## Balanceamento

- nenhum payout pode ficar negativo
- nenhum payout pode ficar zerado
- existe piso minimo por linha, `1.00` por padrao
- quando o alvo e menor, os maiores payouts sao reduzidos primeiro
- quando o alvo e maior, a redistribuicao sobe de forma deterministica
- se o alvo exato for impossivel sob o piso, o app avisa e usa o total viavel mais proximo

## Prioridade Segura por Publisher

Campos pre-run:

- `Publisher prioritario`
- `Percentual de prioridade`

Escopo seguro apenas:

- ordenacao de casos em revisao
- desempate entre linhas ja elegiveis
- vies controlado na redistribuicao positiva do balanced export

Nao faz:

- nao aprova linhas sem match valido
- nao cria conversoes
- nao aumenta payout fora da reconciliacao real

## Modo Postback

O `Modo Postback` e um utilitario operacional separado dos fluxos de validacao.

Entradas aceitas:

- CSV
- XLSX / XLSM / XLS
- grade manual editavel na propria UI

Comportamento:

- cada linha usa os valores da propria linha para preencher placeholders
- o matching de placeholders e case-insensitive no nivel da logica
- os valores finais preservam o conteudo original da linha
- o arquivo processado e baixado em CSV
- a coluna `POSTBACK_FINAL` pode ser copiada em bloco, com um valor por linha

Fluxos suportados:

- `Usar coluna POSTBACK da planilha`: cada linha usa sua propria coluna `POSTBACK`
- `Usar template unico colado manualmente`: o mesmo template e aplicado a todas as linhas
- `Editar/colar dados manualmente`: usa uma grade editavel com colunas `VALOR`, `CLICK`, `TRANSACTION` e `POSTBACK`

Regra de substituicao:

- o app substitui placeholders embrulhados como `{CLICK}` ou `{{CLICK}}`
- o app tambem substitui valores de parametros de query que sejam exatamente iguais ao nome da coluna, como `click_id=CLICK` e `rate=VALOR`
- a substituicao sempre usa o valor da mesma linha
- se um placeholder nao tiver coluna correspondente, o token original e preservado e a linha recebe aviso

Colunas de saida:

- `POSTBACK_FINAL`
- `POSTBACK_STATUS`
- `POSTBACK_WARNINGS`

Status previstos:

- `OK`
- `PLACEHOLDER_SEM_COLUNA`
- `TEMPLATE_VAZIO`
- `ERRO_DE_PROCESSAMENTO`

Saida operacional:

- preview da tabela processada
- bloco copiavel da coluna `POSTBACK_FINAL`
- download CSV do resultado

## Modo Comissao

O `Modo Comissao` gera uma lista de valores cuja soma acompanha o total informado pelo operador.

Entradas principais:

- `Valor total`
- `Quantidade de linhas`
- `Modo de geracao`: `Exato` ou `Media`
- `Valor minimo por linha` (padrao operacional: `1.00`)

Modos suportados:

- `Exato`: divide o total em centavos entre as linhas, distribuindo qualquer resto de 1 centavo nas primeiras linhas ate fechar a soma
- `Media`: parte do piso minimo por linha e redistribui o saldo restante com pesos deterministas, gerando variacao visual sem perder a soma final quando isso e matematicamente possivel

Saida operacional:

- preview com `indice` e `valor_gerado`
- total gerado, quantidade e diferenca vs alvo
- bloco copiavel com um valor por linha
- download CSV

Se o total for pequeno demais para respeitar o piso minimo por linha na quantidade desejada, o app avisa e gera o menor total valido possivel.

## Arquivos Gerados

| Arquivo | Descricao |
|---|---|
| `validated_export_<ts>.csv` | Export final consolidado sem equilibrio |
| `validated_export_balanced_<ts>.csv` | Segundo arquivo opcional, gerado so apos clicar em `Gerar export equilibrado` |
| `validated_export_payout_adjusted_<ts>.csv` | Export com ajuste de payout |
| `match_audit_<ts>.csv` | Auditoria detalhada do matching e da elegibilidade do MASTER, sem `publisher_id` / `affiliate_id` |
| `needs_review_<ts>.csv` | Casos realmente enviados para revisao, sem `publisher_id` / `affiliate_id` |
| `diff_<ts>.csv` | Mudancas de status nas linhas ativas do MASTER, sem `publisher_id` / `affiliate_id` |
| `log_<ts>.txt` / `log_<ts>.json` | Logs da execucao |
| `postback_preenchido_<ts>.csv` | Download gerado no `Modo Postback`, contendo as colunas originais e `POSTBACK_FINAL` |
| `comissao_gerada_<ts>.csv` | Download gerado no `Modo Comissao`, com `indice` e `valor_gerado` |

## Preview x Download

As tabelas mostradas na tela sao previews parciais. O arquivo baixado contem todas as linhas do export correspondente.

Downloads principais na UI:

- `Export final consolidado (sem equilibrio) (CSV)`
- `Export equilibrado (CSV)` apenas depois da acao explicita do operador
- `Baixar arquivo processado (CSV)` no `Modo Postback`
- `Baixar CSV` no `Modo Comissao`

Veja [DOCUMENTATION.md](DOCUMENTATION.md) para a regra detalhada de gating, composicao do export, reconciliacao e prioridade segura por publisher.
