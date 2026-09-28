# Arquitetura

## Visão geral

```
  provedores / servico interno          produtor de mensagens
            | HTTP + Bearer JWT                  | SQS FIFO
            v                                    v
   +------------------+              wager-transactions.fifo --> DLQ
   | httpapi          |                          |
   |                  |<-------------------------+
   | messaging        |
   +------------------+
            | app (casos de uso) -> domain (regras)
            v
   PostgreSQL: wallets, wager_transactions, wallet_ledger_entries,
               inbox_messages, outbox_events
            |
            | relay da outbox (qualquer instancia)
            v
   wallet-events.fifo
```

O mesmo binário roda a API HTTP, o consumidor SQS, o worker de referências pendentes e o relay da outbox. Qualquer número de instâncias pode rodar ao mesmo tempo. Toda a coordenação passa pelo PostgreSQL, sem estado compartilhado em memória nem lock global.

Pacotes:

| Pacote | Conteúdo | Depende de |
| --- | --- | --- |
| `domain/money` | `Money` | stdlib |
| `domain` | `Wallet`, `WagerTransaction`, `LedgerEntry`, eventos, `Settle` | stdlib, `uuid` |
| `app` | casos de uso, relay da outbox, interfaces de repositório | `domain` |
| `postgres`, `messaging`, `httpapi`, `auth` | adaptadores | `app`, `domain` |
| `wiring` | composição Fx, único pacote que importa Fx | todos |

O domínio não conhece Fx, HTTP, SQS nem o driver do banco.

## Dinheiro

`Money` guarda `int64` em unidades mínimas (centavos) e a moeda. É imutável: toda operação devolve um valor novo.

- Escala fixa de 2 casas. As moedas aceitas são códigos ISO 4217 com duas casas decimais: BRL, USD, EUR, GBP, ARS, MXN, CAD e AUD. JPY, por exemplo, é recusado.
- Limite: de -92.233.720.368.547.758,08 a 92.233.720.368.547.758,07. `Add`, `Sub` e `Neg` detectam overflow e devolvem `ErrOverflow`. O parsing usa `strconv.ParseInt` sobre os dígitos, então um valor acima do limite é recusado, não truncado.
- Entrada externa: só a forma canônica `^(0|[1-9][0-9]*)\.[0-9]{2}$`. São recusados vazio, sinal, `NaN`, `Infinity`, notação científica, mais ou menos de duas casas, zeros à esquerda e espaços. Nada é arredondado nem normalizado, então o texto recebido já é a forma usada no hash.
- O valor precisa ser string no JSON. Um número JSON (`25.00` sem aspas) é recusado; nenhum decoder transforma dinheiro em `float64`.
- Operações e comparações entre moedas diferentes devolvem `ErrCurrencyMismatch`. O valor zero de `Money`, sem moeda, é recusado por todas as operações.
- Persistência: `BIGINT` com o sufixo `_minor` mais uma coluna `char(3)` de moeda. A soma da reconciliação é feita em SQL e convertida de volta para `bigint`, o que falha se estourar em vez de perder precisão.
- Valores negativos só existem internamente, por exemplo em `difference` da reconciliação. O saldo da carteira nunca fica negativo (domínio e `CHECK` no banco).

## Transações SQL e repositórios

Acesso com `pgx/v5` e SQL explícito. O `TxManager` abre a transação e a guarda no `context`. Os repositórios usam a transação do contexto quando ela existe, e o pool quando não existe. O caso de uso decide a fronteira; um `WithinTx` aninhado reaproveita a transação externa.

| Caso de uso | Uma única transação contém |
| --- | --- |
| Abrir carteira | carteira, transação `OPENING`, lançamento e eventos |
| Operação por HTTP | lock da carteira, transação, novo saldo, lançamento, eventos, despertar pendências |
| Operação por SQS | registro na inbox, o mesmo que o HTTP e a conclusão na inbox |
| Resolver pendência | lock da carteira, lock da transação pendente, resultado, saldo, lançamento, eventos |
| Reconciliação | leitura `REPEATABLE READ READ ONLY` (mesma snapshot para saldo e ledger) |

Isolamento `READ COMMITTED` com lock explícito. Cada conexão define `lock_timeout` (5 s), `statement_timeout` (15 s), `idle_in_transaction_session_timeout` e `connect_timeout`. A API também limita cada requisição (`REQUEST_TIMEOUT`).

Se o commit falha por perda de conexão, o resultado é incerto. O serviço responde 503 e o cliente reenvia com a mesma chave: se o commit aconteceu, recebe o replay; se não, a operação roda pela primeira vez.

## Concorrência

A coordenação é por carteira, com lock pessimista:

1. `SELECT ... FROM wallets WHERE id = $1 FOR UPDATE` serializa as operações de uma carteira entre todos os processos.
2. O domínio calcula o novo saldo e a versão.
3. `UPDATE wallets ... WHERE id = $1 AND version = $anterior` é uma segunda barreira. Com o lock ela nunca falha, mas um caminho que esquecesse o lock perderia a corrida em vez de sobrescrever.
4. O banco recusa saldo negativo, versão fora de sequência e mudança de saldo sem lançamento (ver abaixo).

Carteiras diferentes não disputam nada. O teste `TestWalletsProgressIndependently` segura o lock de uma carteira numa transação aberta e mostra outra carteira sendo processada normalmente.

Escolhi lock pessimista em vez de controle otimista porque o caso crítico é justamente a disputa pela mesma carteira (duas apostas de 80.00 sobre 100.00, 50 cópias da mesma aposta). Com retry otimista, a maioria das tentativas viraria conflito e retry; com o lock, cada requisição espera a vez e decide com o saldo já confirmado.

Ordem de locks, para não haver deadlock: sempre carteira primeiro, depois a linha da transação pendente. O despertar de pendências usa `FOR UPDATE SKIP LOCKED`, então nunca espera uma linha que um worker já segura.

Corridas pela mesma chave de idempotência que chegam a gravar ao mesmo tempo esbarram nos índices únicos. O perdedor recebe `ErrConcurrentWrite`, a transação inteira é refeita (até 3 vezes) e, na nova tentativa, encontra a linha do vencedor e devolve o replay.

## Invariantes no banco

As regras não dependem do código Go nem da deduplicação do SQS FIFO:

| Regra | Mecanismo |
| --- | --- |
| Saldo nunca negativo | `CHECK (balance_minor >= 0)` |
| Uma carteira por jogador e moeda | `UNIQUE (player_id, currency)` |
| Transação na moeda e do jogador da carteira | FK composta `(wallet_id, player_id, currency)` |
| Versão sobe exatamente 1 quando o saldo muda e não muda sem mudança de saldo | trigger `wallets_guard` |
| Toda mudança de saldo tem o lançamento da nova versão com saldo anterior e posterior corretos | constraint trigger diferido `wallets_require_ledger`, checado no commit |
| Lançamento com aritmética correta | `CHECK` de `balance_after = balance_before ± amount` |
| Lançamentos formam uma cadeia por carteira | trigger `wallet_ledger_entries_chain` e `UNIQUE (wallet_id, wallet_version)` |
| Um lançamento por transação | `UNIQUE (wallet_id, transaction_id)` |
| Lançamento só para transação `PROCESSED`, com o mesmo valor, direção coerente e nunca para `LOSS` | constraint trigger diferido `wallet_ledger_entries_match_transaction` |
| Ledger append-only | trigger que recusa `UPDATE`, `DELETE` e `TRUNCATE` (inclusive para o dono) e `GRANT` só de `SELECT, INSERT` ao role do serviço |
| Transação terminal não muda, campos de negócio nunca mudam, nada é apagado | trigger `wager_transactions_guard` |
| Chave de idempotência e id externo únicos por provedor | índices únicos `(provider_id, idempotency_key)` e `(provider_id, external_transaction_id)` |
| Um crédito inicial por carteira | índice único parcial em `wallet_id WHERE kind = 'OPENING'` |
| Uma transação é revertida no máximo uma vez | índice único parcial em `reference_transaction_id WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK')` |
| Origem interna x externa | `CHECK`s: `OPENING` sempre `INTERNAL`, sem provedor, chave, hash, rodada, jogo nem referência; externas com todos eles |
| Política de valores | `CHECK`: `LOSS` com zero, os demais maiores que zero |
| Payload da outbox imutável | coluna `json` (preserva o texto) e trigger `outbox_events_guard` |

O serviço conecta como `wallet_app`, que não é dono das tabelas. As migrations rodam com o dono.

## Idempotência

A chave é o header `Idempotency-Key` no HTTP e `data.idempotencyKey` no SQS. Ela é única por provedor e é gravada como veio: o servidor nunca a troca por uma chave calculada.

Hash do payload: SHA-256, em hexadecimal, do JSON canônico dos campos de negócio:

- campos: `providerId`, `externalTransactionId`, `playerId`, `walletId`, `roundId`, `gameId`, `kind`, `money.amount`, `money.currency` e, se houver, `referenceExternalTransactionId`;
- fora do hash: a chave de idempotência, headers, `messageId`, `occurredAt` e atributos do SQS;
- chaves em ordem lexicográfica, sem espaços, strings com escape JSON padrão sem escape de HTML, todos os valores como string;
- normalização: UUIDs em minúsculas (aceitos só no formato de 36 caracteres). O valor e a moeda não são normalizados porque só a forma canônica é aceita.

HTTP e SQS usam a mesma função, então a mesma operação produz o mesmo hash pelos dois caminhos.

Fluxo de `Submit`:

1. Busca por `(provider, chave)` ou `(provider, externalTransactionId)`.
   - Mesma chave e mesmo hash: devolve o resultado gravado, com `idempotentReplay: true` e o saldo da época (`balance_after_minor`).
   - Mesma chave e hash diferente: `409 IDEMPOTENCY_KEY_CONFLICT`.
   - Mesmo id externo com outra chave: `409 EXTERNAL_TRANSACTION_CONFLICT`.
2. Lock da carteira e a mesma busca de novo, porque outra requisição pode ter confirmado enquanto esta esperava o lock.
3. Liquidação e gravação. Se mesmo assim duas gravações colidirem, o índice único decide e a perdedora refaz a transação.

Erros corrigíveis (validação, carteira inexistente, jogador ou moeda diferentes da carteira) não gravam nada. O cliente pode corrigir e reenviar com a mesma chave.

## Máquina de estados

```
PENDING -> PROCESSED
PENDING -> REJECTED
PENDING -> FAILED
PENDING -> PENDING_REFERENCE -> PENDING_REFERENCE (nova tentativa)
                             -> PROCESSED | REJECTED | FAILED
```

As transições são métodos do domínio (`MarkProcessed`, `Reject`, `Fail`, `AwaitReference`, `RetryReferenceLater`) e recusam qualquer caminho fora do diagrama com `ErrInvalidTransition`. Estados terminais também são protegidos por trigger.

`PENDING` nunca é confirmado. Uma operação sem dependência é criada, liquidada e gravada já no estado final, na mesma transação. O único estado intermediário que chega ao banco é `PENDING_REFERENCE`, e ele é retomado por qualquer instância (ver abaixo).

Falhas transitórias e permanentes:

| Tipo | Exemplos | Tratamento |
| --- | --- | --- |
| Transitória | conexão, timeout, `lock_timeout` (55P03), serialização ou deadlock, classe 08 e 57, contexto expirado, colisão de índice único | nada é gravado; HTTP 503 com `Retry-After`; SQS volta para a fila com backoff; o worker tenta de novo |
| Entrada corrigível | validação, carteira inexistente, jogador ou moeda da carteira | nada é gravado; HTTP 4xx; SQS vai direto para a DLQ com o motivo |
| Rejeição de negócio | saldo, referência, reversão | grava `REJECTED` com `failureCode`, é terminal; HTTP 422; SQS é removida |
| Permanente de infraestrutura | violação de invariante no banco, erro de domínio inesperado | grava `FAILED` com `PROCESSING_FAILED` numa transação separada, para auditoria; HTTP 500; o replay devolve o mesmo `FAILED` |

A classificação fica em `postgres.classify`, que traduz o erro do driver para `app.ErrUnavailable`, e em `app.Transient`.

## Operações e reversões

| Tipo | Efeito | Regras |
| --- | --- | --- |
| `BET` | débito | valor > 0; sem saldo: `INSUFFICIENT_FUNDS` |
| `WIN` | crédito | valor > 0; referência opcional a uma `BET` da mesma rodada |
| `LOSS` | nenhum | valor `0.00`; não cria lançamento nem muda a versão; emite só `WagerTransactionProcessed` |
| `REFUND` | crédito | referência obrigatória a uma `BET` processada, mesmo valor |
| `ROLLBACK` | inverso do original | referência obrigatória a `BET`, `WIN` ou `REFUND` processada, mesmo valor |

A referência é resolvida por `(providerId, referenceExternalTransactionId)` e precisa concordar em provedor, jogador, carteira, moeda e rodada. Reversões parciais são recusadas (`REFERENCE_AMOUNT_MISMATCH`). `BET` e `LOSS` não aceitam referência.

Combinações sobre a mesma aposta: cada transação pode ser revertida uma única vez, qualquer que seja o tipo da reversão. O índice `wager_transactions_single_reversal` garante isso no banco.

- `BET` seguida de `REFUND`: crédito. Um `ROLLBACK` dessa `BET` depois disso é `ALREADY_REVERSED`, porque o débito já foi devolvido. O mesmo vale para a ordem inversa.
- `ROLLBACK` do `REFUND` debita o reembolso de volta. Com isso a aposta volta a valer, mas não pode ser reembolsada de novo: a vaga de reversão dela continua ocupada pelo `REFUND`. Assim o mesmo débito nunca é devolvido duas vezes.
- `ROLLBACK` de um `ROLLBACK` é recusado (`INVALID_REFERENCE_KIND`).

Uma reversão que precisaria debitar mais que o saldo (`ROLLBACK` de `WIN` ou de `REFUND`) é gravada como `REJECTED` com `INSUFFICIENT_FUNDS_FOR_REVERSAL`, diferente do `INSUFFICIENT_FUNDS` de uma aposta.

## Referências pendentes

Quando a referência não existe, a transação vai para `PENDING_REFERENCE` com `next_attempt_at` e `reference_deadline` (agora + `REFERENCE_TTL`), e o evento `WagerTransactionPendingReference` é gravado. No SQS, a mensagem é concluída na inbox e removida; a continuação fica com o worker.

O worker roda em todas as instâncias:

1. `ClaimDue` pega um lote vencido com `FOR UPDATE SKIP LOCKED` e empurra `next_attempt_at` para frente (`REFERENCE_LEASE`). Esse é o lease: se a instância morrer, a linha volta a vencer e outra instância assume.
2. Para cada item: lock da carteira, lock da transação, confere se ainda está pendente e roda a mesma liquidação do `Submit`.
3. Se a referência continua faltando, agenda a próxima tentativa com backoff exponencial (`REFERENCE_INITIAL_BACKOFF`, dobrando até `REFERENCE_MAX_BACKOFF`).

Quando uma operação é processada, as pendências que esperam por ela são antecipadas para agora, então a resolução normalmente leva só o intervalo de polling.

Casos:

| Situação da referência | Resultado |
| --- | --- |
| Não existe até o prazo ou até `REFERENCE_MAX_ATTEMPTS` | `REJECTED` com `REFERENCE_NOT_FOUND` e `WagerTransactionRejected` |
| Existe mas também está pendente | continua esperando; no prazo, `REJECTED` com `REFERENCE_STILL_PENDING` |
| Terminou `REJECTED` ou `FAILED` | `REJECTED` com `REFERENCE_NOT_PROCESSED`, sem esperar |
| Processada mas incompatível | `REFERENCE_MISMATCH`, `REFERENCE_AMOUNT_MISMATCH`, `INVALID_REFERENCE_KIND` ou `ALREADY_REVERSED` |

Uma referência que chega depois da expiração não reabre a reversão rejeitada.

## Códigos de falha

| Código | Onde | Tipo |
| --- | --- | --- |
| `INSUFFICIENT_FUNDS` | `failureCode`, 422 | definitivo |
| `INSUFFICIENT_FUNDS_FOR_REVERSAL` | `failureCode`, 422 | definitivo |
| `REFERENCE_NOT_FOUND` | `failureCode`, 422 | definitivo |
| `REFERENCE_STILL_PENDING` | `failureCode`, 422 | definitivo |
| `REFERENCE_NOT_PROCESSED` | `failureCode`, 422 | definitivo |
| `REFERENCE_MISMATCH` | `failureCode`, 422 | definitivo |
| `REFERENCE_AMOUNT_MISMATCH` | `failureCode`, 422 | definitivo |
| `INVALID_REFERENCE_KIND` | `failureCode`, 422 | definitivo |
| `ALREADY_REVERSED` | `failureCode`, 422 | definitivo |
| `PROCESSING_FAILED` | `failureCode` de `FAILED`, 500 | definitivo, exige análise |
| `VALIDATION_ERROR`, `MALFORMED_REQUEST`, `INVALID_IDEMPOTENCY_KEY` | `error.code`, 400 | corrigível |
| `CURRENCY_MISMATCH`, `WALLET_PLAYER_MISMATCH` | `error.code`, 400 | corrigível |
| `WALLET_NOT_FOUND` | `error.code`, 404 | corrigível |
| `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT` | `error.code`, 409 | exige chave ou id novo |
| `TEMPORARILY_UNAVAILABLE` | `error.code`, 503 | transitório, reenviar |

Os códigos definitivos ficam gravados e o replay os devolve. Os corrigíveis não gravam nada.

## Consumidor SQS

Contrato da fila `wager-transactions.fifo`:

- `MessageGroupId`: o `walletId`. Mensagens da mesma carteira são entregues em ordem, e carteiras diferentes andam em paralelo.
- `MessageDeduplicationId`: o `messageId` do envelope. Isso descarta reenvios do produtor dentro da janela de 5 minutos do SQS. A inbox cobre o resto.
- Corpo: o envelope do enunciado, com `data.idempotencyKey`.

Processamento:

1. `ReceiveMessage` com long polling (`SQS_WAIT_SECONDS`) e `VisibilityTimeout` de 30 s. O tratamento de cada mensagem tem prazo próprio (`SQS_HANDLE_TIMEOUT`, 20 s), sempre menor que a visibilidade.
2. Validação: remetente (`SenderId`) na lista `SQS_ALLOWED_SENDERS` e provedor permitido para esse remetente, JSON estrito (campos desconhecidos, número no lugar de string e lixo no fim são recusados), `type`, `occurredAt` e as mesmas validações de domínio do HTTP.
3. Numa única transação: inbox (`consumer_name`, `messageId`, hash), a operação e a conclusão na inbox. O hash da inbox cobre `type` e todo o `data`, incluindo a chave. Uma reentrega com o mesmo `messageId` e conteúdo diferente é conflito.
4. Depois do commit, `DeleteMessage`. Se o delete falhar ou o processo morrer antes dele, a mensagem volta, a inbox a reconhece como duplicata e ela é removida sem efeito.

| Resultado | Ação |
| --- | --- |
| Processada, rejeitada por regra, pendente, replay ou duplicata | remove |
| Falha transitória | `ChangeMessageVisibility` com 2^n segundos (n = recebimentos, até a visibilidade) e deixa voltar; após `SQS_MAX_RECEIVES` (5) recebimentos o redrive move para a DLQ |
| Inválida ou conflito | copia para a DLQ com o atributo `failureReason` e remove |

Motivos na DLQ: `MALFORMED_MESSAGE`, `UNSUPPORTED_TYPE`, `VALIDATION_ERROR`, `WALLET_NOT_FOUND`, `CURRENCY_MISMATCH`, `WALLET_PLAYER_MISMATCH`, `MESSAGE_ID_CONFLICT`, `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `UNAUTHORIZED_SENDER`, `FORBIDDEN_PROVIDER`. A DLQ guarda as mensagens por 14 dias. A reentrada é manual (`StartMessageMoveTask` ou reenvio pelo produtor).

Em `SIGTERM`, o consumidor para de buscar mensagens, termina a que está em andamento (com o prazo próprio dela) e devolve à fila, com visibilidade zero, as mensagens já recebidas que ainda não começaram.

## Outbox

Os eventos são gravados em `outbox_events` na mesma transação da mudança que os gerou. O payload é o envelope completo em JSON, gravado uma vez e nunca reconstruído, então uma republicação envia exatamente os mesmos bytes e o mesmo `eventId`.

O relay roda em todas as instâncias:

1. `Claim` pega até `OUTBOX_BATCH` eventos vencidos com `FOR UPDATE SKIP LOCKED`, soma uma tentativa e empurra `next_attempt_at` pelo `OUTBOX_LEASE`. Vários publishers disputam a tabela sem pegar o mesmo evento; o lease expirado de um publisher morto devolve o evento para os outros.
2. Envia em lotes de 10 com `SendMessageBatch`, com timeout por chamada.
3. Marca os enviados como publicados. Os que falharam recebem `last_error` e `next_attempt_at` com backoff exponencial com jitter, até `OUTBOX_MAX_BACKOFF`. Não há limite de tentativas: um evento confirmado nunca é descartado.
4. No shutdown, os eventos pegos e ainda não enviados são devolvidos imediatamente.

| Queda | Efeito |
| --- | --- |
| Antes do commit | nada foi gravado, nem evento |
| Entre commit e publicação | o evento fica pendente e qualquer instância o publica depois |
| Entre publicação e confirmação | o lease expira e outra instância republica com o mesmo `eventId`. O SQS FIFO descarta a cópia dentro de 5 minutos (`MessageDeduplicationId` = `eventId`); depois disso, o consumidor deduplica por `eventId` |

### Contrato dos eventos

Fila `wallet-events.fifo`:

- `MessageGroupId`: `walletId`, então os eventos de uma carteira saem em ordem;
- `MessageDeduplicationId`: `eventId`;
- atributos `eventType` e `eventId`, para roteamento sem abrir o corpo;
- entrega pelo menos uma vez: o consumidor deve deduplicar por `eventId`. `walletVersion` ordena as mudanças de saldo de uma carteira mesmo se um retry inverter a ordem de dois eventos.

Envelope:

```json
{
  "eventId": "uuid v7",
  "eventType": "WalletBalanceChanged",
  "aggregateId": "walletId ou transactionId",
  "correlationId": "X-Correlation-Id da requisição ou messageId",
  "causationId": "requisição, mensagem ou transação que causou o evento",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "version": 1,
  "data": {}
}
```

| Evento | Agregado | Quando | `data` |
| --- | --- | --- | --- |
| `WagerTransactionProcessed` | transação | operação concluída, inclusive `LOSS` e `OPENING` | ids, tipo, `money`, `balanceAfter`, referência resolvida |
| `WagerTransactionRejected` | transação | rejeição de negócio | ids, tipo, `money`, `failureCode` |
| `WalletBalanceChanged` | carteira | mudança efetiva de saldo | `walletId`, `transactionId`, `direction`, `money`, `balanceBefore`, `balanceAfter`, `walletVersion` |
| `WagerTransactionPendingReference` | transação | entrada em espera | ids, referência externa, `retryDeadline` |

Cada evento é um tipo Go próprio, e o tipo e a versão vêm dos métodos desse tipo, sem campo livre para preencher. Datas são UTC em RFC 3339 com milissegundos e valores monetários são strings decimais. Eventos de `OPENING` omitem os campos externos.

## Autenticação e autorização

IdP: Keycloak 26, importado do arquivo do realm. Escolhi o Keycloak porque é o sugerido, é um IdP OIDC completo, roda em container e tem import declarativo, o que torna o ambiente reproduzível. A comunicação entre serviços usa `client_credentials`: cada provedor e o serviço interno são clientes confidenciais com service account. O serviço não guarda senhas nem emite tokens.

Validação de cada requisição (`go-oidc`):

- assinatura RS256 contra o JWKS do realm, com as chaves em cache e atualizadas quando aparece um `kid` novo;
- `iss` igual a `OIDC_ISSUER`, `aud` contendo `wallet-api` e `exp` no futuro;
- `typ` igual a `Bearer`, o que recusa ID tokens e refresh tokens;
- falha em qualquer item: 401 com `WWW-Authenticate`.

O token carrega `provider_id` (mapper de claim fixa por cliente) e `roles` (papéis de realm). O `providerId` autorizado vem do token, nunca da requisição.

| Rota | Papel | Regra extra |
| --- | --- | --- |
| `POST /wallets`, `GET /wallets/{id}`, `GET /wallets/{id}/ledger`, `POST /wallets/{id}/reconciliation` | `wallet-internal` | |
| `POST /wagering/transactions` | `wallet-provider` | `providerId` do corpo igual ao do token, senão 403 antes de qualquer leitura ou escrita |
| `GET /wagering/transactions/{id}` | qualquer um dos dois | provedor só vê as suas; a de outro provedor responde 404, para não revelar que existe |
| `GET /providers/{providerId}/wagering/transactions/{ext}` | qualquer um dos dois | provedor só com o próprio `providerId`, senão 403 |
| `/health/*`, `/metrics` | público | |

A chave de idempotência é única por provedor. O provedor B não tem como ver um resultado do provedor A por replay: o corpo com `providerId` de A é recusado, e a mesma chave usada por B é outra operação.

Não há vínculo entre provedor e jogador no enunciado, então um provedor autenticado pode operar qualquer carteira. Um vínculo desse tipo seria mais uma checagem no caso de uso.

### Mensageria

Cada componente usa suas próprias credenciais de broker, pela cadeia padrão do SDK (variáveis de ambiente localmente, role IAM na AWS). O comando `provision` aplica uma política de recurso em cada fila:

| Fila | Quem pode o quê |
| --- | --- |
| `wager-transactions.fifo` | produtores (`wager-gateway`): `SendMessage`; serviço (`wallet-service`): receber, apagar e mudar visibilidade |
| `wager-transactions-dlq.fifo` | serviço: `SendMessage`; operadores: inspecionar e reprocessar |
| `wallet-events.fifo` | serviço: `SendMessage`; leitores de eventos: consumir |

Os ARNs são configuráveis (`SQS_PRODUCER_PRINCIPALS`, `SQS_SERVICE_PRINCIPAL`, `SQS_EVENT_READER_PRINCIPALS`, `SQS_OPERATOR_PRINCIPALS`).

No consumidor, o `SenderId` de cada mensagem precisa estar em `SQS_ALLOWED_SENDERS`, que também restringe os provedores permitidos por remetente. Mesmo de um remetente confiável, a mensagem passa por todas as validações de domínio: a carteira precisa existir e ser do jogador na moeda informada, a referência precisa ser do mesmo provedor e `OPENING` é recusado.

Limitação: o MiniStack guarda as políticas mas não as avalia, e informa sempre o id da conta como `SenderId`. Localmente, a separação por credencial é garantida só pelo isolamento de conta do MiniStack (credenciais de outra conta não enxergam as filas) e pela lista de remetentes. Na AWS, as mesmas políticas passam a valer, e o `SenderId` passa a identificar o principal IAM.

## Composição com Fx

`wiring.Options` monta a aplicação em módulos:

| Módulo | Fornece |
| --- | --- |
| `observability` | logger, métricas, fault hook |
| `database` | pool, `TxManager`, repositórios (como interfaces do `app`) |
| `messaging` | cliente SQS, URLs das filas, publisher de eventos |
| `auth` | verificador de tokens |
| `services` | `Wagering`, `WalletService`, `OutboxRelay` |
| `workers` | invoke que registra o worker de referências e o relay |
| `consumer` | consumidor SQS |
| `http` | API e servidor |

Tudo é injetado por construtor. Os `fx.Invoke` de workers, consumidor e servidor estão nessa ordem de propósito.

Início:

1. `config.Load` valida todas as variáveis e aborta listando todos os problemas antes de o Fx subir.
2. A resolução das filas espera o SQS responder, com timeout.
3. `OnStart` do pool espera o `Ping`, e o do verificador espera o JWKS responder com ao menos uma chave.
4. Workers, consumidor e servidor HTTP sobem.

Parada (ordem inversa de registro, prazo `SHUTDOWN_TIMEOUT`):

1. HTTP: a readiness passa a responder 503, depois `Shutdown` para de aceitar conexões e espera as requisições em andamento.
2. Consumidor: para de buscar, termina a mensagem em andamento e devolve as recebidas.
3. Workers: cancelados. O relay devolve os eventos pegos, e cada loop espera a iteração atual terminar (`Done` fecha quando o loop sai).
4. Pool: fechado por último, quando ninguém mais o usa.

`TestFxStartAndStopReleaseEverything` confere a ordem pelos logs, confere que o pool e a porta foram fechados e roda o `goleak`. `TestGraph` valida o grafo de dependências sem infraestrutura.

## Observabilidade

Logs JSON (`log/slog`) com os identificadores guardados no contexto: `correlationId`, `messageId`, `transactionId`, `walletId`, `providerId`, `eventId`. Tokens, corpos e payloads não são registrados; a reconciliação registra só a diferença.

Métricas em `/metrics` (Prometheus):

| Métrica | Uso |
| --- | --- |
| `wager_transactions_total{source,kind,status,replay}` | resultados por status |
| `wager_duplicates_total{source,reason}` | replays por chave e duplicatas da inbox |
| `wallet_retries_total{component}` | retries de transação, SQS e referências |
| `wallet_concurrency_conflicts_total{reason}` | colisões de índice único e `lock_timeout` |
| `sqs_messages_total{outcome}`, `sqs_dead_lettered_total{reason}` | consumo e DLQ |
| `outbox_pending_events`, `outbox_lag_seconds` | atraso da outbox (idade do evento pendente mais antigo) |
| `outbox_published_total`, `outbox_publish_failures_total` | publicação |
| `wager_processing_seconds{source}`, `http_request_seconds` | latência |
| `wallet_reconciliation_divergences_total` | divergências de reconciliação |

Health: `/health/live` responde sempre que o processo está de pé; `/health/ready` testa PostgreSQL e SQS e responde 503 durante o shutdown.

## Limitações, interpretações e o que ficou de fora

- O MiniStack não avalia políticas IAM (ver Mensageria).
- A vazão da outbox no ambiente local é limitada pelo MiniStack, que roda numa única thread e publica perto de 200 mensagens por segundo. Isso está medido no README.
- Ordem dos eventos: por carteira, dentro do FIFO. Se um evento falha e o seguinte da mesma carteira é enviado, a ordem se inverte até o retry. `walletVersion` permite reordenar as mudanças de saldo.
- O endpoint `/metrics` é público. Em produção ficaria numa porta interna.
- Interpretações adotadas: `WIN` pode referenciar uma `BET` (opcional) e a referência passa pelas mesmas checagens; `BET` e `LOSS` não aceitam referência; uma transação é revertida no máximo uma vez, independentemente do tipo da reversão; erros de entrada corrigíveis não geram registro nem evento; `POST /wallets` não tem chave de idempotência, e repetir a abertura resulta em 409.
- Não implementado: ledger de partidas dobradas, tracing com OpenTelemetry e dashboards. A abertura de carteira não tem idempotência própria além do 409. Não há rotina de limpeza das linhas antigas da outbox e da inbox; o trigger já permite apagar eventos publicados.
