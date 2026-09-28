# Wallet

Serviço em Go que movimenta carteiras de jogadores a partir de operações de provedores de jogos (`BET`, `WIN`, `LOSS`, `REFUND`, `ROLLBACK`), recebidas por HTTP e por SQS. As duas entradas usam o mesmo caso de uso e as mesmas garantias de idempotência. A composição usa Uber Fx, a persistência é PostgreSQL com `pgx`, a autenticação é feita pelo Keycloak e a mensageria roda no MiniStack.

As decisões de desenho estão em [ARCHITECTURE.md](ARCHITECTURE.md).

## Pré-requisitos

- Docker com Compose v2
- Go 1.25 (o `go.mod` fixa `toolchain go1.25.14`, a mesma versão do Dockerfile), necessário só para rodar testes e o teste de carga no host
- `curl` para os exemplos; `make` é opcional

O LocalStack passou a exigir token de conta a partir da versão 2026.03, então o ambiente usa o MiniStack (`ministackorg/ministack:1.5.17`), que é compatível com a API do SQS e não exige conta.

## Subindo o ambiente

```sh
docker compose up --build
```

O Compose sobe, nesta ordem:

| Serviço | Papel | Porta no host |
| --- | --- | --- |
| `postgres` | PostgreSQL 17, com o role `wallet_app` criado por `deploy/postgres/init.sql` | 55432 |
| `keycloak` | IdP, com o realm `wallet` importado de `deploy/keycloak/wallet-realm.json` | 8180 |
| `ministack` | SQS | 4566 |
| `migrate` | aplica as migrations e termina | |
| `provision` | cria as filas, a DLQ, o redrive e as políticas das filas, e termina | |
| `wallet-1`, `wallet-2`, `wallet-3` | três instâncias independentes do serviço | 8081, 8082, 8083 |

As portas podem ser trocadas copiando `.env.example` para `.env`. Para derrubar tudo e apagar os dados: `docker compose down -v`.

## Identidades de teste

O realm é importado automaticamente. Todos os clientes usam `client_credentials`.

| client_id | secret | Papel | provider_id |
| --- | --- | --- | --- |
| `provider-a` | `provider-a-secret` | `wallet-provider` | `provider-a` |
| `provider-b` | `provider-b-secret` | `wallet-provider` | `provider-b` |
| `provider-a-short-lived` | `provider-a-short-lived-secret` | `wallet-provider`, token de 2 s | `provider-a` |
| `wallet-internal` | `wallet-internal-secret` | `wallet-internal` | |
| `reporting` | `reporting-secret` | nenhum papel (para testar 403) | |
| `other-api` | `other-api-secret` | token sem a audiência `wallet-api` (para testar 401) | |

O issuer dos tokens é fixado em `http://keycloak:8080/realms/wallet` (`KC_HOSTNAME`), o endereço interno do Keycloak, mesmo quando o token é pedido pelo host em `localhost:8180`. O serviço valida esse issuer e busca as chaves em `OIDC_JWKS_URL`.

## Exemplos de chamadas

```sh
token() {
  curl -s -d grant_type=client_credentials -d client_id=$1 -d client_secret=$1-secret \
    http://localhost:8180/realms/wallet/protocol/openid-connect/token | sed -E 's/.*"access_token":"([^"]+)".*/\1/'
}
INTERNAL=$(token wallet-internal)
PROVIDER=$(token provider-a)
PLAYER=0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1
```

Abrir carteira (serviço interno):

```sh
curl -s -X POST localhost:8081/wallets -H "Authorization: Bearer $INTERNAL" -H 'Content-Type: application/json' \
  -d "{\"playerId\":\"$PLAYER\",\"initialBalance\":{\"amount\":\"1000.00\",\"currency\":\"BRL\"}}"
# {"id":"...","playerId":"...","balance":{"amount":"1000.00","currency":"BRL"},"version":1,...}
WALLET=<id retornado>
```

Enviar uma aposta (provedor). Repetir o mesmo comando em qualquer instância devolve o mesmo resultado com `idempotentReplay: true`:

```sh
curl -s -X POST localhost:8082/wagering/transactions -H "Authorization: Bearer $PROVIDER" \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: provider-a:transaction-123' \
  -d "{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-123\",\"playerId\":\"$PLAYER\",
       \"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
       \"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}"
# {"transactionId":"...","status":"PROCESSED","balance":{"amount":"975.00","currency":"BRL"},"idempotentReplay":false}
```

Para `REFUND` e `ROLLBACK`, inclua `referenceExternalTransactionId`. Se a referência ainda não chegou, a resposta é `202` com `status: PENDING_REFERENCE`.

Consultas:

```sh
curl -s localhost:8083/providers/provider-a/wagering/transactions/transaction-123 -H "Authorization: Bearer $PROVIDER"
curl -s localhost:8083/wagering/transactions/<transactionId> -H "Authorization: Bearer $PROVIDER"
curl -s localhost:8081/wallets/$WALLET -H "Authorization: Bearer $INTERNAL"
curl -s "localhost:8081/wallets/$WALLET/ledger?limit=50" -H "Authorization: Bearer $INTERNAL"
curl -s -X POST localhost:8081/wallets/$WALLET/reconciliation -H "Authorization: Bearer $INTERNAL"
curl -s localhost:8081/health/ready
curl -s localhost:8081/metrics
```

Enviar a mesma operação por SQS, usando o aws-cli em container na rede do Compose:

```sh
BODY="{\"messageId\":\"msg-123\",\"type\":\"WagerTransactionRequested\",\"occurredAt\":\"2026-09-08T12:00:00.000Z\",
\"data\":{\"providerId\":\"provider-a\",\"externalTransactionId\":\"transaction-124\",\"idempotencyKey\":\"provider-a:transaction-124\",
\"playerId\":\"$PLAYER\",\"walletId\":\"$WALLET\",\"roundId\":\"round-987\",\"gameId\":\"fortune-chimp\",\"kind\":\"BET\",
\"money\":{\"amount\":\"25.00\",\"currency\":\"BRL\"}}}"

docker run --rm --network wallet_default -e AWS_ACCESS_KEY_ID=wager-gateway -e AWS_SECRET_ACCESS_KEY=x \
  -e AWS_DEFAULT_REGION=us-east-1 amazon/aws-cli:2.27.0 --endpoint-url http://ministack:4566 \
  sqs send-message --queue-url http://ministack:4566/000000000000/wager-transactions.fifo \
  --message-group-id "$WALLET" --message-deduplication-id msg-123 --message-body "$BODY"
```

No Git Bash do Windows, exporte `MSYS_NO_PATHCONV=1` antes do `docker run`. Os eventos de saída ficam na fila `wallet-events.fifo`.

## Contrato HTTP

| Situação | Status | Corpo |
| --- | --- | --- |
| Operação processada (nova ou replay) | 200 | `{transactionId, status: "PROCESSED", balance, idempotentReplay}` |
| Aguardando referência | 202 | `{transactionId, status: "PENDING_REFERENCE", nextAttemptAt, retryDeadline, idempotentReplay}` |
| Rejeição de negócio (persistida, terminal) | 422 | `{transactionId, status: "REJECTED", failureCode, idempotentReplay}` |
| Falha permanente registrada | 500 | `{transactionId, status: "FAILED", failureCode: "PROCESSING_FAILED"}` |
| Entrada inválida (nada é gravado) | 400 | `{error: {code, message, fields?}}` com `VALIDATION_ERROR`, `MALFORMED_REQUEST`, `INVALID_IDEMPOTENCY_KEY`, `CURRENCY_MISMATCH`, `WALLET_PLAYER_MISMATCH`, `INVALID_CURSOR` |
| Carteira ou recurso inexistente | 404 | `WALLET_NOT_FOUND`, `NOT_FOUND` |
| Conflito | 409 | `IDEMPOTENCY_KEY_CONFLICT`, `EXTERNAL_TRANSACTION_CONFLICT`, `WALLET_ALREADY_EXISTS` |
| Sem token, token inválido ou expirado | 401 | `UNAUTHENTICATED`, `INVALID_TOKEN` |
| Papel ou provedor não autorizado | 403 | `FORBIDDEN`, `PROVIDER_MISMATCH` |
| Indisponibilidade transitória | 503 | `TEMPORARILY_UNAVAILABLE`, com `Retry-After`; reenviar com a mesma chave é seguro |

`POST /wallets` responde `201`. Os códigos de rejeição estão descritos em [ARCHITECTURE.md](ARCHITECTURE.md#códigos-de-falha).

## Variáveis de ambiente

Todas têm valor padrão, menos `DATABASE_URL` e `OIDC_ISSUER`. A lista completa, com valores locais, está em [.env.example](.env.example). As principais:

| Variável | Padrão | Uso |
| --- | --- | --- |
| `DATABASE_URL` | | conexão do serviço (role `wallet_app`) |
| `MIGRATION_DATABASE_URL` | `DATABASE_URL` | conexão das migrations (dono do schema) |
| `OIDC_ISSUER` | | issuer esperado nos tokens |
| `OIDC_JWKS_URL` | `<issuer>/protocol/openid-connect/certs` | onde buscar as chaves |
| `OIDC_AUDIENCE` | `wallet-api` | audiência exigida |
| `AWS_ENDPOINT_URL`, `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | | SQS |
| `SQS_WAGER_QUEUE`, `SQS_WAGER_DLQ`, `SQS_EVENTS_QUEUE` | `wager-transactions.fifo`, `wager-transactions-dlq.fifo`, `wallet-events.fifo` | filas |
| `SQS_VISIBILITY_TIMEOUT`, `SQS_HANDLE_TIMEOUT`, `SQS_MAX_RECEIVES` | `30`, `20s`, `5` | tempo de processamento e tentativas antes da DLQ |
| `SQS_ALLOWED_SENDERS` | `000000000000=*` | SenderId autorizado e os provedores que ele pode enviar |
| `DB_LOCK_TIMEOUT`, `REQUEST_TIMEOUT`, `SHUTDOWN_TIMEOUT` | `5s`, `10s`, `25s` | limites de espera |
| `REFERENCE_TTL`, `REFERENCE_MAX_ATTEMPTS` | `5m`, `20` | espera máxima por uma referência |
| `OUTBOX_LEASE`, `OUTBOX_MAX_BACKOFF` | `30s`, `1m` | recuperação e retry da publicação |
| `CONSUMER_ENABLED`, `OUTBOX_ENABLED`, `REFERENCES_ENABLED` | `true` | liga ou desliga cada worker |

## Migrations

As migrations ficam em `migrations/` e são embutidas no binário (golang-migrate). O serviço `migrate` do Compose aplica todas antes das instâncias subirem.

```sh
docker compose run --rm migrate migrate up          # aplica todas
docker compose run --rm migrate migrate down 1      # reverte a última
docker compose run --rm migrate migrate down all    # reverte todas
docker compose run --rm migrate migrate version
docker compose run --rm migrate migrate force 4     # limpa estado sujo após falha
```

Fora do Docker: `MIGRATION_DATABASE_URL=postgres://wallet:wallet@localhost:55432/wallet?sslmode=disable go run ./cmd/wallet migrate up`.

## Filas

O serviço `provision` roda `wallet provision`, que cria `wager-transactions-dlq.fifo`, `wager-transactions.fifo` (com redrive após `SQS_MAX_RECEIVES` recebimentos) e `wallet-events.fifo`, e aplica a política de cada fila. O comando pode ser repetido. Manualmente:

```sh
docker compose run --rm provision
```

## Executando fora do Docker

```sh
docker compose up -d postgres keycloak ministack migrate provision
cp .env.example .env
set -a; . ./.env; set +a
go run ./cmd/wallet serve
```

## Testes

```sh
go vet ./...
go test ./...          # testes unitários, sem dependências externas
go test -race ./...
```

### Integração

Os testes de integração ficam em `test/integration` atrás da build tag `integration`. Eles usam PostgreSQL, Keycloak e MiniStack reais. Cada teste clona um banco já migrado e cria suas próprias filas, então rodam em paralelo sem interferência.

```sh
docker compose up -d postgres keycloak ministack
go vet -tags integration ./...
go test -race -tags integration -count=1 ./test/integration/...
```

Variáveis para apontar para outros endereços: `IT_PG_HOST` (padrão `localhost:55432`), `IT_KEYCLOAK_URL` (`http://localhost:8180`), `IT_OIDC_ISSUER`, `IT_SQS_ENDPOINT` (`http://localhost:4566`), `IT_PG_CONTAINER` e `IT_SQS_CONTAINER` (nomes dos containers pausados nos testes de indisponibilidade).

Também dá para rodar tudo dentro do Docker, em Linux, sem Go no host:

```sh
docker compose --profile test run --rm tests
```

Diferenças entre os dois modos: o teste de SIGTERM só roda em Linux, porque o Windows não entrega sinais a processos filhos. Os testes de indisponibilidade pausam os containers pelo CLI do Docker, então só rodam no host.

### O que cada grupo cobre

| Arquivo | Cenários |
| --- | --- |
| `auth_test.go` | token ausente, malformado, assinado por outra chave, adulterado, com outra audiência e expirado; papéis; isolamento entre provedores, inclusive em replay; nenhum efeito em acesso negado |
| `wagering_test.go` | replay com o saldo original, conflitos de chave e de `externalTransactionId`, a mesma aposta 50 vezes em paralelo, as duas apostas de 80.00 sobre 100.00, 120 débitos concorrentes sem lost update, carteiras independentes com uma delas bloqueada, validações |
| `references_test.go` | `ROLLBACK` antes da `BET`, expiração de referência, combinações de `REFUND` e `ROLLBACK`, reversão sem saldo, `LOSS`, abertura com saldo zero, paginação e reconciliação |
| `messaging_test.go` | provisionamento, consumo, reentrega deduplicada pela inbox, a mesma operação por HTTP e SQS ao mesmo tempo, mensagens inválidas e conflito de `messageId` na DLQ, falha transitória até o redrive, remetente e provedor não autorizados |
| `outbox_test.go` | dois publishers disputando a outbox, queda entre publicar e confirmar, eventos confirmados antes de uma queda, retry com backoff |
| `schema_test.go` | migrations up/down/up, ledger append-only para o role do serviço e para o dono, constraints e triggers, divergência detectada pela reconciliação |
| `processes_test.go` | três processos independentes (corrida, duplicatas, HTTP e SQS cruzados), queda do consumidor depois do commit, queda antes do commit, queda do publisher depois de publicar, reinício com pendências, SIGTERM |
| `lifecycle_test.go` | composição Fx, ordem de parada e ausência de goroutines vazadas |
| `outage_test.go` | PostgreSQL e MiniStack pausados e retomados |

Ao final dos cenários financeiros, o teste compara o saldo gravado de cada carteira com a soma de créditos menos débitos do ledger.

### Múltiplas instâncias e simulação de falhas

`processes_test.go` compila o binário e sobe processos separados, cada um com seu pool e sua memória. As quedas usam `FAULT_POINTS`, que encerra o processo com `os.Exit(86)` num ponto definido:

| Ponto | Onde |
| --- | --- |
| `wager.before_commit` | dentro da transação de uma operação, antes do commit |
| `consumer.after_commit` | depois do commit de uma mensagem e antes do `DeleteMessage` |
| `outbox.after_publish` | depois de enviar eventos ao SQS e antes de marcá-los como publicados |

Para reproduzir à mão com o Compose, por exemplo:

```sh
docker compose run --rm -e FAULT_POINTS=consumer.after_commit -p 8090:8080 wallet-1
```

As três instâncias do Compose (8081, 8082, 8083) também servem para testes manuais de concorrência entre processos.

## Teste de carga

Diferencial opcional. O comando abaixo usa as três instâncias do Compose:

```sh
go run ./cmd/loadtest -duration 60s -concurrency 32 -wallets 100            # vazão máxima
go run ./cmd/loadtest -duration 60s -concurrency 16 -wallets 100 -rate 100  # taxa fixa
```

Metodologia: abre as carteiras com saldo alto, e cada cliente envia `BET` de 1.00 e, em uma a cada quatro requisições, `WIN` de 0.50. As requisições vão para instâncias alternadas e 10% delas reenviam uma operação anterior (replay). Ao final, o comando lê `/metrics` das instâncias e reconcilia todas as carteiras.

Ambiente: Windows 11, Docker Desktop (WSL2), 16 CPUs, 32 GB. Stack limpa (`docker compose down -v && docker compose up --build`), três instâncias, gerador rodando no host.

| Cenário | Requisições | Vazão | p50 | p95 | p99 | Erros | Conflitos | Outbox |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| Vazão máxima, 32 clientes, 60 s | 99.582 | 1.659 req/s | 12,9 ms | 53,3 ms | 95,3 ms | 0 | 0 | 151 mil eventos pendentes ao fim |
| 100 req/s, 16 clientes, 60 s | 6.013 | 100 req/s | 4,5 ms | 6,1 ms | 8,5 ms | 0 | 0 | p99 de 0,57 s entre commit e publicação, fila zerada 4 s após a carga |

Nas duas rodadas as 100 carteiras terminaram consistentes. No cenário sem limite, a API grava cerca de 3 mil eventos por segundo, mas o MiniStack roda em uma única thread Python e publica perto de 200 mensagens por segundo, com o container a 100% de CPU enquanto instâncias e PostgreSQL ficam ociosos. O atraso da outbox nesse cenário mede o emulador. A métrica `outbox_lag_seconds` e o drain posterior mostram a fila se esvaziando sem perda.

## Estrutura

```
cmd/wallet            binário: serve, migrate, provision, healthcheck
cmd/loadtest          teste de carga
internal/domain       Money, Wallet, WagerTransaction, ledger, eventos e regras de liquidação
internal/app          casos de uso, relay da outbox e interfaces de repositório
internal/postgres     repositórios pgx, TxManager e migrations
internal/messaging    consumidor SQS, publisher de eventos, provisionamento e políticas
internal/httpapi      handlers, autenticação e mapeamento de erros
internal/auth         validação de tokens OIDC
internal/wiring       composição Fx
internal/worker       loop dos workers
internal/fault        pontos de falha para testes
migrations            SQL versionado
deploy                init do PostgreSQL e realm do Keycloak
test/integration      testes de integração
```
