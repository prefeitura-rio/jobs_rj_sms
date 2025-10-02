## Uso
```sh
$ docker compose --profile gdb-export build
$ docker compose --profile gdb-export up
```

Para requisitar a exportação de um GDB para um .ZIP de CSVs, faça POST ao endpoint `/export/` passando o URI do arquivo, em um bucket do GCS, como parâmetro `gcs_uri`. Por exemplo:

```sh
$ curl -d '{ "gcs_uri": "gs://bucket/path/to/your/file/BACKUP.GDB" }' -H "Content-Type: application/json" http://your_api_domain/export/
#=> {"success":true,"id":"0f26ade5-ecc7-4f75-a034-545506c34a9b"}
```

> [!NOTE]
> O worker do Celery recebe a flag `--concurrency=1`, o que limita ele a 1 task por vez. Isto é, múltiplas requisições distintas a `/export/` não são um problema; exportações ficarão em fila esperando sua execução.

Você pode, então, usar esse ID retornado para verificar o status, através de um GET para `/check/{id}`:

```sh
$ curl http://your_api_domain/check/0f26ade5-ecc7-4f75-a034-545506c34a9b
#=> {"status":"PROGRESS","result":{"status":"Requesting export of file '0f26ade5-ecc7-4f75-a034-545506c34a9b.gdb'...","current":2,"total":6},"task_id":"0f26ade5-ecc7-4f75-a034-545506c34a9b"}
```

Quando a exportação terminar, o resultado será algo como:
```json
{
  "status": "SUCCESS",
  "result": {
    "success": true,
    "output": "gs://bucket/path/to/your/file/BACKUP.zip"
  },
  "task_id": "0f26ade5-ecc7-4f75-a034-545506c34a9b"
}
```

Outros endpoints potencialmente úteis:

* `/list/`
  Retorna a lista de arquivos no volume no momento de execução. Exemplo de resposta no meio de uma exportação:
```text
{
  "/data": [
    [
      "0f26ade5-ecc7-4f75-a034-545506c34a9b.gdb",
      "924.34 MB"
    ]
  ],
  "/data/csv": [
    [
      "CNESHIST.csv",
      "4.07 KB"
    ],
    [
      "CFCES006.csv",
      "4.93 MB"
    ],
    ...
  ]
}
```
* `/clear/`
  Remove o conteúdo inteiro do volume.
> [!CAUTION]
> Não faça GET para `/clear/` no meio de uma exportação! Não testei mas provavelmente vai dar algum caô.


## Desenvolvimento
Como eu tenho desenvolvido:

```sh
$ docker compose --profile gdb-export build
$ docker compose up gdb-export--redis gdb-export--flower
# Em outros terminais separados, para poder
# interromper execução e/ou fazer modificações:
$ docker compose up gdb-export--gdb2csv
$ docker compose up gdb-export--fastapi --build
$ docker compose up gdb-export--celery_worker --build
```

Você pode executar `poetry shell && poetry install --no-root` dentro da pasta `src/` do projeto que estiver desenvolvendo para que o VSCode coloque corzinha e ofereça autocomplete. Contudo, a execução ainda é via `docker compose up (...) --build`. Não é possível, no momento, testar 100% "localmente" – dependemos tanto do volume compartilhado entre containers, quanto da rede do docker para comunicação entre imagens. Provavelmente precisaria configurar profiles no docker compose, com portas expostas publicamente quando em dev; nos scripts, domínios em constantes condicionais (coisas como `EXPORT_DOMAIN = "localhost" if is_dev else "gdb2csv"`) para as requisições entre containers; .....


**TODO**:
- Permitir parâmetros de nomes de tabelas desejadas, charset, etc
- [gdb2csv] `exit(1)` se não conseguir conectar com o banco
- Forma de cancelar exportações correntes (considerando que pode haver uma fila de exportações seguintes aguardando)

---

Adaptado de:
- FastAPI + Celery via Docker Compose: https://github.com/jitendrasinghiitg/docker-fastapi-celery
- Exportação de GDBs: https://github.com/MatheusAvellar/gdb2csv
