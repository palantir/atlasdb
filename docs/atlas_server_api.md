---
title: AtlasDB Server
---

## Table Requests

### Create a Table 
```sh
curl -XPOST http://localhost:3828/atlasdb/create-table/my_table
```

### Puts and Gets (Auto-Committed)

```sh
curl -XPOST http://localhost/atlasdb/put/auto-commit -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"],"val":"KAA="}]}'
```

```sh
curl -XPOST http://localhost/atlasdb/cells/auto-commit -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"]}]}'
```

## Transactions 

### Open a Transaction

```sh
curl -XPOST http://localhost:3828/atlasdb/transaction
```

returns this

```json
{"id":"14f0656a-e5f3-48d7-a15e-6fa3504db797"}
```

### Read and Write Transactionally

```sh
curl -XPOST http://localhost/atlasdb/put/14f0656a-e5f3-48d7-a15e-6fa3504db797 -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"],"val":"KAA="}]}'
```

```sh
curl -XPOST http://localhost/atlasdb/cells/14f0656a-e5f3-48d7-a15e-6fa3504db797 -d'{"table":"my_table","data":[{"row":["AAEC"],"col":["AwQF"]}]}'
```

### Commit a Transaction

```sh
curl -XPOST http://localhost:3828/atlasdb/commit/14f0656a-e5f3-48d7-a15e-6fa3504db797
```
